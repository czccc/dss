use std::{
    cmp::{max, min},
    fmt::Display,
    sync::Arc,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Mutex,
    },
    task::Poll,
    thread,
    time::Duration,
};

// use futures::Stream;
use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot::{channel, Receiver, Sender},
    },
    prelude::*,
    Stream,
};
use futures_timer::Delay;
use rand::Rng;
use tokio::runtime::Builder;

fn election_timeout() -> Duration {
    let variant = rand::thread_rng().gen_range(600, 800);
    Duration::from_millis(variant)
}

fn heartbeat_timeout() -> Duration {
    // let variant = rand::thread_rng().gen_range(100, 104);
    let variant = 50;
    Duration::from_millis(variant)
}

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

#[derive(Message, Clone)]
pub struct ApplyMsg {
    #[prost(bool, tag = "1")]
    pub command_valid: bool,
    #[prost(bytes, tag = "2")]
    pub command: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Message, Clone)]
pub struct Persistent {
    #[prost(uint64, tag = "1")]
    pub current_term: u64,
    #[prost(int32, tag = "2")]
    pub voted_for: i32,
    #[prost(message, repeated, tag = "3")]
    pub log: Vec<LogEntry>,
    #[prost(uint64, tag = "4")]
    pub last_included_index: u64,
    #[prost(uint64, tag = "5")]
    pub last_included_term: u64,
}

#[derive(PartialEq, Clone)]
enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // Persistent state on all servers
    // Updated on stable storage before responding to RPCs
    current_term: Arc<AtomicU64>,
    voted_for: Option<usize>,
    log: Vec<LogEntry>,

    // auxilary state
    role: RaftRole,
    is_leader: Arc<AtomicBool>,
    log_index: Arc<AtomicU64>,

    // Volatile state on all servers
    commit_index: Arc<AtomicU64>,
    last_applied: Arc<AtomicU64>,

    // Volatile state on leader
    // Reinitialized after election
    next_index: Vec<Arc<AtomicU64>>,
    match_index: Vec<Arc<AtomicU64>>,

    // Persistent state on all servers
    // Add when using Snapshot
    last_included_index: Arc<AtomicU64>,
    last_included_term: Arc<AtomicU64>,

    // current save size
    // update when persist
    raft_state_size: Arc<AtomicU64>,

    // RaftEvent channel used in RaftExecutor
    sender: UnboundedSender<RaftEvent>,

    // ApplyMsg channel
    apply_ch: UnboundedSender<ApplyMsg>,

    // for stream
    receiver: UnboundedReceiver<RaftEvent>,
    timeout: Delay,
    apply_msg_delay: Delay,
}

impl Display for Raft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let role = match self.role {
            RaftRole::Follower => "Follower ",
            RaftRole::Candidate => "Candidate",
            RaftRole::Leader => "Leader   ",
        };
        write!(
            f,
            "[{} {}] [Term {}] [Snap {} {}] [Log {} {}] [Commit {} {}]",
            role,
            self.me,
            self.current_term.load(Ordering::SeqCst),
            self.last_included_index.load(Ordering::SeqCst),
            self.last_included_term.load(Ordering::SeqCst),
            self.log.len(),
            self.log.last().map_or(0, |v| v.term),
            self.commit_index.load(Ordering::SeqCst),
            self.last_applied.load(Ordering::SeqCst),
        )
    }
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let peers_num = peers.len();

        let (sender, receiver) = unbounded();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            // state: Arc::default(),
            current_term: Arc::new(AtomicU64::new(0)),
            voted_for: None,
            log: vec![],

            is_leader: Arc::new(AtomicBool::new(false)),
            role: RaftRole::Follower,
            log_index: Arc::new(AtomicU64::new(0)),

            commit_index: Arc::new(AtomicU64::new(0)),
            last_applied: Arc::new(AtomicU64::new(0)),

            last_included_index: Arc::new(AtomicU64::new(0)),
            last_included_term: Arc::new(AtomicU64::new(0)),
            raft_state_size: Arc::new(AtomicU64::new(0)),

            next_index: Vec::new(),
            match_index: Vec::new(),

            sender,
            apply_ch,
            // for stream
            receiver,
            timeout: Delay::new(election_timeout()),
            apply_msg_delay: Delay::new(heartbeat_timeout()),
        };

        for _i in 0..peers_num {
            rf.next_index.push(Arc::new(AtomicU64::new(0)));
            rf.match_index.push(Arc::new(AtomicU64::new(0)));
        }

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        debug!("{} Started!", rf);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
        let mut data = Vec::new();
        // let msg = (&self.current_term, &self.voted_for, &self.log);
        let per = Persistent {
            current_term: self.current_term.load(Ordering::SeqCst),
            voted_for: self.voted_for.map_or(-1, |v| v as i32),
            log: self.log.clone(),
            last_included_index: self.last_included_index.load(Ordering::SeqCst),
            last_included_term: self.last_included_term.load(Ordering::SeqCst),
        };
        labcodec::encode(&per, &mut data).unwrap();
        self.raft_state_size
            .store(data.len() as u64, Ordering::SeqCst);
        self.persister.save_raft_state(data);
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist_with_snapshot(&mut self, snapshot: Vec<u8>) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
        let mut data = Vec::new();
        // let msg = (&self.current_term, &self.voted_for, &self.log);
        let per = Persistent {
            current_term: self.current_term.load(Ordering::SeqCst),
            voted_for: self.voted_for.map_or(-1, |v| v as i32),
            log: self.log.clone(),
            last_included_index: self.last_included_index.load(Ordering::SeqCst),
            last_included_term: self.last_included_term.load(Ordering::SeqCst),
        };
        labcodec::encode(&per, &mut data).unwrap();
        self.persister.save_state_and_snapshot(data, snapshot);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
        match labcodec::decode::<Persistent>(data) {
            Ok(o) => {
                self.current_term = Arc::new(AtomicU64::new(o.current_term));
                self.voted_for = {
                    match o.voted_for < 0 {
                        true => Some(o.voted_for as usize),
                        false => None,
                    }
                };
                self.log = o.log;
                self.last_included_index = Arc::new(AtomicU64::new(o.last_included_index));
                self.last_included_term = Arc::new(AtomicU64::new(o.last_included_term));
                self.commit_index = Arc::new(AtomicU64::new(o.last_included_index));
                self.last_applied = Arc::new(AtomicU64::new(o.last_included_index));
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

    fn send_apply_msg(&mut self) {
        while !self.apply_ch.is_closed()
            && self.last_applied.load(Ordering::SeqCst) < self.commit_index.load(Ordering::SeqCst)
        {
            let index = (self.last_applied.load(Ordering::SeqCst)
                - self.last_included_index.load(Ordering::SeqCst)) as usize;
            // let mut apply_ch = self.apply_ch.clone();
            let msg = ApplyMsg {
                command_valid: true,
                command: self.log[index].command.to_owned(),
                command_index: self.last_applied.load(Ordering::SeqCst) + 1,
            };
            self.apply_ch
                .unbounded_send(msg)
                .expect("Unable send ApplyMsg");
            self.last_applied.fetch_add(1, Ordering::SeqCst);
            if self.is_leader.load(Ordering::SeqCst) {
                info!(
                    "{} Apply command: [ApplyMsg {} Term {}]",
                    self,
                    self.last_applied.load(Ordering::SeqCst),
                    self.log[index].term
                );
            }
        }
    }

    fn start(&mut self, command: &[u8]) -> Result<(u64, u64)> {
        let index = self.log.len() as u64 + self.last_included_index.load(Ordering::SeqCst) + 1;
        let term = self.current_term.load(Ordering::SeqCst);
        let is_leader = self.is_leader.load(Ordering::SeqCst);
        // Your code here (2B).

        if is_leader {
            self.log.push(LogEntry {
                command: command.to_owned(),
                index,
                term,
            });
            self.log_index.store(index, Ordering::SeqCst);
            info!(
                "{} Receive a Command! Append to [log {} {}]",
                self, index, term
            );
            self.match_index[self.me] = Arc::new(AtomicU64::new(index as u64));
            self.persist();
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn get_last_log_info(&self) -> (u64, u64) {
        let mut index = self.last_included_index.load(Ordering::SeqCst);
        let mut term = self.last_included_term.load(Ordering::SeqCst);
        index += self.log.len() as u64;
        if !self.log.is_empty() {
            term = self.log.last().unwrap().term;
        }
        (index, term)
    }

    fn become_leader(&mut self, term: u64) {
        self.current_term.store(term, Ordering::SeqCst);
        self.role = RaftRole::Leader;
        self.is_leader.store(true, Ordering::SeqCst);
        let (index, _term) = self.get_last_log_info();
        for i in 0..self.peers.len() {
            self.next_index[i] = Arc::new(AtomicU64::new(index + 1));
            self.match_index[i] = Arc::new(AtomicU64::new(0));
        }
        self.match_index[self.me] = Arc::new(AtomicU64::new(index));
        self.persist();

        let last_included_index = self.last_included_index.load(Ordering::SeqCst);
        self.commit_index
            .store(last_included_index, Ordering::SeqCst);
        self.last_applied
            .store(last_included_index, Ordering::SeqCst);
        let snapshot = self.persister.snapshot();
        let msg = ApplyMsg {
            command_valid: false,
            command: snapshot,
            command_index: 0,
        };
        self.apply_ch
            .unbounded_send(msg)
            .expect("Unable send ApplyMsg");

        info!("{} Become Leader", self);
    }

    fn become_follower(&mut self, term: u64) {
        self.current_term.store(term, Ordering::SeqCst);
        // self.voted_for = None;
        self.role = RaftRole::Follower;
        self.is_leader.store(false, Ordering::SeqCst);
        self.persist();
        debug!("{} Become Follower", self);
    }

    fn become_candidate(&mut self) {
        self.current_term.fetch_add(1, Ordering::SeqCst);
        self.role = RaftRole::Candidate;
        self.is_leader.store(false, Ordering::SeqCst);
        self.voted_for = Some(self.me);
        self.persist();
        debug!("{} Become Candidate", self);

        self.send_request_vote_all();
    }

    fn update_commit_index(&mut self) {
        let last_included_index = self.last_included_index.load(Ordering::SeqCst);
        let last_included_term = self.last_included_term.load(Ordering::SeqCst);
        self.match_index[self.me].store(
            self.log.len() as u64 + last_included_index,
            Ordering::SeqCst,
        );
        let mut match_index_all: Vec<u64> = self
            .match_index
            .iter()
            .map(|v| v.load(Ordering::SeqCst))
            .collect();
        match_index_all.sort_unstable();
        let match_n = match_index_all[self.peers.len() / 2];
        if match_n > self.commit_index.load(Ordering::SeqCst)
            && (match_n == last_included_index
                || self
                    .log
                    .get((match_n - last_included_index - 1) as usize)
                    .map_or(last_included_term, |v| v.term)
                    == self.current_term.load(Ordering::SeqCst))
        {
            debug!("{} Update commit index: {}", self, match_n);
            self.commit_index.store(match_n, Ordering::SeqCst);
        }
        self.send_apply_msg();
    }
}

impl Raft {
    fn last_index(&self) -> u64 {
        self.log
            .last()
            .map_or(self.last_included_index.load(Ordering::SeqCst), |v| v.index)
    }
    fn last_term(&self) -> u64 {
        self.log
            .last()
            .map_or(self.last_included_term.load(Ordering::SeqCst), |v| v.term)
    }
    fn term(&self, idx: u64) -> Option<u64> {
        let last_included_index = self.last_included_index.load(Ordering::SeqCst);
        if idx < last_included_index {
            None
        } else if idx == last_included_index {
            Some(self.last_included_term.load(Ordering::SeqCst))
        } else if idx <= self.last_index() {
            Some(self.log[(idx - last_included_index - 1) as usize].term)
        } else {
            None
        }
    }
    fn match_term(&self, idx: u64, term: u64) -> bool {
        self.term(idx).map(|t| t == term).unwrap_or(false)
    }
    fn is_up_to_date(&self, last_index: u64, term: u64) -> bool {
        term > self.last_term() || (term == self.last_term() && last_index >= self.last_index())
    }
}

impl Raft {
    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel::<Result<RequestVoteReply>>();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        let current_term = self.current_term.load(Ordering::SeqCst);
        if current_term < args.term {
            self.voted_for = None;
            self.become_follower(args.term);
        }

        if args.term < current_term {
            debug!("{} Handle {}, Vote false due to older term", self, args);
            RequestVoteReply {
                term: current_term,
                vote_granted: false,
            }
        } else if self.voted_for.is_some() && self.voted_for != Some(args.candidate_id as usize) {
            debug!(
                "{} Handle {}, Vote false due to already vote for {}",
                self,
                args,
                self.voted_for.unwrap()
            );
            RequestVoteReply {
                term: args.term,
                vote_granted: false,
            }
        } else if !self.is_up_to_date(args.last_log_index, args.last_log_term) {
            debug!("{} Handle {}, Vote false due to older log", self, args);
            RequestVoteReply {
                term: args.term,
                vote_granted: false,
            }
        } else {
            info!("{} Handle {}, Vote true", self, args);
            self.voted_for = Some(args.candidate_id as usize);
            RequestVoteReply {
                term: args.term,
                vote_granted: true,
            }
        }
    }

    fn send_request_vote_all(&mut self) {
        let vote_count = Arc::new(AtomicUsize::new(1));
        let (last_log_index, last_log_term) = self.get_last_log_info();
        let args = RequestVoteArgs {
            term: self.current_term.load(Ordering::SeqCst),
            candidate_id: self.me as i32,
            last_log_index,
            last_log_term,
        };
        // let mut rx_vec = FuturesUnordered::new();
        info!("{} Send {} to ALL Node", self, args);
        let is_candidate = Arc::new(AtomicBool::new(true));
        for server in 0..self.peers.len() {
            if server != self.me {
                let args = args.clone();
                let mut tx = self.sender.clone();
                let peers_num = self.peers.len();
                let is_candidate = is_candidate.clone();
                let term = self.current_term.load(Ordering::SeqCst);
                let vote_count = vote_count.clone();
                // rx_vec.push(self.send_request_vote(server, args));
                let rx = self.send_request_vote(server, args);
                tokio::spawn(async move {
                    if let Ok(reply) = rx.await {
                        if let Ok(reply) = reply {
                            if is_candidate.load(Ordering::SeqCst) {
                                debug!(
                                    "Get one {}, current {}, total {}",
                                    reply,
                                    vote_count.load(Ordering::SeqCst),
                                    peers_num
                                );
                                if reply.term > term {
                                    tx.send(RaftEvent::BecomeFollower(reply.term))
                                        .await
                                        .unwrap();
                                } else if reply.vote_granted {
                                    vote_count.fetch_add(1, Ordering::Relaxed);
                                    if vote_count.load(Ordering::SeqCst) > peers_num / 2 {
                                        is_candidate.store(false, Ordering::SeqCst);
                                        tx.send(RaftEvent::BecomeLeader(reply.term)).await.unwrap();
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }
    }
}

impl Raft {
    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_append_entries(
        &self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel::<Result<AppendEntriesReply>>();
        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let last_included_index = self.last_included_index.load(Ordering::SeqCst);
        let last_included_term = self.last_included_term.load(Ordering::SeqCst);

        let current_term = self.current_term.load(Ordering::SeqCst);

        if current_term < args.term {
            self.voted_for = Some(args.leader_id as usize);
            self.become_follower(args.term);
            debug!(
                "{} Become Follower. New Leader id: {}",
                self, args.leader_id
            );
        }
        if args.term < current_term {
            debug!("{} Handle {}, Success false due to older term", self, args);
            AppendEntriesReply {
                term: current_term,
                success: false,
                conflict_log_index: 0,
                conflict_log_term: 0,
            }
        } else if args.prev_log_index < last_included_index
            || args.prev_log_index == last_included_index
                && args.prev_log_term != last_included_term
        {
            debug!(
                "{} Handle {}, Success false due to Snapshot not match",
                self, args
            );
            AppendEntriesReply {
                term: current_term,
                success: false,
                conflict_log_term: last_included_term,
                conflict_log_index: last_included_index,
            }
        } else if args.prev_log_index > self.last_index()
            || args.prev_log_index > last_included_index
                && !self.match_term(args.prev_log_index, args.prev_log_term)
        {
            debug!(
                "{} Handle {}, Success false due to log not match",
                self, args
            );
            let conflict_log_term = self.term(args.prev_log_index).unwrap_or(last_included_term);
            AppendEntriesReply {
                term: self.current_term.load(Ordering::SeqCst),
                success: false,
                conflict_log_term,
                conflict_log_index: self
                    .log
                    .iter()
                    .filter(|v| v.term == conflict_log_term)
                    .take(1)
                    .next()
                    .map_or(last_included_index, |v| v.index),
            }
        } else {
            debug!("{} Handle {}, Success true", self, args);
            self.log
                .truncate((args.prev_log_index - last_included_index) as usize);
            self.log.extend(args.entries);
            self.persist();
            if args.leader_commit > self.commit_index.load(Ordering::SeqCst) {
                self.commit_index
                    .store(min(args.leader_commit, self.last_index()), Ordering::SeqCst);
            }
            AppendEntriesReply {
                term: self.current_term.load(Ordering::SeqCst),
                success: true,
                conflict_log_index: 0,
                conflict_log_term: 0,
            }
        }
    }

    fn send_append_entries_all(&mut self) {
        // let mut rx_vec = FuturesUnordered::new();
        debug!("{} Send append entries to ALL Node", self);
        let term = self.current_term.load(Ordering::SeqCst);
        // let peers_num = self.peers.len();
        for server in 0..self.peers.len() {
            if server != self.me {
                let match_index = self.match_index[server].clone();
                let next_index = self.next_index[server].clone();
                let mut tx = self.sender.clone();
                let is_leader = self.is_leader.clone();
                let last_included_index = self.last_included_index.load(Ordering::SeqCst);
                let last_included_term = self.last_included_term.load(Ordering::SeqCst);

                let prev_log_index = max(1, self.next_index[server].load(Ordering::SeqCst)) - 1;
                if prev_log_index < last_included_index {
                    let args = InstallSnapshotArgs {
                        term: self.current_term.load(Ordering::SeqCst),
                        leader_id: self.me as i32,
                        last_included_index,
                        last_included_term,
                        offset: 0,
                        data: self.persister.snapshot(),
                        done: true,
                    };
                    debug!("{} Send Node {} {} ", self, server, args);
                    let rx = self.send_install_snapshot(server, args);
                    tokio::spawn(async move {
                        if let Ok(Ok(reply)) = rx.await {
                            if reply.term > term {
                                tx.send(RaftEvent::BecomeFollower(reply.term))
                                    .await
                                    .unwrap();
                            } else {
                                match_index.store(last_included_index, Ordering::SeqCst);
                                next_index.store(last_included_index + 1, Ordering::SeqCst);
                            }
                        }
                    });
                } else {
                    let prev_log_term = {
                        if prev_log_index == last_included_index {
                            last_included_term
                        } else {
                            // self.log[(prev_log_index - 1) as usize].term
                            self.log[(prev_log_index - last_included_index - 1) as usize].term
                        }
                    };
                    // let upper_log_index = min(prev_log_index + 5, self.log.len() as u64);
                    let upper_log_index = last_included_index + self.log.len() as u64;
                    let entries = {
                        if prev_log_index < upper_log_index {
                            self.log[((prev_log_index - last_included_index) as usize)
                                ..((upper_log_index - last_included_index) as usize)]
                                .to_vec()
                        } else {
                            Vec::new()
                        }
                    };
                    let args = AppendEntriesArgs {
                        term: self.current_term.load(Ordering::SeqCst),
                        leader_id: self.me as i32,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit: self.commit_index.load(Ordering::SeqCst),
                    };
                    debug!("{} Send Node {} {} ", self, server, args);
                    // rx_vec.push(self.send_append_entries(server, args));
                    let rx = self.send_append_entries(server, args);
                    tokio::spawn(async move {
                        if let Ok(Ok(reply)) = rx.await {
                            if is_leader.load(Ordering::SeqCst) {
                                if !reply.success && reply.term > term {
                                    is_leader.store(false, Ordering::SeqCst);
                                    tx.send(RaftEvent::BecomeFollower(reply.term))
                                        .await
                                        .unwrap();
                                } else if reply.success {
                                    // info!("recv {}, upper: {}", reply, upper_log_index);
                                    match_index.store(upper_log_index, Ordering::SeqCst);
                                    next_index.store(upper_log_index + 1, Ordering::SeqCst);
                                } else {
                                    next_index.store(reply.conflict_log_index, Ordering::SeqCst);
                                }
                            }
                        }
                    });
                    self.update_commit_index();
                }
                self.update_commit_index();
            }
        }
    }
}

impl Raft {
    fn send_install_snapshot(
        &self,
        server: usize,
        args: InstallSnapshotArgs,
    ) -> Receiver<Result<InstallSnapshotReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel::<Result<InstallSnapshotReply>>();
        peer.spawn(async move {
            let res = peer_clone.install_snapshot(&args).await.map_err(Error::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn handle_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        if self.current_term.load(Ordering::SeqCst) < args.term {
            self.voted_for = Some(args.leader_id as usize);
            self.become_follower(args.term);
            debug!(
                "{} Become Follower. New Leader id: {}",
                self, args.leader_id
            );
        }
        if args.term == self.current_term.load(Ordering::SeqCst)
            && args.last_included_index > self.last_included_index.load(Ordering::SeqCst)
        {
            let range = min(
                self.log.len(),
                (args.last_included_index - self.last_included_index.load(Ordering::SeqCst))
                    as usize,
            );
            self.log.drain(..range);
            self.last_included_index
                .store(args.last_included_index, Ordering::SeqCst);
            self.last_included_term
                .store(args.last_included_term, Ordering::SeqCst);
            self.persist_with_snapshot(args.data.clone());
            self.commit_index
                .fetch_max(args.last_included_index, Ordering::SeqCst);
            self.last_applied
                .fetch_max(args.last_included_index, Ordering::SeqCst);

            let msg = ApplyMsg {
                command_valid: false,
                command: args.data,
                command_index: 0,
            };
            self.apply_ch
                .unbounded_send(msg)
                .expect("Unable send ApplyMsg");
        }

        InstallSnapshotReply {
            term: self.current_term.load(Ordering::SeqCst),
        }
    }
}

enum RaftEvent {
    RequestVote(RequestVoteArgs, Sender<RequestVoteReply>),
    AppendEntries(AppendEntriesArgs, Sender<AppendEntriesReply>),
    InstallSnapshot(InstallSnapshotArgs, Sender<InstallSnapshotReply>),
    BecomeLeader(u64),
    BecomeFollower(u64),
    StartCommand(Vec<u8>, Sender<Result<(u64, u64)>>),
    StartSnapshot(Vec<u8>, u64),
    Shutdown,
}

impl Stream for Raft {
    type Item = ();

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        trace!("{} poll event!", self);
        match self.timeout.poll_unpin(cx) {
            Poll::Ready(()) => {
                return {
                    trace!("{} poll timeout ready!", self);
                    if self.is_leader.load(Ordering::SeqCst) {
                        self.timeout.reset(heartbeat_timeout());
                        self.send_append_entries_all();
                        Poll::Ready(Some(()))
                    } else {
                        trace!("{} loss Leader connection", self);
                        self.timeout.reset(election_timeout());
                        self.become_candidate();
                        Poll::Ready(Some(()))
                    }
                };
            }
            Poll::Pending => {}
        };
        match self.apply_msg_delay.poll_unpin(cx) {
            Poll::Ready(()) => {
                trace!("{} poll Apply Msg ready!", self);
                self.apply_msg_delay.reset(heartbeat_timeout());
                self.send_apply_msg();
                return Poll::Ready(Some(()));
            }
            Poll::Pending => {}
        };
        match self.receiver.poll_next_unpin(cx) {
            Poll::Ready(Some(event)) => match event {
                RaftEvent::RequestVote(args, tx) => {
                    let reply = self.handle_request_vote(args);
                    if reply.vote_granted {
                        self.timeout.reset(election_timeout());
                    }
                    let _ = tx.send(reply);
                    Poll::Ready(Some(()))
                }
                RaftEvent::AppendEntries(args, tx) => {
                    let current_term = args.term;
                    let reply = self.handle_append_entries(args);
                    if reply.success || reply.term == current_term {
                        self.timeout.reset(election_timeout());
                    }
                    let _ = tx.send(reply);
                    Poll::Ready(Some(()))
                }
                RaftEvent::InstallSnapshot(args, tx) => {
                    let reply = self.handle_install_snapshot(args);
                    let _ = tx.send(reply);
                    Poll::Ready(Some(()))
                }
                RaftEvent::BecomeLeader(term) => {
                    self.become_leader(term);
                    self.timeout.reset(heartbeat_timeout());
                    self.send_append_entries_all();
                    Poll::Ready(Some(()))
                }
                RaftEvent::BecomeFollower(term) => {
                    self.become_follower(term);
                    self.timeout.reset(election_timeout());
                    Poll::Ready(Some(()))
                }
                RaftEvent::StartCommand(command, tx) => {
                    debug!("{} Exexutor -- Receive command!", self);
                    let _ = tx.send(self.start(&command));
                    Poll::Ready(Some(()))
                }
                RaftEvent::StartSnapshot(snapshot, last_applied) => {
                    let snapshot_len =
                        (last_applied - self.last_included_index.load(Ordering::SeqCst)) as usize;
                    if snapshot_len > 0 {
                        info!("{} Exexutor -- Receive Snapshot!", self);
                        self.last_included_index
                            .store(last_applied, Ordering::SeqCst);
                        self.last_included_term
                            .store(self.log[snapshot_len - 1].term, Ordering::SeqCst);
                        self.log.drain(..snapshot_len);
                        self.persist_with_snapshot(snapshot);
                        info!("{} Exexutor -- Finish Snapshot!", self);
                    }
                    Poll::Ready(Some(()))
                }
                RaftEvent::Shutdown => Poll::Ready(None),
            },
            Poll::Ready(None) => Poll::Ready(Some(())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    handle: Arc<Mutex<thread::JoinHandle<()>>>,
    me: usize,
    sender: UnboundedSender<RaftEvent>,
    pub term: Arc<AtomicU64>,
    pub is_leader: Arc<AtomicBool>,
    pub raft_state_size: Arc<AtomicU64>,
    log_index: Arc<AtomicU64>,
    commit_index: Arc<AtomicU64>,
    last_applied: Arc<AtomicU64>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        let me = raft.me;
        let sender = raft.sender.clone();
        let term = raft.current_term.clone();
        let is_leader = raft.is_leader.clone();
        let raft_state_size = raft.raft_state_size.clone();
        let commit_index = raft.commit_index.clone();
        let last_applied = raft.last_applied.clone();
        let log_index = raft.log_index.clone();

        // let mut raft_executor = RaftExecutor::new(raft);
        let threaded_rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let handle = thread::Builder::new()
            .name(format!("RaftNode-{}", me))
            .spawn(move || {
                threaded_rt.block_on(async move {
                    debug!("Enter main executor!");
                    while raft.next().await.is_some() {
                        trace!("get event");
                    }
                    debug!("Leave main executor!");
                })
            })
            .unwrap();
        Node {
            handle: Arc::new(Mutex::new(handle)),
            me,
            sender,
            log_index,
            term,
            is_leader,
            raft_state_size,
            commit_index,
            last_applied,
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.is_leader() {
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).unwrap();

            let threaded_rt = Builder::new_multi_thread().build().unwrap();

            let (tx, rx) = channel();
            let sender = self.sender.clone();
            let handle = thread::spawn(move || {
                sender
                    .unbounded_send(RaftEvent::StartCommand(buf, tx))
                    .expect("Unable to send start command to RaftExecutor");

                let fut_values = async { rx.await };
                threaded_rt.block_on(fut_values).unwrap()
            });
            let response = handle.join().unwrap();
            debug!(
                "Node {} -- Start a Command, response with: {:?}",
                self.me, response
            );
            response
        } else {
            debug!("Node {} -- Start a Command but in Not Leader", self.me);
            Err(Error::NotLeader)
        }
    }

    pub fn start_snapshot(&self, snapshot: Vec<u8>, last_applied: u64) {
        self.sender
            .unbounded_send(RaftEvent::StartSnapshot(snapshot, last_applied))
            .expect("Unable to send start Snapshot to RaftExecutor");

        debug!("Node {} -- Start a Snapshot", self.me,);
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // crate::your_code_here(())
        // let raft = self.raft.lock().unwrap();
        // raft.current_term.load(Ordering::SeqCst)
        self.term.load(Ordering::SeqCst)
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // crate::your_code_here(())
        // let raft = self.raft.lock().unwrap();
        // raft.role == RaftRole::Leader
        self.is_leader.load(Ordering::SeqCst)
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        let _ = self.sender.unbounded_send(RaftEvent::Shutdown);
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        let (tx, rx) = channel();
        let event = RaftEvent::RequestVote(args, tx);
        let _ = self.sender.clone().send(event).await;
        let reply = rx.await;
        reply.map_err(labrpc::Error::Recv)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let (tx, rx) = channel();
        let event = RaftEvent::AppendEntries(args, tx);
        let _ = self.sender.clone().send(event).await;
        let reply = rx.await;
        reply.map_err(labrpc::Error::Recv)
    }

    async fn install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> labrpc::Result<InstallSnapshotReply> {
        let (tx, rx) = channel();
        let event = RaftEvent::InstallSnapshot(args, tx);
        let _ = self.sender.clone().send(event).await;
        let reply = rx.await;
        reply.map_err(labrpc::Error::Recv)
    }
}
