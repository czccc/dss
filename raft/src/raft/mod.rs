use std::{
    sync::Arc,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Mutex,
    },
    // thread,
    time::{Duration, Instant},
};

// use futures::Stream;
use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot::{channel, Receiver, Sender},
    },
    prelude::*,
    Future, Stream,
};
use futures_timer::Delay;
use rand::Rng;

const RAFT_ELECTION_TIMEOUT: Duration = Duration::from_millis(500);
const RAFT_HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(100);

fn election_timeout() -> Duration {
    let variant = rand::thread_rng().gen_range(0, 100);
    RAFT_ELECTION_TIMEOUT + Duration::from_millis(variant)
}

fn heartbeat_timeout() -> Duration {
    let variant = rand::thread_rng().gen_range(0, 50);
    RAFT_HEARTBEAT_TIMEOUT + Duration::from_millis(variant)
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

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
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

#[derive(Message)]
pub struct Persistent {
    #[prost(uint64, tag = "1")]
    current_term: u64,
    #[prost(int32, tag = "2")]
    voted_for: i32,
    #[prost(bytes, tag = "3")]
    log: Vec<u8>,
}

#[derive(PartialEq)]
enum Role {
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
    current_term: Arc<AtomicU64>,
    voted_for: Option<usize>,
    log: Vec<u8>,
    commit_index: usize,
    last_applied: usize,

    next_index: Vec<u8>,
    match_index: Vec<u8>,

    role: Role,
    last_update_time: Instant,

    sender: Option<UnboundedSender<RaftEvent>>,
    receiver: Option<UnboundedReceiver<RaftEvent>>,
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

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            // state: Arc::default(),
            current_term: Arc::new(AtomicU64::new(0)),
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,

            next_index: vec![],
            match_index: vec![],

            role: Role::Follower,
            last_update_time: Instant::now(),
            sender: None,
            receiver: None,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        let _ = apply_ch;

        info!(
            "[Server {} Follower ] [Term {}] [Log len {}] created!",
            me,
            rf.current_term.load(Ordering::SeqCst),
            rf.log.len()
        );

        // thread::spawn(|| tokio::spawn(rf.run()));
        // rf.run();

        // crate::your_code_here((rf, apply_ch))
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
        };
        labcodec::encode(&per, &mut data).unwrap();
        self.persister.save_raft_state(data);
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
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

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
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        // let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        // crate::your_code_here((server, args, tx, rx))
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel::<Result<RequestVoteReply>>();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            tx.send(res).unwrap();
        });
        rx
    }

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
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        // let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        // crate::your_code_here((server, args, tx, rx))
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel::<Result<AppendEntriesReply>>();
        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            tx.send(res).unwrap();
        });
        rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn log(&self, description: &str) {
        let role = match self.role {
            Role::Follower => "Follower ",
            Role::Candidate => "Candidate",
            Role::Leader => "Leader   ",
        };
        info!(
            "[Server {} {}] [Term {}] [Log len {}] {}",
            self.me,
            role,
            self.current_term.load(Ordering::SeqCst),
            self.log.len(),
            description
        );
    }

    async fn run(&mut self) {
        loop {
            self.log("Started!");
            match self.role {
                Role::Follower | Role::Candidate => {
                    println!("aaa");
                    self.log("1!");
                    let random_timeout = election_timeout();
                    // sleep(random_timeout);
                    Delay::new(random_timeout).await;
                    self.log("After election timeout!");
                    if self.role != Role::Leader {
                        let last = self.last_update_time.elapsed();
                        if last > random_timeout {
                            self.log("Missing Leader!");
                            // let node_clone = self.clone();
                            // thread::spawn(move || node_clone.become_candidate());
                            async {
                                self.become_candidate();
                            }
                            .await;
                        }
                    }
                }
                Role::Leader => {
                    let random_timeout = heartbeat_timeout();
                    // sleep(random_timeout);
                    Delay::new(random_timeout).await;
                    for server in 0..self.peers.len() {
                        let args = AppendEntriesArgs {
                            term: self.current_term.load(Ordering::SeqCst),
                            leader_id: self.me as i32,
                        };
                        let rx = self.send_append_entries(server, args);
                        rx.await.unwrap().unwrap();
                    }
                }
            };
        }
    }

    fn become_leader(&mut self) {
        self.voted_for = None;
        self.last_update_time = Instant::now();
        self.role = Role::Leader;
        self.log("Become Leader");
    }
    fn become_follower(&mut self, term: u64) {
        self.current_term.store(term, Ordering::SeqCst);
        self.voted_for = None;
        self.last_update_time = Instant::now();
        self.role = Role::Follower;
        self.log("Become Follower");
    }
    fn become_candidate(&mut self) {
        self.current_term.fetch_add(1, Ordering::SeqCst);
        self.voted_for = Some(self.me);
        self.last_update_time = Instant::now();
        self.role = Role::Candidate;
        self.log("Become Candidate");

        // let mut recv = vec![];
        let vote_count = Arc::new(AtomicUsize::new(0));
        for server in 0..self.peers.len() {
            if server != self.me {
                let vote_count = vote_count.clone();
                async {
                    let args = RequestVoteArgs {
                        term: self.current_term.load(Ordering::SeqCst),
                        candidate_id: self.me as i32,
                    };
                    let rx = self.send_request_vote(server, args);
                    let reply = rx.await.unwrap();
                    if let Ok(reply) = reply {
                        if self.role == Role::Candidate {
                            if reply.term > self.current_term.load(Ordering::SeqCst) {
                                self.become_follower(reply.term);
                            } else if reply.vote_granted {
                                vote_count.fetch_add(1, Ordering::Relaxed);
                                if vote_count.load(Ordering::SeqCst) >= self.peers.len() / 2 {
                                    self.become_leader();
                                }
                            }
                        }
                    }
                };
            }
        }

        // for rx in recv {
        //     let node_clone = self.clone();
        //     let vote_count = vote_count.clone();
        //     let stream = Stream::futures_unordered(recv)
        //         .take_while(move |reply| {
        //             let reply: Result<RequestVoteReply> = reply.recv().unwrap();
        //             if let Ok(reply) = reply {
        //                 if self.role == Role::Candidate {
        //                     if reply.term > self.current_term {
        //                         node_clone.become_follower(reply.term);
        //                     } else if reply.vote_granted {
        //                         vote_count.fetch_add(1, Ordering::Relaxed);
        //                         if vote_count.load(Ordering::Relaxed) >= self.peers.len() / 2 {
        //                             node_clone.become_leader();
        //                         }
        //                     }
        //                 }
        //             }
        //         })
        //         .for_each(|_| Ok(()))
        //         // Canceled Err on the send side should not appear.
        //         .map_err(|_| ());
        //     tokio::spawn(stream);
        //     // thread::spawn(move || {
        //     //     let reply: Result<RequestVoteReply> = rx.recv().unwrap();
        //     //     if let Ok(reply) = reply {
        //     //         if self.role == Role::Candidate {
        //     //             if reply.term > self.current_term {
        //     //                 node_clone.become_follower(reply.term);
        //     //             } else if reply.vote_granted {
        //     //                 vote_count.fetch_add(1, Ordering::Relaxed);
        //     //                 if vote_count.load(Ordering::Relaxed) >= self.peers.len() / 2 {
        //     //                     node_clone.become_leader();
        //     //                 }
        //     //             }
        //     //         }
        //     //     }
        //     // });
        // }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        // let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
        let _ = &self.commit_index;
        let _ = &self.last_applied;
        let _ = &self.match_index;
        let _ = &self.next_index;
    }
}

enum RaftEvent {
    RequestVote(RequestVoteArgs),
    AppendEntries(AppendEntriesArgs),
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
    // raft: Arc<Mutex<Raft>>,
    sender: UnboundedSender<RaftEvent>,
    receiver: Arc<UnboundedReceiver<RaftEvent>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        // crate::your_code_here(raft)
        let (node_tx, raft_rx) = unbounded();
        let (raft_tx, node_rx) = unbounded();
        // let raft = Arc::new(Mutex::new(raft));
        // let raft_clone = raft.clone();
        let node = Node {
            sender: node_tx,
            receiver: Arc::new(node_rx),
        };
        // let node_clone = node.clone();
        // async move { raft_clone.run() };
        // tokio::spawn(async move { node_clone.raft.lock().unwrap().run() });
        node
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
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        crate::your_code_here(())
        // let raft = self.raft.lock().unwrap();
        // raft.current_term.load(Ordering::SeqCst)
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        crate::your_code_here(())
        // let raft = self.raft.lock().unwrap();
        // raft.role == Role::Leader
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
        // Your code here, if desired.
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        crate::your_code_here(args)
        // let raft = self.raft.clone();
        // let current_term = self.term();
        // let voted_for = self.raft.lock().unwrap().voted_for;
        // if args.term < current_term {
        //     Ok(RequestVoteReply {
        //         term: current_term,
        //         vote_granted: false,
        //     })
        // } else if args.term == current_term
        //     && voted_for.is_some()
        //     && voted_for != Some(args.candidate_id as usize)
        // {
        //     Ok(RequestVoteReply {
        //         term: args.term,
        //         vote_granted: false,
        //     })
        // } else {
        //     self.raft.lock().unwrap().voted_for = Some(args.candidate_id as usize);
        //     // *(self.raft.lock().unwrap().current_term.get_mut()) = args.term;
        //     Ok(RequestVoteReply {
        //         term: args.term,
        //         vote_granted: true,
        //     })
        // }
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        // let mut raft = self.raft.lock().unwrap();
        // raft.last_update_time = Instant::now();
        // Ok(AppendEntriesReply {
        //     term: args.term,
        //     success: false,
        // })
        crate::your_code_here(args)
    }
}
