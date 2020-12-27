use std::{
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
    stream::FuturesUnordered,
    Stream,
};
use futures_timer::Delay;
use rand::Rng;
use tokio::runtime::Builder;

const RAFT_ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);
const RAFT_HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(300);

fn election_timeout() -> Duration {
    let variant = rand::thread_rng().gen_range(0, 500);
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
    current_term: Arc<AtomicU64>,
    is_leader: Arc<AtomicBool>,
    voted_for: Option<usize>,
    log: Vec<u8>,
    commit_index: usize,
    last_applied: usize,

    next_index: Vec<u8>,
    match_index: Vec<u8>,

    role: RaftRole,

    sender: UnboundedSender<RaftEvent>,
    receiver: UnboundedReceiver<RaftEvent>,
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

        let (sender, receiver) = unbounded();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            // state: Arc::default(),
            current_term: Arc::new(AtomicU64::new(0)),
            is_leader: Arc::new(AtomicBool::new(false)),
            role: RaftRole::Follower,
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,

            next_index: vec![],
            match_index: vec![],

            sender,
            receiver,
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
        // self.log(&format!("!!send request vote to {}", server));
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel::<Result<RequestVoteReply>>();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            tx.send(res).unwrap();
        });
        rx
    }

    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        let current_term = self.current_term.load(Ordering::SeqCst);
        let voted_for = self.voted_for;
        let reply = {
            if args.term < current_term {
                RequestVoteReply {
                    term: current_term,
                    vote_granted: false,
                }
            } else if args.term == current_term
                && voted_for.is_some()
                && voted_for != Some(args.candidate_id as usize)
            {
                RequestVoteReply {
                    term: args.term,
                    vote_granted: false,
                }
            } else {
                self.voted_for = Some(args.candidate_id as usize);
                RequestVoteReply {
                    term: args.term,
                    vote_granted: true,
                }
            }
        };
        self.log(&format!(
            "Handle request vote from Node {}, Vote {}",
            args.candidate_id, reply.vote_granted
        ));
        reply
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

    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let current_term = self.current_term.load(Ordering::SeqCst);
        let reply = {
            if args.term < current_term {
                AppendEntriesReply {
                    term: current_term,
                    success: false,
                }
            } else {
                if self.role != RaftRole::Follower || current_term < args.term {
                    self.current_term.store(args.term, Ordering::SeqCst);
                    self.voted_for = Some(args.leader_id as usize);
                    self.role = RaftRole::Follower;
                    self.is_leader.store(false, Ordering::SeqCst);
                    self.log(&format!(
                        "Become Follower. New Leader id: {}",
                        args.leader_id
                    ));
                }
                AppendEntriesReply {
                    term: args.term,
                    success: true,
                }
            }
        };
        self.log(&format!(
            "Handle append entries from Node {}, Success {}",
            args.leader_id, reply.success
        ));
        reply
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
            RaftRole::Follower => "Follower ",
            RaftRole::Candidate => "Candidate",
            RaftRole::Leader => "Leader   ",
        };
        info!(
            "[{} {}] [Log {}] [Term {}] {}",
            role,
            self.me,
            self.log.len(),
            self.current_term.load(Ordering::SeqCst),
            description
        );
    }

    fn become_leader(&mut self, term: u64) {
        self.current_term.store(term, Ordering::SeqCst);
        self.role = RaftRole::Leader;
        self.is_leader.store(true, Ordering::SeqCst);
        self.log("Become Leader");
    }
    fn become_follower(&mut self, term: u64) {
        self.current_term.store(term, Ordering::SeqCst);
        // self.voted_for = None;
        self.role = RaftRole::Follower;
        self.is_leader.store(false, Ordering::SeqCst);
        self.log("Become Follower");
    }
    fn become_candidate(&mut self) {
        self.current_term.fetch_add(1, Ordering::SeqCst);
        self.voted_for = Some(self.me);
        self.role = RaftRole::Candidate;
        self.is_leader.store(false, Ordering::SeqCst);
        self.log("Become Candidate");

        self.send_request_vote_all();
    }
    fn send_request_vote_all(&mut self) {
        // let mut recv = vec![];
        let vote_count = Arc::new(AtomicUsize::new(1));
        let args = RequestVoteArgs {
            term: self.current_term.load(Ordering::SeqCst),
            candidate_id: self.me as i32,
        };
        let mut rx_vec = FuturesUnordered::new();
        self.log("Send request vote to ALL Node");
        for server in 0..self.peers.len() {
            if server != self.me {
                let args = args.clone();
                rx_vec.push(self.send_request_vote(server, args));
            }
        }
        let mut tx = self.sender.clone();
        let peers_num = self.peers.len();
        let mut role = self.role.clone();
        let term = self.current_term.load(Ordering::SeqCst);
        tokio::spawn(async move {
            while let Some(reply) = rx_vec.next().await {
                if let Ok(reply) = reply.unwrap() {
                    if role == RaftRole::Candidate {
                        debug!(
                            "Get one request vote reply {} - {}, current {}, total {}",
                            reply.term,
                            reply.vote_granted,
                            vote_count.load(Ordering::SeqCst),
                            peers_num
                        );
                        if reply.term > term {
                            tx.send(RaftEvent::BecomeFollower(reply.term))
                                .await
                                .unwrap();
                        } else if reply.vote_granted {
                            vote_count.fetch_add(1, Ordering::Relaxed);
                            if vote_count.load(Ordering::SeqCst) >= peers_num / 2 {
                                role = RaftRole::Leader;
                                tx.send(RaftEvent::BecomeLeader(reply.term)).await.unwrap();
                            }
                        }
                    }
                }
            }
        });
    }
    fn send_append_entries_all(&mut self) {
        // let mut recv = vec![];
        let args = AppendEntriesArgs {
            term: self.current_term.load(Ordering::SeqCst),
            leader_id: self.me as i32,
        };
        let mut rx_vec = FuturesUnordered::new();
        self.log("Send append entries to ALL Node");
        for server in 0..self.peers.len() {
            if server != self.me {
                let args = args.clone();
                rx_vec.push(self.send_append_entries(server, args));
            }
        }
        let mut tx = self.sender.clone();
        let role = self.role.clone();
        let term = self.current_term.load(Ordering::SeqCst);
        tokio::spawn(async move {
            while let Some(reply) = rx_vec.next().await {
                // self.log(&format!("recv append entries from Node {}", server));
                if let Ok(reply) = reply.unwrap() {
                    if role == RaftRole::Leader && !reply.success && reply.term > term {
                        tx.send(RaftEvent::BecomeFollower(reply.term))
                            .await
                            .unwrap();
                    }
                }
            }
        });
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
    RequestVote(RequestVoteArgs, Sender<RequestVoteReply>),
    AppendEntries(AppendEntriesArgs, Sender<AppendEntriesReply>),
    BecomeLeader(u64),
    BecomeFollower(u64),
    Shutdown,
}

struct RaftExecutor {
    raft: Raft,
    timeout: Delay,
}

impl RaftExecutor {
    fn new(raft: Raft) -> RaftExecutor {
        RaftExecutor {
            raft,
            timeout: Delay::new(election_timeout()),
        }
    }
}

impl Stream for RaftExecutor {
    type Item = ();

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        debug!("Node {} poll event!", self.raft.me);
        match self.timeout.poll_unpin(cx) {
            Poll::Ready(()) => {
                return {
                    debug!("Node {} poll timeout ready!", self.raft.me);
                    if self.raft.is_leader.load(Ordering::SeqCst) {
                        self.timeout.reset(heartbeat_timeout());
                        self.raft.send_append_entries_all();
                        Poll::Ready(Some(()))
                    } else {
                        self.raft.log("loss Leader connection");
                        self.timeout.reset(election_timeout());
                        self.raft.become_candidate();
                        Poll::Ready(Some(()))
                    }
                };
            }
            Poll::Pending => {}
        };
        match self.raft.receiver.poll_next_unpin(cx) {
            Poll::Ready(Some(event)) => match event {
                RaftEvent::RequestVote(args, tx) => {
                    let reply = self.raft.handle_request_vote(args);
                    let _ = tx.send(reply);
                    Poll::Ready(Some(()))
                }
                RaftEvent::AppendEntries(args, tx) => {
                    let reply = self.raft.handle_append_entries(args);
                    if reply.success {
                        self.timeout.reset(election_timeout());
                    }
                    let _ = tx.send(reply);
                    Poll::Ready(Some(()))
                }
                RaftEvent::BecomeLeader(term) => {
                    self.raft.become_leader(term);
                    self.timeout.reset(heartbeat_timeout());
                    self.raft.send_append_entries_all();
                    Poll::Ready(Some(()))
                }
                RaftEvent::BecomeFollower(term) => {
                    self.raft.become_follower(term);
                    self.timeout.reset(election_timeout());
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
    sender: UnboundedSender<RaftEvent>,
    handle: Arc<Mutex<thread::JoinHandle<()>>>,
    term: Arc<AtomicU64>,
    is_leader: Arc<AtomicBool>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        // crate::your_code_here(raft)
        // let raft = Arc::new(Mutex::new(raft));
        // let raft_clone = raft.clone();
        let sender = raft.sender.clone();
        let term = raft.current_term.clone();
        let is_leader = raft.is_leader.clone();
        let mut executor = RaftExecutor::new(raft);
        let threaded_rt = Builder::new_multi_thread().build().unwrap();
        let handle = thread::spawn(move || {
            threaded_rt.block_on(async move {
                info!("Enter main executor!");
                while executor.next().await.is_some() {
                    // info!("get event");
                }
                info!("Leave main executor!");
            })
        });
        Node {
            sender,
            handle: Arc::new(Mutex::new(handle)),
            term,
            is_leader,
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
        // Your code here, if desired.
        let threaded_rt = Builder::new_multi_thread().build().unwrap();
        let mut sender = self.sender.clone();
        let handle = thread::spawn(move || threaded_rt.block_on(sender.send(RaftEvent::Shutdown)));
        let _ = handle.join();
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        // crate::your_code_here(args)
        // info!("RaftService receive request vote");
        let (tx, rx) = channel();
        let event = RaftEvent::RequestVote(args, tx);
        let _ = self.sender.clone().send(event).await;
        let reply = rx.await;
        reply.map_err(labrpc::Error::Recv)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        // crate::your_code_here(args)
        // info!("RaftService receive append entries");
        let (tx, rx) = channel();
        let event = RaftEvent::AppendEntries(args, tx);
        let _ = self.sender.clone().send(event).await;
        let reply = rx.await;
        reply.map_err(labrpc::Error::Recv)
    }
}
