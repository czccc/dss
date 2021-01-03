use std::{
    collections::HashMap,
    sync::Arc,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Mutex,
    },
    task::Poll,
    thread,
    time::Duration,
};

use crate::raft::ApplyMsg;
use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot::{channel, Sender},
    },
    prelude::*,
    Stream,
};
use futures_timer::Delay;
use tokio::{runtime::Builder, time::timeout};

use crate::proto::kvraftpb::*;
use crate::raft;

enum PendingOP {
    Get(Sender<GetReply>, String, u64),
    PutAppend(Sender<PutAppendReply>, String, u64),
}

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    apply_ch: UnboundedReceiver<ApplyMsg>,
    sender: UnboundedSender<KvEvent>,
    receiver: UnboundedReceiver<KvEvent>,

    // Clone from Raft
    term: Arc<AtomicU64>,
    is_leader: Arc<AtomicBool>,

    // DB
    db: Mutex<HashMap<String, String>>,
    pending: HashMap<u64, PendingOP>,
    last_index: HashMap<String, Arc<AtomicUsize>>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);
        let rf = raft::Node::new(rf);
        let term = rf.term.clone();
        let is_leader = rf.is_leader.clone();
        let (sender, receiver) = unbounded();

        KvServer {
            rf,
            me,
            maxraftstate,
            apply_ch,
            sender,
            receiver,
            term,
            is_leader,
            db: Mutex::new(HashMap::new()),
            pending: HashMap::new(),
            last_index: HashMap::new(),
        }
    }

    fn desp(&self) -> String {
        if self.is_leader.load(Ordering::SeqCst) {
            String::from("Leader")
        } else {
            String::from("Follow")
        }
    }

    // fn check_client(&self) {}

    fn handle_get(&mut self, args: GetRequest, sender: Sender<GetReply>) {
        info!(
            "[Server {} -> Client {}] recv [GetRequest key: \"{}\" index: {}]",
            self.me, args.name, args.key, args.index
        );
        let res = self.rf.start(&args);
        if res.is_err() {
            let _ = sender.send(GetReply {
                wrong_leader: true,
                err: String::from("NotLeader"),
                value: String::new(),
            });
            return;
        }
        let (index, _term) = res.unwrap();

        let (tx, rx) = channel();
        if let Some(op) = self
            .pending
            .insert(index, PendingOP::Get(tx, args.name.to_owned(), args.index))
        {
            match op {
                PendingOP::Get(tx, _name, _index) => {
                    let _ = tx.send(GetReply {
                        wrong_leader: true,
                        err: String::from("Command uncommitted"),
                        value: String::new(),
                    });
                }
                PendingOP::PutAppend(tx, _name, _index) => {
                    let _ = tx.send(PutAppendReply {
                        wrong_leader: true,
                        err: String::from("Command uncommitted"),
                    });
                }
            }
        }

        tokio::spawn(async {
            let reply = match rx.await {
                Ok(reply) => reply,
                Err(_) => GetReply {
                    wrong_leader: true,
                    err: String::from("Channel Cancelled"),
                    value: String::new(),
                },
            };
            let _ = sender.send(reply);
        });
    }
    fn handle_put_append(&mut self, args: PutAppendRequest, sender: Sender<PutAppendReply>) {
        info!(
            "[{} {} -> Client {}] recv [PutAppendRequest op: {} key: \"{}\" value: \"{}\" index: {}]",
            self.desp(), self.me, args.name, args.op, args.key, args.value, args.index
        );
        let res = self.rf.start(&args);
        if res.is_err() {
            info!(
                "[{} {} -> Client {}] send [PutAppendReply true NotLeader]",
                self.desp(),
                self.me,
                args.name
            );
            let _ = sender.send(PutAppendReply {
                wrong_leader: true,
                err: String::from("NotLeader"),
            });
            return;
        }
        let (index, _term) = res.unwrap();

        let (tx, rx) = channel();
        if let Some(op) = self.pending.insert(
            index,
            PendingOP::PutAppend(tx, args.name.to_owned(), args.index),
        ) {
            match op {
                PendingOP::Get(tx, _name, _index) => {
                    let _ = tx.send(GetReply {
                        wrong_leader: true,
                        err: String::from("Command uncommitted"),
                        value: String::new(),
                    });
                }
                PendingOP::PutAppend(tx, _name, _index) => {
                    let _ = tx.send(PutAppendReply {
                        wrong_leader: true,
                        err: String::from("Command uncommitted"),
                    });
                }
            }
        }
        tokio::spawn(async {
            let reply = match timeout(Duration::from_millis(2000), rx).await {
                Ok(Ok(reply)) => {
                    // self.la
                    reply
                }
                Ok(Err(_)) => PutAppendReply {
                    wrong_leader: true,
                    err: String::from("Timeout"),
                },
                Err(_) => PutAppendReply {
                    wrong_leader: true,
                    err: String::from("Channel Cancelled"),
                },
            };
            let _ = sender.send(reply);
        });
    }
    fn handle_apply_msg(&mut self, msg: ApplyMsg) {
        if !msg.command_valid {
            return;
        }
        info!(
            "Server {} recv [ApplyMsg {:?} {}]",
            self.me, msg.command, msg.command_index
        );
        let index = msg.command_index;
        if let Some(op) = self.pending.remove(&index) {
            match op {
                PendingOP::Get(tx, name, index) => {
                    if let Ok(o) = labcodec::decode::<GetRequest>(&msg.command) {
                        info!(
                            "{} {} ApplyMsg decode to [GetRequest {} {} {}]",
                            self.desp(),
                            self.me,
                            o.key,
                            o.name,
                            o.index
                        );
                        if name == o.name && index == o.index {
                            let value = self
                                .db
                                .lock()
                                .unwrap()
                                .get(&o.key)
                                .map_or(String::new(), |v| v.to_owned());
                            let _ = tx.send(GetReply {
                                wrong_leader: false,
                                err: String::new(),
                                value,
                            });
                        }
                    }
                }
                PendingOP::PutAppend(tx, name, index) => {
                    if let Ok(o) = labcodec::decode::<PutAppendRequest>(&msg.command) {
                        info!(
                            "{} {} ApplyMsg decode to [PutAppendRequest {} {} {} {} {}]",
                            self.desp(),
                            self.me,
                            o.op,
                            o.key,
                            o.value,
                            o.name,
                            o.index
                        );
                        if name == o.name && index == o.index {
                            let value = self
                                .db
                                .lock()
                                .unwrap()
                                .get(&o.key)
                                .map_or(String::new(), |v| v.to_owned());
                            let value = match o.op {
                                1 => o.value,
                                2 => value + &o.value,
                                _ => panic!("Unknown Op"),
                            };
                            self.db.lock().unwrap().insert(o.key, value);
                            let _ = tx.send(PutAppendReply {
                                wrong_leader: false,
                                err: String::new(),
                            });
                        }
                    }
                }
            }
        }
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
        let _ = &self.last_index;
        crate::your_code_here(self);
    }
}

#[derive(Debug)]
enum KvEvent {
    Get(GetRequest, Sender<GetReply>),
    PutAppend(PutAppendRequest, Sender<PutAppendReply>),
    Shutdown,
}

struct KvExecutor {
    server: KvServer,
}

impl KvExecutor {
    fn new(kv: KvServer) -> KvExecutor {
        KvExecutor { server: kv }
    }
}

impl Stream for KvExecutor {
    type Item = ();

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.server.receiver.poll_next_unpin(cx) {
            Poll::Ready(Some(event)) => {
                info!(
                    "[{} {}] Executor recv [Event]",
                    self.server.desp(),
                    self.server.me
                );
                return match event {
                    KvEvent::Get(args, tx) => {
                        info!(
                            "[{} {} -> Client {}] Executor recv [GetEvent {} {}]",
                            self.server.desp(),
                            self.server.me,
                            args.name,
                            args.key,
                            args.index
                        );
                        self.server.handle_get(args, tx);
                        // let _ = tx.send(reply);
                        Poll::Ready(Some(()))
                    }
                    KvEvent::PutAppend(args, tx) => {
                        info!(
                            "[{} {} -> Client {}] Executor recv [PutAppendEvent {} {} {}]",
                            self.server.desp(),
                            self.server.me,
                            args.name,
                            args.key,
                            args.value,
                            args.index
                        );
                        self.server.handle_put_append(args, tx);
                        Poll::Ready(Some(()))
                    }
                    KvEvent::Shutdown => Poll::Ready(None),
                };
            }
            Poll::Ready(None) => {}
            Poll::Pending => {}
        }
        match self.server.apply_ch.poll_next_unpin(cx) {
            Poll::Ready(Some(msg)) => {
                self.server.handle_apply_msg(msg);
                Poll::Ready(Some(()))
            }
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
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    handle: Arc<Mutex<thread::JoinHandle<()>>>,
    me: usize,
    sender: UnboundedSender<KvEvent>,
    term: Arc<AtomicU64>,
    is_leader: Arc<AtomicBool>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        // crate::your_code_here(kv);
        let me = kv.me;
        let sender = kv.sender.clone();
        let term = kv.term.clone();
        let is_leader = kv.is_leader.clone();

        let mut raft_executor = KvExecutor::new(kv);
        let threaded_rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let handle = thread::spawn(move || {
            threaded_rt.block_on(async move {
                debug!("Enter KvRaft main executor!");
                while raft_executor.next().await.is_some() {
                    trace!("KvRaft: Get event");
                }
                debug!("Leave KvRaft main executor!");
            })
        });
        Node {
            handle: Arc::new(Mutex::new(handle)),
            me,
            sender,
            term,
            is_leader,
        }
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
        let _ = self.sender.unbounded_send(KvEvent::Shutdown);
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        raft::State {
            term: self.term.load(Ordering::SeqCst),
            is_leader: self.is_leader.load(Ordering::SeqCst),
        }
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        // Your code here.
        // crate::your_code_here(arg)
        let (tx, rx) = channel();
        let event = KvEvent::Get(arg, tx);
        let _ = self.sender.clone().send(event).await;
        // info!("Server {} recv service Get", self.me);
        tokio::select! {
            _ = Delay::new(Duration::from_millis(2000)) => {Err(labrpc::Error::Timeout)}
            reply = rx => {reply.map_err(labrpc::Error::Recv)}
        }
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        // Your code here.
        // crate::your_code_here(arg)
        // info!(
        //     "[Server {} -> Client {}] recv service PutAppend",
        //     self.me, arg.name
        // );
        let (tx, rx) = channel();
        let event = KvEvent::PutAppend(arg, tx);
        let _ = self.sender.clone().send(event).await;
        tokio::select! {
            _ = Delay::new(Duration::from_millis(2000)) => {Err(labrpc::Error::Timeout)}
            reply = rx => {reply.map_err(labrpc::Error::Recv)}
        }
    }
}
