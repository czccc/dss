use std::{
    collections::HashMap,
    sync::Arc,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
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

// enum PendingOP {
//     Get(Sender<KvReply>, String, u64),
//     Put(Sender<KvReply>, String, u64),
//     Append(Sender<KvReply>, String, u64),
// }

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
    pending: HashMap<u64, (Sender<KvReply>, KvRequest)>,
    last_index: HashMap<String, Arc<AtomicU64>>,
}

impl std::fmt::Display for KvServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let desp = if self.is_leader.load(Ordering::SeqCst) {
            String::from("Leader")
        } else {
            String::from("Follow")
        };
        write!(f, "[{} {}]", desp, self.me)
    }
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

    // fn check_client(&self) {}
    fn handle_request(&mut self, args: KvRequest, sender: Sender<KvReply>) {
        if self.is_leader.load(Ordering::SeqCst) {
            info!("{} recv {}", self, args);
        } else {
            debug!("{} recv {}", self, args);
        }
        let reply_template = KvReply {
            op: args.op,
            success: false,
            err: String::new(),
            key: args.key.to_owned(),
            value: String::new(),
            name: args.name.to_owned(),
            seq: args.seq.to_owned(),
        };
        if !self.last_index.contains_key(&args.name) {
            self.last_index
                .insert(args.name.clone(), Arc::new(AtomicU64::new(0)));
        }
        let last_index = self.last_index[&args.name].clone();
        if args.seq < last_index.load(Ordering::SeqCst) {
            let _ = sender.send(KvReply {
                err: String::from("Duplicated Request"),
                ..reply_template
            });
            return;
        }
        let res = self.rf.start(&args);
        if res.is_err() {
            let _ = sender.send(KvReply {
                err: String::from("NotLeader"),
                ..reply_template
            });
            return;
        }
        let (index, _term) = res.unwrap();

        let (tx, rx) = channel();
        self.pending.insert(index, (tx, args));

        tokio::spawn(async move {
            let reply = match timeout(Duration::from_millis(3000), rx).await {
                Ok(Ok(reply)) => {
                    last_index.store(reply.seq, Ordering::SeqCst);
                    reply
                }
                Ok(Err(_)) => KvReply {
                    err: String::from("Timeout"),
                    ..reply_template
                },
                Err(_) => KvReply {
                    err: String::from("Channel Cancelled"),
                    ..reply_template
                },
            };
            let _ = sender.send(reply);
        });
    }

    fn handle_apply_msg(&mut self, msg: ApplyMsg) {
        if !msg.command_valid {
            return;
        }
        debug!(
            "{} recv [ApplyMsg {} {}]",
            self, msg.command_valid, msg.command_index
        );
        let index = msg.command_index;
        if let Ok(req) = labcodec::decode::<KvRequest>(&msg.command) {
            if !self.last_index.contains_key(&req.name) {
                self.last_index
                    .insert(req.name.clone(), Arc::new(AtomicU64::new(0)));
            }
            if req.seq > self.last_index[&req.name].load(Ordering::SeqCst) {
                self.last_index[&req.name].store(req.seq, Ordering::SeqCst);
                match req.op {
                    1 => {
                        self.db
                            .lock()
                            .unwrap()
                            .insert(req.key.clone(), req.value.clone());
                    }
                    2 => {
                        let mut value = self
                            .db
                            .lock()
                            .unwrap()
                            .get(&req.key)
                            .map_or(String::new(), |v| v.clone());
                        value.push_str(&req.value);
                        self.db.lock().unwrap().insert(req.key.clone(), value);
                    }
                    _ => {}
                }
            }
            if let Some((tx, args)) = self.pending.remove(&index) {
                let reply = KvReply {
                    op: args.op,
                    success: false,
                    err: String::new(),
                    key: args.key.to_owned(),
                    value: String::new(),
                    name: args.name.to_owned(),
                    seq: args.seq.to_owned(),
                };
                if req != args {
                    let _ = tx.send(KvReply {
                        err: String::from("Command uncommitted"),
                        ..reply
                    });
                } else if req.seq < self.last_index[&req.name].load(Ordering::SeqCst) {
                    let _ = tx.send(KvReply {
                        err: String::from("Duplicated Request"),
                        ..reply
                    });
                } else {
                    match args.op {
                        1 | 2 | 3 => {
                            let value = self
                                .db
                                .lock()
                                .unwrap()
                                .get(&args.key)
                                .map_or(String::new(), |v| v.to_owned());
                            let _ = tx.send(KvReply {
                                success: true,
                                value,
                                ..reply
                            });
                        }
                        _ => {
                            let _ = tx.send(KvReply {
                                err: String::from("Unknown Request"),
                                ..reply
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
    Request(KvRequest, Sender<KvReply>),
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
                debug!("{} Executor recv [Event]", self.server);
                return match event {
                    KvEvent::Request(args, tx) => {
                        debug!(
                            "{} -> [Client {}] Executor recv {}",
                            self.server, args.name, args
                        );
                        self.server.handle_request(args, tx);
                        Poll::Ready(Some(()))
                    }
                    KvEvent::Shutdown => {
                        self.server.rf.kill();
                        Poll::Ready(None)
                    }
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
        let handle = thread::Builder::new()
            .name(format!("KvServerNode-{}", me))
            .spawn(move || {
                threaded_rt.block_on(async move {
                    debug!("Enter KvRaft main executor!");
                    while raft_executor.next().await.is_some() {
                        trace!("KvRaft: Get event");
                    }
                    debug!("Leave KvRaft main executor!");
                })
            })
            .unwrap();
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
    async fn request(&self, args: KvRequest) -> labrpc::Result<KvReply> {
        let (tx, rx) = channel();
        let event = KvEvent::Request(args, tx);
        let _ = self.sender.clone().send(event).await;
        tokio::select! {
            _ = Delay::new(Duration::from_millis(3000)) => {
                Err(labrpc::Error::Timeout)
            }
            reply = rx => {
                reply.map_err(labrpc::Error::Recv)
            }
        }
        // rx.await.map_err(labrpc::Error::Recv)
    }
}
