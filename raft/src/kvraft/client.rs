use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use futures_timer::Delay;
use tokio::runtime::Builder;

use crate::proto::kvraftpb::*;

// enum Op {
//     Put(String, String),
//     Append(String, String),
// }

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
    pub last_leader: Arc<AtomicUsize>,
    seq: Arc<AtomicU64>,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl fmt::Display for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[Client {} Seq {}]",
            self.name,
            self.seq.load(Ordering::SeqCst)
        )
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk {
            name,
            servers,
            last_leader: Arc::new(AtomicUsize::new(0)),
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    fn handle_request(&self, args: KvRequest) -> KvReply {
        let last_leader = self.last_leader.clone();
        let mut leader = last_leader.load(Ordering::SeqCst);
        let server_num = self.servers.len();

        let threaded_rt = Builder::new_multi_thread().enable_all().build().unwrap();
        loop {
            debug!("{} send [Server {}]  {}", self, leader, args);
            if let Ok(Ok(reply)) = threaded_rt.block_on(async {
                tokio::select! {
                    _ = Delay::new(Duration::from_millis(4000)) => {
                        Err("Timeout")
                    }
                    reply = self.servers[leader].request(&args) => {
                        Ok(reply)
                    }
                }
            }) {
                debug!("{} recv [Server {}] {}", self, leader, reply);
                if reply.success {
                    last_leader.store(leader, Ordering::SeqCst);
                    return reply;
                }
            }
            thread::sleep(Duration::from_millis(500));
            leader = (leader + 1) % server_num;
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { do something  }
    pub fn get(&self, key: String) -> String {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let args = KvRequest {
            op: 3,
            key,
            value: String::new(),
            name: self.name.to_owned(),
            seq,
        };
        info!("{} Get Request {}", self, args);
        let reply = self.handle_request(args);
        info!("{} Get Success {}", self, reply);
        reply.value
    }

    pub fn put(&self, key: String, value: String) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let args = KvRequest {
            op: 1,
            key,
            value,
            name: self.name.to_owned(),
            seq,
        };
        info!("{} Put Request {}", self, args);
        let reply = self.handle_request(args);
        info!("{} Put Success {}", self, reply);
    }

    pub fn append(&self, key: String, value: String) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let args = KvRequest {
            op: 2,
            key,
            value,
            name: self.name.to_owned(),
            seq,
        };
        info!("{} Append Request {}", self, args);
        let reply = self.handle_request(args);
        info!("{} Append Success {}", self, reply);
    }
}
