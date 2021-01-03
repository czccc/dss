use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use tokio::runtime::Builder;

use crate::proto::kvraftpb::*;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
    pub last_leader: Arc<AtomicUsize>,
    index: Arc<AtomicU64>,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk {
            name,
            servers,
            last_leader: Arc::new(AtomicUsize::new(0)),
            index: Arc::new(AtomicU64::new(1)),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { do something  }
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        // crate::your_code_here(key)
        let index = self.index.fetch_add(1, Ordering::SeqCst);
        let args = GetRequest {
            key,
            name: self.name.to_owned(),
            index,
        };
        let last_leader = self.last_leader.clone();
        let mut leader = last_leader.load(Ordering::SeqCst);
        let server_num = self.servers.len();

        let threaded_rt = Builder::new_multi_thread().enable_all().build().unwrap();
        loop {
            info!(
                "[Client {} -> Server {}] send [GetRequest key: \"{}\" index: {}]",
                self.name, leader, args.key, args.index,
            );
            if let Ok(reply) = threaded_rt.block_on(self.servers[leader].get(&args)) {
                info!(
                    "[Client {} -> Server {}] recv [GetReply Ok: {} err: \"{}\" value: \"{}\"]",
                    self.name, leader, reply.wrong_leader, reply.err, reply.value
                );
                if !reply.wrong_leader {
                    last_leader.store(leader, Ordering::SeqCst);
                    return reply.value;
                }
            }
            leader = (leader + 1) % server_num;
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        // crate::your_code_here(op)
        let index = self.index.fetch_add(1, Ordering::SeqCst);
        let args = match op {
            Op::Put(key, value) => PutAppendRequest {
                op: 1,
                key,
                value,
                name: self.name.to_owned(),
                index,
            },
            Op::Append(key, value) => PutAppendRequest {
                op: 2,
                key,
                value,
                name: self.name.to_owned(),
                index,
            },
        };
        let last_leader = self.last_leader.clone();
        let mut leader = last_leader.load(Ordering::SeqCst);
        let server_num = self.servers.len();

        let threaded_rt = Builder::new_multi_thread().enable_all().build().unwrap();
        loop {
            info!(
                "[Client {}] send [Server {}] [PutAppendRequest Op: {} Key: \"{}\" Value: \"{}\" Index: {}]",
                self.name, leader, args.op, args.key, args.value,  args.index
            );
            if let Ok(reply) = threaded_rt.block_on(self.servers[leader].put_append(&args)) {
                info!(
                    "[Client {}] recv [Server {}] [PutAppendReply Wrong: {} Err: \"{}\"]",
                    self.name, leader, reply.wrong_leader, reply.err
                );
                if !reply.wrong_leader {
                    last_leader.store(leader, Ordering::SeqCst);
                    return;
                }
            }
            thread::sleep(Duration::from_millis(500));
            leader = (leader + 1) % server_num;
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
