pub mod raftpb {
    include!(concat!(env!("OUT_DIR"), "/raftpb.rs"));

    labrpc::service! {
        service raft {
            rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);

            // Your code here if more rpc desired.
            // rpc xxx(yyy) returns (zzz)

            rpc append_entries(AppendEntriesArgs) returns (AppendEntriesReply);
        }
    }
    pub use self::raft::{
        add_service as add_raft_service, Client as RaftClient, Service as RaftService,
    };
}

pub mod kvraftpb {
    include!(concat!(env!("OUT_DIR"), "/kvraftpb.rs"));

    labrpc::service! {
        service kv {
            // rpc get(GetRequest) returns (GetReply);
            // rpc put_append(PutAppendRequest) returns (PutAppendReply);
            rpc request(KvRequest) returns (KvReply);

            // Your code here if more rpc desired.
            // rpc xxx(yyy) returns (zzz)
        }
    }
    pub use self::kv::{add_service as add_kv_service, Client as KvClient, Service as KvService};
}

use crate::proto::raftpb::*;

impl std::fmt::Display for RequestVoteArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[RequestVoteArgs ID: {} Term: {} Log: {} {}]",
            self.candidate_id, self.term, self.last_log_index, self.last_log_term
        )
    }
}

impl std::fmt::Display for RequestVoteReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[RequestVoteReply Term: {} Vote: {}]",
            self.term, self.vote_granted
        )
    }
}

impl std::fmt::Display for AppendEntriesArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[AppendEntriesArgs ID: {} Term: {} Commit: {} Log: {} {} {}]",
            self.leader_id,
            self.term,
            self.leader_commit,
            self.prev_log_index,
            self.prev_log_term,
            self.entries.len()
        )
    }
}

impl std::fmt::Display for AppendEntriesReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[AppendEntriesReply Term: {} Success: {} Conflict: {} {}]",
            self.term, self.success, self.conflict_log_index, self.conflict_log_term
        )
    }
}

use crate::proto::kvraftpb::*;

impl std::fmt::Display for PutAppendRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = match self.op {
            1 => "Put",
            2 => "Apd",
            3 => "Get",
            _ => "Unk",
        };
        write!(
            f,
            "[Request ({} \"{}\": \"{}\") from ({}-{})]",
            op, self.key, self.value, self.name, self.seq
        )
    }
}
impl std::fmt::Display for PutAppendReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = match self.op {
            1 => "Put",
            2 => "Append",
            3 => "Get",
            _ => "Unknown",
        };
        if self.wrong_leader {
            write!(f, "[Reply ({} Err: \"{}\")]", op, self.err)
        } else {
            write!(f, "[Reply ({} Ok)]", op)
        }
    }
}
impl std::fmt::Display for GetRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[Request (Get \"{}\") from ({}-{})]",
            self.key, self.name, self.seq
        )
    }
}
impl std::fmt::Display for GetReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.wrong_leader {
            write!(f, "[Reply (Get Err: \"{}\")]", self.err)
        } else {
            write!(f, "[Reply (Get Val: \"{}\")]", self.value)
        }
    }
}

fn wrap_too_long_text(text: &str) -> String {
    if text.len() <= 33 {
        text.to_owned()
    } else {
        let mut wrap = String::new();
        wrap.push_str(&text[0..14]);
        wrap.push_str(" ... ");
        wrap.push_str(&text[(text.len() - 14)..text.len()]);
        wrap
    }
}

impl std::fmt::Display for KvRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = match self.op {
            1 => "Put",
            2 => "Append",
            3 => "Get",
            _ => "Unknown",
        };
        write!(
            f,
            "[Request ({} \"{}\":\"{}\") from ({}-{})]",
            op,
            self.key,
            wrap_too_long_text(&self.value),
            self.name,
            self.seq
        )
    }
}
impl std::fmt::Display for KvReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = match self.op {
            1 => "Put",
            2 => "Append",
            3 => "Get",
            _ => "Unknown",
        };
        if self.success {
            write!(
                f,
                "[Reply ({} \"{}\": \"{}\") to ({}-{})]",
                op,
                self.key,
                wrap_too_long_text(&self.value),
                self.name,
                self.seq
            )
        } else {
            write!(
                f,
                "[Reply ({} Err: \"{}\") to ({}-{})]",
                op, self.err, self.name, self.seq
            )
        }
    }
}
