# Distributed Systems in Rust

This project is followed with [Talent Plan](https://github.com/pingcap/talent-plan) from [PingCAP University](https://university.pingcap.com/).

## Project Workspaces

- `labcodec`: encode and decode message in the RPC network
- `labrpc`: simulate a distributed network with multi-node and RPC service between nodes
- `linearizability`: some test for linearizability (maybe not used)
- `percolator`: 
    - a system built by Google for incremental processing on a very large data set.
    - It also provides a distributed transaction protocol with ACID snapshot-isolation semantics
    - origin paper: [Large-scale Incremental Processing Using Distributed Transactions and Notifications](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36726.pdf)
    - see more in [percolator/README.md](./percolator/README.md)
- `raft`:
    - a series of labs on a key/value storage system built with the Raft consensus algorithm. 
    - These labs are derived from the [lab2:raft](http://nil.csail.mit.edu/6.824/2018/labs/lab-raft.html) and [lab3:kvraft](http://nil.csail.mit.edu/6.824/2018/labs/lab-kvraft.html) from the famous [MIT 6.824](http://nil.csail.mit.edu/6.824/2018/index.html) course but rewritten in Rust.
    - origin paper: [In Search of an Understandable Consensus Algorithm (Extended Version)](https://raft.github.io/raft.pdf)
    - see more in [raft/README.md](./raft/README.md)
- `report`: some log output file

## Feature

- similar with MIT 6.824 but rewrite all test code in Rust by [Talent Plan](https://github.com/pingcap/talent-plan)
- writen with pure Rust and ProtoBuf protocol
- use [`prost`](https://docs.rs/prost/0.7.0/prost/) to parse ProtoBuf message
- use [`log`](https://docs.rs/log/0.4.13/log/) and [`env_logger`](https://docs.rs/env_logger/0.8.2/env_logger/) as log format
- largely use [`futures`](https://docs.rs/futures/0.3.12/futures/) and [`tokio`](https://docs.rs/tokio/1.0.2/tokio/) to achieve asynchrony

## Progress:

- Raft (lab 2)
    - [X] 2A: leader election and heartbeat
    - [X] 2B: log entry and append entries
    - [X] 2C: persist and unliable network
    - [X] All tests passed
- KvRaft (lab 3)
    - [X] 3A: server/client query
    - [X] 3B: log compaction and install snapshot
    - [X] All tests passed
- Percolator
    - [X] Server: TimestampOracle
    - [X] Server: KvTable and MemoryStorage
    - [X] Server: Get, Prewrite, Commit
    - [X] Client: Set, Get, Commit
    - [X] All tests passed

## TODO

- Raft and KvRaft
    - [ ] add more comment
    - [ ] prune some condition to make program clear
- Percolator
    - [ ] improve 2PC logic
    - [ ] improve lock conflict and back-off/roll-forward