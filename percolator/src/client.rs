use std::time::Duration;

use crate::msg::*;
use futures::executor::block_on;
use labrpc::*;

use crate::service::{TSOClient, TransactionClient};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    writes: Vec<Write>,
    start_ts: u64,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            writes: Vec::new(),
            start_ts: 0,
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        for i in 0..RETRY_TIMES + 1 {
            let reply = block_on(self.tso_client.get_timestamp(&TimestampRequest {}));
            if let Ok(reply) = reply {
                return Ok(reply.ts);
            }
            if i < RETRY_TIMES {
                std::thread::sleep(Duration::from_millis(BACKOFF_TIME_MS));
            }
        }
        Err(labrpc::Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        // unimplemented!()
        self.start_ts = self.get_timestamp().unwrap();
        self.writes = Vec::new();
        info!("Begin at TS: {}", self.start_ts);
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        // unimplemented!()
        let args = GetRequest {
            row: key,
            col: Vec::new(),
            ts: self.start_ts,
        };
        info!("Get Request Key: {:?} at TS: {}", args.row, self.start_ts);
        for i in 0..RETRY_TIMES + 1 {
            let reply = block_on(self.txn_client.get(&args));
            if let Ok(reply) = reply {
                info!(
                    "Get Reply Value: {:?} at TS: {}",
                    reply.value, self.start_ts
                );
                return Ok(reply.value);
            }
            if i < RETRY_TIMES {
                info!(
                    "Get Retry {} Key: {:?} at TS: {}",
                    i, args.row, self.start_ts
                );
                std::thread::sleep(Duration::from_millis(BACKOFF_TIME_MS * (1 << i)));
            }
        }
        Err(labrpc::Error::Timeout)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        // unimplemented!()
        self.writes.push(Write {
            row: key,
            col: Vec::new(),
            value,
        });
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        // unimplemented!()
        let primary = self.writes.first().unwrap().to_owned();
        for w in self.writes.iter() {
            let args = PrewriteRequest {
                w: Some(w.to_owned()),
                primary: Some(primary.to_owned()),
                ts: self.start_ts,
            };

            info!("PreWrite Request at TS: {}", self.start_ts);
            for i in 0..RETRY_TIMES + 1 {
                let reply = block_on(self.txn_client.prewrite(&args));
                if let Ok(reply) = reply {
                    if reply.ok {
                        info!("PreWrite Reply at TS: {}", self.start_ts);
                        break;
                    } else {
                        return Ok(false);
                    }
                }
                if i < RETRY_TIMES {
                    info!("PreWrite Retry {} at TS: {}", i, self.start_ts);
                    std::thread::sleep(Duration::from_millis(BACKOFF_TIME_MS * (1 << i)));
                }
            }
        }
        let commit_ts = self.get_timestamp().unwrap();
        let args = CommitRequest {
            is_primary: true,
            w: Some(primary),
            start_ts: self.start_ts,
            commit_ts,
        };
        info!(
            "Commit Request Primary at TS: {}, TS: {}",
            self.start_ts, commit_ts
        );
        for i in 0..RETRY_TIMES + 1 {
            let reply = block_on(self.txn_client.commit(&args));
            match reply {
                Err(Error::Other(v)) => {
                    if v == "resphook" {
                        return Err(Error::Other("resphook".to_owned()));
                    }
                }
                Ok(reply) => {
                    if reply.ok {
                        info!(
                            "Commit Reply Primary at TS: {}, TS: {}",
                            self.start_ts, commit_ts
                        );
                        break;
                    } else {
                        return Ok(false);
                    }
                }
                _ => {}
            }
            if i < RETRY_TIMES {
                info!(
                    "Commit Retry Primary at TS: {}, TS: {}",
                    self.start_ts, commit_ts
                );
                std::thread::sleep(Duration::from_millis(BACKOFF_TIME_MS * (1 << i)));
            } else {
                return Ok(false);
            }
        }
        for second in self.writes.iter().skip(1) {
            let args = CommitRequest {
                is_primary: false,
                w: Some(second.to_owned()),
                start_ts: self.start_ts,
                commit_ts,
            };
            info!("Commit Request at TS: {}, TS: {}", self.start_ts, commit_ts);
            for i in 0..RETRY_TIMES + 1 {
                let reply = block_on(self.txn_client.commit(&args));
                if let Ok(reply) = reply {
                    if reply.ok {
                        info!("Commit Reply at TS: {}, TS: {}", self.start_ts, commit_ts);
                        break;
                    }
                }
                if i < RETRY_TIMES {
                    info!("Commit Retry at TS: {}, TS: {}", self.start_ts, commit_ts);
                    std::thread::sleep(Duration::from_millis(BACKOFF_TIME_MS * (1 << i)));
                }
            }
        }
        Ok(true)
    }
}
