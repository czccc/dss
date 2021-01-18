use std::{
    collections::BTreeMap, ops::Bound::*, sync::atomic::AtomicU64, sync::atomic::Ordering, thread,
    time::Instant,
};
use std::{collections::HashMap, time::Duration};
use std::{
    ops::{Bound, RangeBounds},
    sync::{Arc, Mutex},
};

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    stamp: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        // unimplemented!()
        let ts = self.stamp.fetch_add(1, Ordering::SeqCst);
        Ok(TimestampResponse { ts })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        // unimplemented!()
        let key_start_inclusive = ts_start_inclusive
            .map_or(Included((key.to_owned(), u64::MIN)), |v| {
                Included((key.to_owned(), v))
            });
        let key_end_inclusive = ts_end_inclusive
            .map_or(Included((key.to_owned(), u64::MAX)), |v| {
                Included((key.to_owned(), v))
            });
        let db = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };
        db.range((key_start_inclusive, key_end_inclusive)).last()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        // unimplemented!()
        let key: Key = (key, ts);
        let db = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        db.insert(key, value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        // unimplemented!()
        let key_start_inclusive = Included((key.to_owned(), u64::MIN));
        let key_end_inclusive = Included((key.to_owned(), commit_ts));
        let mut key_start: Key = (key, 0);
        let db = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        let keys: Vec<Key> = db
            .range((key_start_inclusive, key_end_inclusive))
            .map(|v| v.0.to_owned())
            .collect();
        for k in keys {
            db.remove(&k);
        }
    }
}

fn generate_key(row: Vec<u8>, mut col: Vec<u8>) -> Vec<u8> {
    let mut key = row;
    key.append(col.as_mut());
    key
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
    pending_lock: Arc<Mutex<HashMap<Vec<u8>, Instant>>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        // unimplemented!()
        use Column::*;
        let key = generate_key(req.row, req.col);
        loop {
            let lock = self
                .data
                .lock()
                .unwrap()
                .read(key.to_owned(), Lock, None, Some(req.ts))
                .map(|(_, v)| v.to_owned());
            if lock.is_some() {
                info!("Get previous Lock in Key: {:?}, Lock: {:?}", key, lock);
                self.back_off_maybe_clean_up_lock(req.ts, key.to_owned());
                continue;
            }

            let mut data = self.data.lock().unwrap();
            let last_write = data.read(key.to_owned(), Write, None, Some(req.ts));
            if last_write.is_none() {
                return Ok(GetResponse {
                    ok: false,
                    value: Vec::new(),
                });
            }
            if let Value::Timestamp(data_ts) = last_write.unwrap().1.to_owned() {
                let value = data.read(key, Data, Some(data_ts), Some(data_ts));
                if let Value::Vector(value) = value.unwrap().1.to_owned() {
                    return Ok(GetResponse { ok: true, value });
                } else {
                    return Err(labrpc::Error::Other("Data part is not Vector".to_owned()));
                }
            } else {
                return Err(labrpc::Error::Other(
                    "Write part is not Timestamp".to_owned(),
                ));
            }
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        // unimplemented!()
        use Column::*;
        let w = req.w.unwrap();
        let primary = req.primary.unwrap();
        let mut data = self.data.lock().unwrap();
        let key = generate_key(w.row, w.col);
        let primary_key = generate_key(primary.row, primary.col);
        if data
            .read(key.to_owned(), Write, Some(req.ts), None)
            .is_some()
            || data.read(key.to_owned(), Lock, None, None).is_some()
        {
            return Ok(PrewriteResponse { ok: false });
        }
        data.write(key.to_owned(), Data, req.ts, Value::Vector(w.value));
        data.write(key, Lock, req.ts, Value::Vector(primary_key));
        Ok(PrewriteResponse { ok: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        // unimplemented!()
        use Column::*;
        let w = req.w.unwrap();
        let key = generate_key(w.row, w.col);
        let mut data = self.data.lock().unwrap();
        if req.is_primary
            && data
                .read(key.to_owned(), Lock, Some(req.start_ts), Some(req.start_ts))
                .is_none()
        {
            return Ok(CommitResponse { ok: false });
        }

        let primary = data.read(key.to_owned(), Lock, Some(req.start_ts), Some(req.start_ts));
        if let Some((_, Value::Vector(primary))) = primary {
            let mut pending_lock = self.pending_lock.lock().unwrap();
            pending_lock.insert(primary.to_owned(), Instant::now());
        }
        info!("Commit {:?}", key);
        data.write(
            key.to_owned(),
            Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        data.erase(key, Lock, req.commit_ts);
        Ok(CommitResponse { ok: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        // unimplemented!()
        use Column::*;
        let mut data = self.data.lock().unwrap();
        if let Some((primary_ts, Value::Vector(primary))) = data
            .read(key.to_owned(), Lock, None, Some(start_ts))
            .map(|(k, v)| (k.1, v.to_owned()))
        {
            let mut pending_lock = self.pending_lock.lock().unwrap();
            if pending_lock.contains_key(&primary) {
                let last_instant = pending_lock.get(&primary).unwrap();
                if last_instant.elapsed().as_nanos() as u64 >= TTL {
                    if data
                        .read(primary.to_owned(), Lock, None, Some(start_ts))
                        .map(|(_, v)| v.to_owned())
                        .is_some()
                    {
                        info!("Pending Remove Key: {:?} and Roll back", primary);
                        pending_lock.remove(&primary);
                        data.erase(primary, Lock, start_ts);
                    } else if let Some((primary_commit_ts, value)) = data
                        .read(primary.to_owned(), Write, Some(primary_ts), None)
                        .map(|(k, v)| (k.to_owned().1, v.to_owned()))
                    {
                        info!("Pending Remove Key: {:?} and Roll forward", primary);
                        data.write(key.to_owned(), Write, primary_commit_ts, value);
                        data.erase(key, Lock, start_ts);
                        pending_lock.remove(&primary);
                    } else {
                        info!("Pending Remove Key: {:?} and Roll back", key);
                        data.erase(key, Lock, start_ts);
                    }
                }
            } else {
                info!("7");
                info!("Pending Add Key: {:?}", primary);
                pending_lock.insert(primary, Instant::now());
            }
        }
    }
}
