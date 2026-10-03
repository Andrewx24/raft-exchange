//! Durable Raft state for a real node.
//!
//! - `hardstate.json` — term and vote, replaced atomically
//!   (write temp → fsync → rename → fsync directory), so a crash leaves
//!   either the old or the new state, never half of one.
//! - `log.jsonl` — append-only records `{index, entry}`. Raft sometimes
//!   overwrites the log tail; instead of rewriting the file, a record for
//!   index `i` *means* "truncate to `i - 1`, then append". Replay applies
//!   that rule, so the file only ever grows.
//!
//! A torn final line is a write the crash interrupted; it was never synced,
//! so never acknowledged, and is dropped on load. A bad line anywhere else
//! is corruption and fails the load rather than silently losing entries.

use crate::raft::{Entry, HardState, Index};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
struct Record<C> {
    index: Index,
    entry: Entry<C>,
}

pub struct Storage<C> {
    dir: PathBuf,
    log: BufWriter<File>,
    dirty: bool,
    _c: PhantomData<C>,
}

impl<C: Serialize + DeserializeOwned + Clone> Storage<C> {
    pub fn open(dir: &Path) -> io::Result<(Self, HardState, Vec<Entry<C>>)> {
        fs::create_dir_all(dir)?;
        let hs = match fs::read(dir.join("hardstate.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => HardState::default(),
            Err(e) => return Err(e),
        };
        let log_path = dir.join("log.jsonl");
        let mut log: Vec<Entry<C>> = Vec::new();
        if let Ok(f) = File::open(&log_path) {
            let lines: Vec<String> = BufReader::new(f).lines().collect::<Result<_, _>>()?;
            for (i, line) in lines.iter().enumerate() {
                match serde_json::from_str::<Record<C>>(line) {
                    Ok(r) if r.index >= 1 && r.index as usize <= log.len() + 1 => {
                        log.truncate(r.index as usize - 1);
                        log.push(r.entry);
                    }
                    Ok(r) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("log record for index {} leaves a gap", r.index),
                        ));
                    }
                    Err(_) if i + 1 == lines.len() => break,
                    Err(e) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("log line {}: {e}", i + 1),
                        ));
                    }
                }
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        let s = Storage {
            dir: dir.to_path_buf(),
            log: BufWriter::with_capacity(1 << 16, file),
            dirty: false,
            _c: PhantomData,
        };
        Ok((s, hs, log))
    }

    pub fn save_hard_state(&mut self, hs: HardState) -> io::Result<()> {
        let tmp = self.dir.join("hardstate.json.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&serde_json::to_vec(&hs)?)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, self.dir.join("hardstate.json"))?;
        File::open(&self.dir)?.sync_all()
    }

    /// Buffers entries starting at `from`. Call [`Storage::sync`] before
    /// sending any message that depends on them.
    pub fn append(&mut self, from: Index, entries: &[Entry<C>]) -> io::Result<()> {
        for (k, entry) in entries.iter().enumerate() {
            let rec = Record {
                index: from + k as Index,
                entry: entry.clone(),
            };
            serde_json::to_writer(&mut self.log, &rec)?;
            self.log.write_all(b"\n")?;
        }
        self.dirty |= !entries.is_empty();
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        if std::mem::take(&mut self.dirty) {
            self.log.flush()?;
            self.log.get_ref().sync_data()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::Payload;

    fn e(term: u64, c: u32) -> Entry<u32> {
        Entry {
            term,
            payload: Payload::Command(c),
        }
    }

    #[test]
    fn overwrites_replay_as_truncation_and_torn_tails_are_dropped() {
        let dir = std::env::temp_dir().join(format!("rx-storage-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        {
            let (mut s, hs, log) = Storage::<u32>::open(&dir).unwrap();
            assert_eq!((hs, log.len()), (HardState::default(), 0));
            s.append(1, &[e(1, 10), e(1, 11), e(1, 12)]).unwrap();
            // A new leader overwrites index 2 onward.
            s.append(2, &[e(2, 20)]).unwrap();
            s.sync().unwrap();
            s.save_hard_state(HardState {
                term: 2,
                voted_for: Some(3),
            })
            .unwrap();
        }
        OpenOptions::new()
            .append(true)
            .open(dir.join("log.jsonl"))
            .unwrap()
            .write_all(b"{\"index\":3,\"en")
            .unwrap();
        let (_, hs, log) = Storage::<u32>::open(&dir).unwrap();
        assert_eq!(
            hs,
            HardState {
                term: 2,
                voted_for: Some(3)
            }
        );
        assert_eq!(log, vec![e(1, 10), e(2, 20)]);
        let _ = fs::remove_dir_all(&dir);
    }
}
