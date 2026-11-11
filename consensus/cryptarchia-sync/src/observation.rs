//! Typed, scenario-local observations for era-boundary experiments.

use std::{
    fs::{File, OpenOptions},
    io::{self, Read as _, Seek as _, SeekFrom, Write as _},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use lb_core::header::HeaderId;
use serde::{Deserialize, Serialize};

/// Schema version for the on-disk journal.
pub const SCHEMA_VERSION: u32 = 1;
pub const JOURNAL_PATH_ENV: &str = "LOGOS_BLOCKCHAIN_ERA_OBSERVATION_JOURNAL";

static NODE_JOURNAL: OnceLock<Option<ObservationJournal>> = OnceLock::new();

/// Called before node services start. An unset path leaves observations off.
pub fn initialize_node_journal() -> io::Result<()> {
    let journal = std::env::var_os(JOURNAL_PATH_ENV)
        .map(|path| ObservationJournal::create(Path::new(&path)))
        .transpose()?;
    NODE_JOURNAL
        .set(journal)
        .map_err(|_| io::Error::other("node observation journal initialized more than once"))
}

#[must_use]
pub fn node_journal_is_active() -> bool {
    NODE_JOURNAL.get().is_some_and(Option::is_some)
}

/// Failed writes terminate an instrumented node so evidence loss cannot pass.
pub fn emit(event: Observation) {
    if let Some(Some(journal)) = NODE_JOURNAL.get()
        && let Err(error) = journal.record(event)
    {
        eprintln!("era observation journal write failed: {error}");
        std::process::exit(1);
    }
}

/// A peer identity in the canonical libp2p byte representation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PeerIdentity(pub Vec<u8>);

impl From<libp2p::PeerId> for PeerIdentity {
    fn from(peer: libp2p::PeerId) -> Self {
        Self(peer.to_bytes())
    }
}

/// Request identity is local to the emitting node. The context is not an ID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestObservation {
    pub local_request_id: u64,
    pub remote_peer: PeerIdentity,
    pub protocol: String,
    pub target: HeaderId,
    pub requester_tip: HeaderId,
    pub latest_immutable: HeaderId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionKind {
    StreamError,
    ResponseTimeout,
}

/// The payload is constructed at the observation site, never from tracing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Observation {
    RequesterRequestStarted {
        request: RequestObservation,
    },
    ProviderRequestAccepted {
        request: RequestObservation,
    },
    ProviderBlockResponseSent {
        request: RequestObservation,
        response_index: u64,
    },
    RequesterBlockResponseReceived {
        request: RequestObservation,
        response_index: u64,
    },
    RequesterFirstBlockDecoded {
        remote_peer: PeerIdentity,
        target: HeaderId,
        requester_tip: HeaderId,
        latest_immutable: HeaderId,
        block_id: HeaderId,
    },
    IbdRequestStarted {
        target: HeaderId,
        requester_tip: HeaderId,
        latest_immutable: HeaderId,
    },
    IbdDownloaderDownloading {
        target: HeaderId,
    },
    IbdBlockApplied {
        block_id: HeaderId,
    },
    ProviderResponseHoldActivated {
        request: RequestObservation,
        blocks_sent: u64,
    },
    EraRotationStarted {
        previous_protocol: String,
        new_protocol: String,
        pending_provider_responses: u64,
    },
    EraTransitionExpired {
        old_protocol: String,
        pending_provider_responses: u64,
    },
    ProviderResponseHoldCancelled {
        request: RequestObservation,
        blocks_sent: u64,
    },
    RequesterInterruptionDetected {
        request: RequestObservation,
        response_count: u64,
        kind: InterruptionKind,
    },
    IbdDownloadStreamError {
        target: HeaderId,
        blocks_received: u64,
    },
    ProviderRequestCompleted {
        request: RequestObservation,
        blocks_sent: u64,
    },
    RequesterRequestCompleted {
        request: RequestObservation,
        response_count: u64,
    },
    IbdCompleted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationRecord {
    pub schema_version: u32,
    pub sequence: u64,
    /// Diagnostic only; never used for cross-node ordering.
    pub timestamp_unix_ms: u64,
    /// Monotonic elapsed time within this node, for measuring local durations.
    pub elapsed_ns: u64,
    pub event: Observation,
}

struct JournalState {
    file: File,
    next_sequence: u64,
    started: Instant,
}

/// A lock covers both sequence allocation and the complete record write.
pub struct ObservationJournal {
    state: Mutex<JournalState>,
}

impl ObservationJournal {
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Self {
            state: Mutex::new(JournalState {
                file,
                next_sequence: 1,
                started: Instant::now(),
            }),
        })
    }

    pub fn record(&self, event: Observation) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("observation journal lock poisoned"))?;
        let timestamp_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let record = ObservationRecord {
            schema_version: SCHEMA_VERSION,
            sequence: state.next_sequence,
            timestamp_unix_ms,
            elapsed_ns: u64::try_from(state.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            event,
        };
        let mut bytes = serde_json::to_vec(&record).map_err(io::Error::other)?;
        bytes.push(b'\n');
        state.file.write_all(&bytes)?;
        state.next_sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("observation sequence exhausted"))?;
        drop(state);
        Ok(())
    }
}

/// Incrementally reads one node's journal, retaining its last complete record.
#[derive(Debug)]
pub struct ObservationReader {
    path: PathBuf,
    offset: u64,
    next_sequence: u64,
    observed_len: u64,
    anchor: Vec<u8>,
    #[cfg(unix)]
    file_identity: Option<(u64, u64)>,
}

impl ObservationReader {
    #[must_use]
    pub const fn new(path: PathBuf) -> Self {
        Self {
            path,
            offset: 0,
            next_sequence: 1,
            observed_len: 0,
            anchor: Vec::new(),
            #[cfg(unix)]
            file_identity: None,
        }
    }

    #[expect(
        clippy::verbose_file_reads,
        reason = "reading starts at the saved cursor rather than at the beginning of the file"
    )]
    pub fn read_available(&mut self) -> io::Result<Vec<ObservationRecord>> {
        let mut file = File::open(&self.path)?;
        let metadata = file.metadata()?;
        let length = metadata.len();
        #[cfg(unix)]
        #[expect(
            clippy::semicolon_if_nothing_returned,
            reason = "the semicolon belongs outside this feature-gated statement block"
        )]
        {
            use std::os::unix::fs::MetadataExt as _;
            let identity = (metadata.dev(), metadata.ino());
            if self
                .file_identity
                .is_some_and(|previous| previous != identity)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "observation journal was replaced",
                ));
            }
            self.file_identity = Some(identity)
        };
        if length < self.observed_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "observation journal was truncated",
            ));
        }
        if !self.anchor.is_empty() {
            let start =
                self.offset - u64::try_from(self.anchor.len()).expect("anchor length fits u64");
            file.seek(SeekFrom::Start(start))?;
            let mut anchor = vec![0; self.anchor.len()];
            file.read_exact(&mut anchor)?;
            if anchor != self.anchor {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "observation journal contents before cursor changed",
                ));
            }
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let consumed = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        let mut next_sequence = self.next_sequence;
        let mut records = Vec::new();
        for line in bytes[..consumed].split_inclusive(|byte| *byte == b'\n') {
            let line = &line[..line.len() - 1];
            let record: ObservationRecord = serde_json::from_slice(line)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if record.schema_version != SCHEMA_VERSION || record.sequence != next_sequence {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid observation schema or sequence: expected {next_sequence}, got schema {} sequence {}",
                        record.schema_version, record.sequence
                    ),
                ));
            }
            next_sequence = next_sequence.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "observation sequence exhausted")
            })?;
            records.push(record);
        }
        self.observed_len = self
            .offset
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            .max(length);
        self.offset = self
            .offset
            .saturating_add(u64::try_from(consumed).unwrap_or(u64::MAX));
        if consumed != 0 {
            let mut tail = std::mem::take(&mut self.anchor);
            tail.extend_from_slice(&bytes[..consumed]);
            let keep_from = tail.len().saturating_sub(64);
            self.anchor = tail[keep_from..].to_vec();
        }
        self.next_sequence = next_sequence;
        Ok(records)
    }

    /// A partial last line is acceptable during a run but not at completion.
    pub fn finish(&mut self) -> io::Result<Vec<ObservationRecord>> {
        let records = self.read_available()?;
        if self.offset != self.observed_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete final observation record",
            ));
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use super::*;

    #[test]
    fn concurrent_records_have_contiguous_sequences_in_file_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("observations.ndjson");
        let journal = Arc::new(ObservationJournal::create(&path).unwrap());
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let journal = Arc::clone(&journal);
                scope.spawn(move || {
                    for _ in 0..25 {
                        journal.record(Observation::IbdCompleted).unwrap();
                    }
                });
            }
        });
        let contents = fs::read_to_string(path).unwrap();
        let records = contents
            .lines()
            .map(|line| serde_json::from_str::<ObservationRecord>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 200);
        for (index, record) in records.iter().enumerate() {
            assert_eq!(record.sequence, u64::try_from(index + 1).unwrap());
            assert_eq!(record.schema_version, SCHEMA_VERSION);
            assert_eq!(record.event, Observation::IbdCompleted);
        }
    }

    #[test]
    fn incremental_reader_handles_partial_records_and_rejects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("observations.ndjson");
        let journal = ObservationJournal::create(&path).unwrap();
        journal.record(Observation::IbdCompleted).unwrap();
        let mut reader = ObservationReader::new(path.clone());
        assert_eq!(reader.read_available().unwrap().len(), 1);
        assert_eq!(reader.read_available().unwrap(), []);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"schema_version\":").unwrap();
        assert_eq!(reader.read_available().unwrap(), []);
        assert_eq!(
            reader.finish().unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        file.write_all(b"broken}\n").unwrap();
        assert_eq!(
            reader.read_available().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn incremental_reader_rejects_truncation_and_sequence_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("observations.ndjson");
        let journal = ObservationJournal::create(&path).unwrap();
        journal.record(Observation::IbdCompleted).unwrap();
        let mut reader = ObservationReader::new(path.clone());
        assert_eq!(reader.read_available().unwrap().len(), 1);
        fs::write(&path, b"").unwrap();
        assert_eq!(
            reader.read_available().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        let gap = ObservationRecord {
            schema_version: SCHEMA_VERSION,
            sequence: 2,
            timestamp_unix_ms: 0,
            elapsed_ns: 0,
            event: Observation::IbdCompleted,
        };
        fs::write(&path, format!("{}\n", serde_json::to_string(&gap).unwrap())).unwrap();
        let mut reader = ObservationReader::new(path);
        assert_eq!(
            reader.read_available().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn reader_rejects_unknown_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("observations.ndjson");
        let record = ObservationRecord {
            schema_version: SCHEMA_VERSION + 1,
            sequence: 1,
            timestamp_unix_ms: 0,
            elapsed_ns: 0,
            event: Observation::IbdCompleted,
        };
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&record).unwrap()),
        )
        .unwrap();
        let mut reader = ObservationReader::new(path);
        assert_eq!(
            reader.read_available().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn reader_rejects_rewritten_prefix_after_truncation_and_regrowth() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("observations.ndjson");
        let journal = ObservationJournal::create(&path).unwrap();
        journal.record(Observation::IbdCompleted).unwrap();
        let mut reader = ObservationReader::new(path.clone());
        assert_eq!(reader.read_available().unwrap().len(), 1);
        let old_len = fs::metadata(&path).unwrap().len();
        fs::write(&path, vec![b'x'; usize::try_from(old_len + 1).unwrap()]).unwrap();
        assert_eq!(
            reader.read_available().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
