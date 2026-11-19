//! Private on-disk records for prepared faucet payments.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write as _},
    os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
};

use serde_json::Value;

pub struct PaymentStore {
    directory: PathBuf,
    // The OS releases this lock if the process exits unexpectedly.
    _lock: File,
}

impl PaymentStore {
    pub fn open(directory: &Path) -> io::Result<Self> {
        if !directory.exists() {
            fs::DirBuilder::new().mode(0o700).create(directory)?;
        }
        let metadata = fs::symlink_metadata(directory)?;
        if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(io::Error::other("faucet state directory must be private"));
        }
        let parent = directory
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join("lock"))?;
        lock.try_lock().map_err(io::Error::other)?;
        Ok(Self {
            directory: directory.to_owned(),
            _lock: lock,
        })
    }

    pub fn load_pending(&self) -> io::Result<Option<Value>> {
        match fs::read(self.directory.join("pending.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(io::Error::other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn completed(&self, id: &str) -> io::Result<Option<Value>> {
        Self::validate_id(id)?;
        match fs::read(self.directory.join(format!("completed-{id}.json"))) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(io::Error::other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn save_pending(&self, payment: &Value) -> io::Result<()> {
        let path = self.directory.join("pending.json");
        // A partial file after a crash blocks submission and requires operator
        // intervention; it cannot be mistaken for a saved transaction.
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(&serde_json::to_vec(payment).map_err(io::Error::other)?)?;
        file.sync_all()?;
        File::open(&self.directory)?.sync_all()
    }

    pub fn complete(&self, id: &str) -> io::Result<()> {
        Self::validate_id(id)?;
        let source = self.directory.join("pending.json");
        let destination = self.directory.join(format!("completed-{id}.json"));
        // link fails if this payment already exists, and leaves the opening
        // recoverable after a crash at either side of the unlink.
        match fs::hard_link(&source, &destination) {
            Ok(()) => (),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if fs::read(&source)? != fs::read(&destination)? {
                    return Err(io::Error::other(
                        "payment archive conflicts with pending payment",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
        File::open(&self.directory)?.sync_all()?;
        fs::remove_file(source)?;
        File::open(&self.directory)?.sync_all()
    }

    fn validate_id(id: &str) -> io::Result<()> {
        if id.len() != 64
            || !id
                .bytes()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        {
            return Err(io::Error::other("invalid payment id"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_are_private_recoverable_and_exclusively_locked() {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("state");
        let store = PaymentStore::open(&directory).unwrap();
        let payment = serde_json::json!({"opening": "sensitive", "transaction": "signed"});
        store.save_pending(&payment).unwrap();
        assert_eq!(
            fs::metadata(directory.join("pending.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(PaymentStore::open(&directory).is_err());
        drop(store);
        let store = PaymentStore::open(&directory).unwrap();
        assert_eq!(store.load_pending().unwrap(), Some(payment.clone()));
        let id = "ab".repeat(32);
        store.complete(&id).unwrap();
        assert_eq!(store.load_pending().unwrap(), None);
        assert_eq!(
            serde_json::from_slice::<Value>(
                &fs::read(directory.join(format!("completed-{id}.json"))).unwrap()
            )
            .unwrap(),
            payment
        );
    }
}
