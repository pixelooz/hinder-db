use std::{collections::HashSet, path::Path};

use crate::{
    error::Error,
    storage::{
        page::DiskManager,
        wal::{WalManager, WalRecord},
    },
};

/// Handles database startup recovery by replaying historical Wals against in-memory pages
/// in the [BufferPool].
pub struct RecoveryEngine;

impl RecoveryEngine {
    /// Bootstraps database recovery on startup. Walks the wal file in reverse chronological
    /// order restoring the pages, applying either Undo or Redo operation depending on whether
    /// the transaction was committed or not.
    pub fn init<P>(path: P, disk_manager: &DiskManager) -> Result<(), Error>
    where
        P: AsRef<Path>,
    {
        let mut wal = WalManager::open(path, false)?;
        let record_batch = wal.read_batch()?;

        // If the wal is completely empty, either the database shut down cleanly
        // or is brand new.
        if record_batch.is_empty() {
            return Ok(());
        }
        let mut committed_txns = HashSet::new();

        for record in &record_batch {
            if let WalRecord::Commit { txn_id } = record {
                committed_txns.insert(*txn_id);
            }
        }
        // We read the pages in reverse because the first time we see a page it is
        // guaranteed to be its final correct state.
        let mut restored_pages = HashSet::new();
        for record in record_batch.into_iter().rev() {
            match record {
                WalRecord::Undo {
                    page_id,
                    txn_id,
                    page,
                } => {
                    // Apply Undo only if the txn is not committed and we haven't
                    // already seen this page before.
                    if !committed_txns.contains(&txn_id) && !restored_pages.contains(&page_id) {
                        disk_manager.write_page(page_id, &page)?;
                        restored_pages.insert(page_id);
                    }
                }
                WalRecord::Redo {
                    page_id,
                    txn_id,
                    page,
                } => {
                    // Apply Redo only if the txn is committed and we haven't already
                    // seen this page before.
                    if committed_txns.contains(&txn_id) && !restored_pages.contains(&page_id) {
                        disk_manager.write_page(page_id, &page)?;
                        restored_pages.insert(page_id);
                    }
                }
                WalRecord::Commit { .. } => {}
            }
        }
        disk_manager.save_header()?;
        wal.truncate()?;
        Ok(())
    }
}

/*  TODO: Too many changes, will write tests if I see something break when running the query
directly now, since we have the entire flow accessible now. */
