use std::sync::Arc;

use crate::{error::Error, storage::buffer_pool::BufferPool};

/// A transaction is a simple struct representing a database transaction and controls
/// locking and unlocking tables etc.
///
/// If this struct goes out of scope without explicit calls to `commit` or `abort`
/// manually, the `Drop` implementation will rollback the changes made preserving
/// database integrity.
#[derive(Debug)]
pub struct Transaction {
    buffer_pool: Arc<BufferPool>,
    pub txn_id: u64,
    completed: bool,
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.completed
            && let Err(abort_err) = self.abort()
        {
            eprintln!(
                "CRITICAL: Failed to abort transaction {} during drop: {:?}; the data is probably f*ked",
                self.txn_id, abort_err
            );
        }
    }
}

impl Transaction {
    /// Constructor.
    pub fn new(txn_id: u64, pool: Arc<BufferPool>) -> Self {
        Self {
            buffer_pool: pool,
            txn_id,
            completed: false,
        }
    }

    /// Commits the transaction, flushing the REDO logs to disk.
    pub fn commit(&mut self) -> Result<(), Error> {
        if self.completed {
            return Ok(());
        }
        self.buffer_pool.commit_transaction(self.txn_id)?;
        self.completed = true;
        Ok(())
    }

    /// Aborts the transaction, reverting UNDO logs from disk.
    pub fn abort(&mut self) -> Result<(), Error> {
        if self.completed {
            return Ok(());
        }
        self.buffer_pool.abort_transaction(self.txn_id)?;
        self.completed = true;
        Ok(())
    }
}
