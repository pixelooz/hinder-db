use std::{
    sync::{
        Arc,
        mpsc::{self},
    },
    thread::{self},
    time::Duration,
};

use crate::storage::buffer_pool::BufferPool;

// * TODO: make the flush duration configurable.

/// The interval at which background thread wakes up to flush dirty pages.
const FLUSH_INTERVAL: Duration = Duration::from_millis(250);

/// The physical size limit of Wal before a checkpoint is triggered.
const WAL_CHECKPOINT_THRESHOLD: u64 = 32 * 1024 * 1024;

#[derive(Debug)]
/// Manages the lifecycle of the background writing thread.
pub struct BackgroundFlusher {
    /// Channel sender used to signal the background thread to shut down.
    shutdown_tx: Option<mpsc::Sender<()>>,

    /// The handle to the spawned thread, allowing us to safely block
    /// during shutdown until all final disk I/O completes.
    handle: Option<thread::JoinHandle<()>>,
}

impl Drop for BackgroundFlusher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl BackgroundFlusher {
    /// Spawns a background thread that flushes dirty pages in the BufferPool
    /// at `FLUSH_INTERVAL`s.
    /// Takes an `Arc<Mutex<BufferPool>>` to share ownership with the
    /// foreground storage engine.
    pub fn start(pool: Arc<BufferPool>) -> Self {
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            loop {
                // Blocks the current thread for 100ms, or until a shutdown signal
                // is received.
                // If timeout is reached, we flush all the dirty pages to disk,
                // and log if an error does occur.
                match shutdown_rx.recv_timeout(FLUSH_INTERVAL) {
                    Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                        // Shutdown signal received, exit the loop.
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(err) = pool.clean_pages_watermark() {
                            eprintln!("background flusher watermark failed: {:?}", err)
                        }
                        let wal_manager = pool.get_wal_manager();
                        // Check if size exhausts our limit of 32MB
                        let should_checkpoint = {
                            let wal_guard = wal_manager.lock();
                            wal_guard.size().unwrap_or(0) > WAL_CHECKPOINT_THRESHOLD
                        };
                        // Never truncate the wal if there are active transactions.
                        if should_checkpoint && pool.can_checkpoint() {
                            match pool.flush_all_pages() {
                                Ok(_) => {
                                    let mut wal_guard = wal_manager.lock();
                                    if let Err(err) = wal_guard.truncate() {
                                        eprintln!("wal truncation failed: {:?}", err)
                                    }
                                }
                                Err(err) => {
                                    eprintln!("checkpoint flush failed: {:?}", err);
                                }
                            }
                        }
                    }
                }
            }
            // One final checkpoint before shutting down to prevent wal bloat
            // between reboots.
            if pool.can_checkpoint() {
                let _ = pool.flush_all_pages();
                let _ = pool.get_wal_manager().lock().truncate();
            }
        });
        Self {
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
        }
    }

    /// Signals the background thread to stop and waits for it to finish its
    /// final flush.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            // Signal the thread to wake up immediately and exit.
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            // Block until the final flush is complete.
            let _ = handle.join();
        }
    }
}
