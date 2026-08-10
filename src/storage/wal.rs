use std::{
    fs::{File, OpenOptions},
    io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    error::Error,
    storage::page::{PAGE_SIZE, Page, PageId},
};

/// Represents the type of operation we are logging to the wal.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordType {
    Undo = 0,
    Redo = 1,
    Commit = 2,
}

/// A physical record for STEAL + UNDO/REDO crash recovery.
#[derive(Debug)]
pub enum WalRecord {
    /// The before-image of a page, written before the first modification in a transaction.
    Undo {
        page_id: PageId,
        txn_id: u64,
        page: Box<Page>,
    },
    /// Marks a transaction as fully durable.
    Commit { txn_id: u64 },
    /// The after-image of a page, written after the transaction commits.
    Redo {
        page_id: PageId,
        txn_id: u64,
        page: Box<Page>,
    },
}

impl WalRecord {
    /// Serializes the wal record into a raw little-endian byte vector.
    pub fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        match self {
            WalRecord::Undo {
                page_id,
                txn_id,
                page,
            } => {
                buffer.push(WalRecordType::Undo as u8);
                buffer.extend_from_slice(&txn_id.to_le_bytes());
                buffer.extend_from_slice(&page_id.0.to_le_bytes());
                buffer.extend_from_slice(page.as_bytes());
            }
            WalRecord::Commit { txn_id } => {
                buffer.push(WalRecordType::Commit as u8);
                buffer.extend_from_slice(&txn_id.to_le_bytes());
            }
            WalRecord::Redo {
                page_id,
                txn_id,
                page,
            } => {
                buffer.push(WalRecordType::Redo as u8);
                buffer.extend_from_slice(&txn_id.to_le_bytes());
                buffer.extend_from_slice(&page_id.0.to_le_bytes());
                buffer.extend_from_slice(page.as_bytes());
            }
        }
        buffer
    }

    /// Deserialize a raw little-endian byte slice into a `WalRecord`.
    pub fn decode(buffer: &[u8]) -> Result<Self, Error> {
        if buffer.is_empty() {
            return Err(Error::CorruptPage("empty Wal record buffer".into()));
        }
        let record_type = buffer[0];

        let txn_id = u64::from_le_bytes(buffer[1..9].try_into().unwrap());
        match record_type {
            0 | 1 => {
                if buffer.len() < 17 + PAGE_SIZE {
                    return Err(Error::CorruptPage("wal page record too small".into()));
                }
                let page_id = u64::from_le_bytes(buffer[9..17].try_into().unwrap());
                let mut page = Box::new(Page::new());

                page.as_bytes_mut()
                    .copy_from_slice(&buffer[17..17 + PAGE_SIZE]);

                if record_type == 0 {
                    Ok(WalRecord::Undo {
                        page_id: page_id.into(),
                        txn_id,
                        page,
                    })
                } else {
                    Ok(WalRecord::Redo {
                        page_id: page_id.into(),
                        txn_id,
                        page,
                    })
                }
            }
            2 => Ok(WalRecord::Commit { txn_id }),
            _ => Err(Error::CorruptPage(format!(
                "invalid wal record type: {}",
                record_type
            ))),
        }
    }
}

/// Manages the append-only Write-Ahead Log (Wal) file on disk.
#[derive(Debug)]
pub struct WalManager {
    file: File,
    /// Tracks the absolute offset that has been flushed to the disk.
    flushed_offset: AtomicU64,
}

impl WalManager {
    /// Opens an existing wal file in append mode or creates a new one returning
    /// `WalManager` with the fields initialized.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            file,
            flushed_offset: AtomicU64::new(size),
        })
    }

    /// Returns the current physical size of the Wal file in bytes.
    pub fn size(&self) -> Result<u64, Error> {
        let metadata = self.file.metadata()?;
        Ok(metadata.len())
    }

    /// Returns the highest flushed wal offset.
    pub fn flushed_offset(&self) -> u64 {
        self.flushed_offset.load(Ordering::Acquire)
    }

    /// Explicitly fsyncs the wal file to the disk and stores the flushed offset.
    pub fn sync(&mut self) -> Result<(), Error> {
        self.file.sync_all()?;
        let offset = self.file.seek(SeekFrom::End(0))?;
        self.flushed_offset
            .store(offset, Ordering::Release);
        Ok(())
    }

    /// Appends a single record to the Wal and returns its absolute byte offset.
    /// This offset is cached by the BufferPool for Undo retrieval during abort.
    pub fn write_record(&mut self, record: &WalRecord) -> Result<u64, Error> {
        let encoded = record.encode();
        let len = encoded.len() as u32;

        // Capture the exact offset where this record begins.
        let offset = self.file.seek(SeekFrom::End(0))?;

        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&encoded)?;

        Ok(offset)
    }

    /// Seeks to a specific byte offset and decodes a single Wal record.
    /// Used for runtime transaction rollback.
    pub fn read_record_at(&mut self, offset: u64) -> Result<WalRecord, Error> {
        self.file
            .seek(SeekFrom::End(offset.try_into().unwrap()))?;
        let mut len_buf = [0u8; 4];

        self.file.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut buffer = vec![0u8; len];
        self.file.read_exact(&mut buffer)?;

        WalRecord::decode(&buffer)
    }

    /// Appends a batch of log entries to the Wal file using 4-byte little-endian
    /// length indicator for each entry.
    /// Buffers the entire batch in-memory and issues a single POSIX write and if
    /// applicable, a single disk sync.
    pub fn write_batch(&mut self, batch: &[WalRecord]) -> Result<(), Error> {
        if batch.is_empty() {
            return Ok(());
        }
        // Calculate the approximate memory capacity required for this entire batch.
        // header(undo/redo) = len + type + txn_id + page_id + PAGE_SIZE.
        // commits are tiny.
        let estimated_size = batch.len() * (4 + 1 + 8 + 8 + PAGE_SIZE);

        let mut buffer = Vec::with_capacity(estimated_size);
        for entry in batch {
            let encoded_entry = entry.encode();
            let entry_len = encoded_entry.len() as u32;

            // prepend the size of this entry.
            buffer.extend_from_slice(&entry_len.to_le_bytes());
            buffer.extend_from_slice(&encoded_entry);
        }
        self.file.write_all(&buffer)?;
        Ok(())
    }

    /// Truncates the physical Wal file to 0 bytes and rewinding the Os file
    /// cursor as well.
    ///
    /// Method exists solely for checkpointing purposes and must only be
    /// called after `buffer_pool.flush_all_pages()` and
    /// `disk_manager.save_header()` succeed during a database checkpoint.
    pub fn truncate(&mut self) -> Result<(), Error> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    /// Reads the entire Wal file, parsing length-prefixed entries into a batch.
    /// Terminates cleanly when encountering EOF or 0 for length-prefix.
    ///
    /// Returns an error if a torn write is detected.
    pub fn read_batch(&mut self) -> Result<Vec<WalRecord>, Error> {
        let mut batch = Vec::new();

        // Ensure we start reading from the very beginning of the log file.
        self.file.seek(SeekFrom::Start(0))?;

        let mut reader = BufReader::new(&self.file);
        let mut len_buf = [0u8; 4];

        use ErrorKind::*;
        loop {
            // Attempt to read length indicator of exactly 4 bytes.
            match reader.read_exact(&mut len_buf) {
                Err(err) if err.kind() == UnexpectedEof => {
                    // Expected: we reached the end of log file.
                    break;
                }
                Err(err) => return Err(Error::Io(err)),
                Ok(_) => {}
            }
            let record_len = u32::from_le_bytes(len_buf) as usize;

            // zero length prefix means end of valid log data.
            if record_len == 0 {
                break;
            }
            let mut buffer = vec![0u8; record_len];

            // Read the exact payload bytes into buffer.
            match reader.read_exact(&mut buffer) {
                Err(err) if err.kind() == UnexpectedEof => {
                    return Err(Error::CorruptPage(format!(
                        "torn write: header reported {} bytes, buf file ended unexpectedly",
                        record_len
                    )));
                }
                Err(err) => return Err(Error::Io(err)),
                Ok(_) => {}
            }
            let entry = WalRecord::decode(&buffer)?;
            batch.push(entry);
        }
        Ok(batch)
    }
}

/*  TODO: Too many changes, will write tests if I see something break when running the query
directly now, since we have the entire flow accessible now. */
