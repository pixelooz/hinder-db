use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use parking_lot::{Mutex, RwLock};

use crate::{
    catalog::manager::CatalogManager,
    error::Error,
    execution::executor::ExecutionContext,
    planner::Planner,
    relation::tuple::Tuple,
    sql::{lexer::Lexer, parser::Parser},
    storage::{buffer_pool::BufferPool, page::DiskManager, wal::WalManager},
};

/// The global instance of our database. It owns all the state of our database and
/// provides thread-safe access to storage, metadata catalog and enables you to
/// create lightweight connections to the database.
#[derive(Debug)]
pub struct Database {
    buffer_pool: Arc<BufferPool>,
    catalog: Arc<RwLock<CatalogManager>>,
    next_txn_id: AtomicU64,
}

impl Database {
    /// Boots the database engine, Recovers from the Wal(todo), initializes the BufferPool,
    /// and bootstrap the in-memory Catalog from the system pages.
    pub fn open<P>(db_path: P, wal_path: P, pool_cap: usize, sync: bool) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let disk_manager = DiskManager::open(db_path)?;
        let wal_manager = Arc::new(Mutex::new(WalManager::open(wal_path, sync)?));

        let buffer_pool = BufferPool::new(disk_manager, pool_cap, wal_manager.clone());

        // run the recovery engine here
        let mut catalog = CatalogManager::new();
        catalog.bootstrap(&buffer_pool)?;

        Ok(Self {
            buffer_pool: Arc::new(buffer_pool),
            catalog: Arc::new(RwLock::new(catalog)),
            next_txn_id: AtomicU64::new(1),
        })
    }

    /// Spawns a new session connected to this database instance.
    pub fn connect(&self) -> Connection<'_> {
        Connection { db: self }
    }
}

/// A lightweight session for executing SQL queries.
pub struct Connection<'a> {
    db: &'a Database,
}

impl<'a> Connection<'a> {
    /// Parses, plans, and executes a raw SQL string, returning a collection of Tuples.
    pub fn execute(&mut self, sql: &str) -> Result<Vec<Tuple>, Error> {
        let lexer = Lexer::new(sql);

        let mut parser = Parser::new(lexer)?;
        let stmt = parser.parse_statement()?;

        let mut executor = {
            let catalog_guard = self.db.catalog.read();
            let planner = Planner::new(&catalog_guard);
            planner.plan_statement(stmt)?
        };
        let txn_id = self.db.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let mut ctx = ExecutionContext::new(&self.db.buffer_pool, &self.db.catalog, txn_id);
        let mut results = Vec::new();
        while let Some(tuple) = executor.next(&mut ctx)? {
            results.push(tuple);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fs, path::Path};

    use crate::shrimp::Database;

    #[test]
    fn test_complete_database_demo() -> Result<(), Box<dyn Error>> {
        let db_path = Path::new("/Volumes/External T7/test_full.db");
        let wal_path = Path::new("/Volumes/External T7/test_full.wal");

        let db = Database::open(db_path, wal_path, 100, true)?;
        let mut conn = db.connect();
        let tuples = conn.execute("CREATE TABLE users (id INT, name VARCHAR(255))")?;
        dbg!(tuples);

        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
        Ok(())
    }
}
