use std::{
    io,
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
    storage::{
        buffer_pool::BufferPool, flusher::BackgroundFlusher, page::DiskManager, wal::WalManager,
    },
};

/// The global instance of our database. It owns all the state of our database and
/// provides thread-safe access to storage, metadata catalog and enables you to
/// create lightweight connections to the database.
#[derive(Debug)]
pub struct Database {
    buffer_pool: Arc<BufferPool>,
    catalog: Arc<RwLock<CatalogManager>>,
    next_txn_id: AtomicU64,
    _flusher: BackgroundFlusher,
}

impl Database {
    /// Boots the database engine, Recovers from the Wal(todo), initializes the BufferPool,
    /// and bootstrap the in-memory Catalog from the system pages.
    pub fn open<P>(db_path: P, wal_path: P, pool_cap: usize) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let disk_manager = DiskManager::open(db_path)?;
        let wal_manager = Arc::new(Mutex::new(WalManager::open(wal_path)?));

        let buffer_pool = BufferPool::new(disk_manager, pool_cap, wal_manager.clone());

        // run the recovery engine here
        let mut catalog = CatalogManager::new();
        catalog.bootstrap(&buffer_pool)?;

        let buffer_pool = Arc::new(buffer_pool);
        let flusher = BackgroundFlusher::start(buffer_pool.clone());

        Ok(Self {
            buffer_pool,
            catalog: Arc::new(RwLock::new(catalog)),
            next_txn_id: AtomicU64::new(1),
            _flusher: flusher,
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
        loop {
            match executor.next(&mut ctx) {
                Ok(None) => {
                    // pipeline exhausted/finished successfully.
                    self.db.buffer_pool.commit_transaction(txn_id)?;
                    break;
                }
                Ok(Some(tuple)) => results.push(tuple),
                Err(err) => {
                    if let Err(abort_err) = self.db.buffer_pool.abort_transaction(txn_id) {
                        return Err(Error::Io(io::Error::other(format!(
                            "transaction failed: {:?}, AND rollback failed {:?}",
                            err, abort_err,
                        ))));
                    }
                    return Err(err);
                }
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fs, path::Path};

    use crate::{pellet::Database, relation::types::Value};

    fn cleanup_files(db_path: &Path, wal_path: &Path) {
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
    }

    #[test]
    fn test_create_table_demo() -> Result<(), Box<dyn Error>> {
        let wal_path = Path::new("/Volumes/External T7/create_table.wal");
        let db_path = Path::new("/Volumes/External T7/create_table.db");
        cleanup_files(&db_path, &wal_path);

        let db = Database::open(db_path, wal_path, 100)?;
        let mut conn = db.connect();
        let tuples = conn.execute("CREATE TABLE users (id INT, name VARCHAR(255))")?;
        dbg!(tuples);

        cleanup_files(&db_path, &wal_path);
        Ok(())
    }

    #[test]
    fn end_to_end_create_and_insert() -> Result<(), Box<dyn Error>> {
        let wal_path = Path::new("/Volumes/External T7/create_and_insert.wal");
        let db_path = Path::new("/Volumes/External T7/create_and_insert.db");
        cleanup_files(&db_path, &wal_path);

        let db = Database::open(db_path, wal_path, 100).expect("Failed to boot database");
        let mut conn = db.connect();

        let create_table_sql = "CREATE TABLE users (id INT, name VARCHAR(50), infinite_money BIGINT, is_broke BOOLEAN);";

        let create_table_res = conn
            .execute(create_table_sql)
            .expect("CREATE TABLE failed");

        assert!(create_table_res.is_empty(), "DDL should returns zero tuple");

        let create_index_sql = "CREATE INDEX idx_name ON users (name)";

        let create_index_res = conn
            .execute(create_index_sql)
            .expect("CREATE INDEX failed");

        assert!(create_index_res.is_empty(), "DDL should returns zero tuple");

        let insert_sql =
            "INSERT INTO users (id, name, is_broke) VALUES (1, 'Parth', true), (2, 'Juhi', false)";
        let insert_res = conn.execute(insert_sql).expect("INSERT failed");

        dbg!(&insert_res);
        assert_eq!(insert_res.len(), 2);

        assert_eq!(insert_res[0].values[0], Value::Int(1));
        assert_eq!(insert_res[0].values[1], Value::Varchar("Parth".into()));
        assert_eq!(insert_res[0].values[2], Value::Null);
        assert_eq!(insert_res[0].values[3], Value::Boolean(true));

        assert_eq!(insert_res[1].values[0], Value::Int(2));
        assert_eq!(insert_res[1].values[1], Value::Varchar("Juhi".into()));
        assert_eq!(insert_res[1].values[2], Value::Null);
        assert_eq!(insert_res[1].values[3], Value::Boolean(false));

        let insert_sql =
            "INSERT INTO users VALUES (3, 'Parth2', 0, true), (4, 'Juhi2', 100, false)";

        let insert_res = conn.execute(insert_sql).expect("INSERT failed");
        dbg!(&insert_res);
        assert_eq!(insert_res.len(), 2);

        assert_eq!(insert_res[0].values[0], Value::Int(3));
        assert_eq!(insert_res[0].values[1], Value::Varchar("Parth2".into()));
        assert_eq!(insert_res[0].values[2], Value::BigInt(0));
        assert_eq!(insert_res[0].values[3], Value::Boolean(true));

        assert_eq!(insert_res[1].values[0], Value::Int(4));
        assert_eq!(insert_res[1].values[1], Value::Varchar("Juhi2".into()));
        assert_eq!(insert_res[1].values[2], Value::BigInt(100));
        assert_eq!(insert_res[1].values[3], Value::Boolean(false));

        let select_sql = "SELECT * FROM users WHERE id > 1;";
        let select_res = conn.execute(select_sql).expect("SELECT failed");

        dbg!(&select_res);

        assert_eq!(select_res.len(), 3);
        assert_eq!(select_res[0].values[1], Value::Varchar("Juhi".into()));
        assert_eq!(select_res[1].values[1], Value::Varchar("Parth2".into()));
        assert_eq!(select_res[2].values[1], Value::Varchar("Juhi2".into()));

        cleanup_files(db_path, wal_path);
        Ok(())
    }
}
