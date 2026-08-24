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
    concurrency::{lock_manager::LockManager, transaction::Transaction},
    error::Error,
    execution::ExecutionContext,
    manager::CatalogManager,
    planner::Planner,
    relation::{schema::Schema, tuple::Tuple},
    sql::{ast::Statement, lexer::Lexer, parser::Parser},
    storage::{
        buffer_pool::BufferPool, flusher::BackgroundFlusher, page::DiskManager, wal::WalManager,
    },
};

/// The output payload returned to the user.
#[derive(Debug)]
pub enum ResultSet {
    /// Returned by DQL operations(SELECT). Included column metadata and data rows.
    Query { rows: Vec<Tuple>, schema: Schema },

    /// Returned by DML/DDL operations(INSERT/UPDATE/DELETE/CREATE).
    Mutation { rows_affected: usize },
}

/// The global instance of our database. It owns all the state of our database and
/// provides thread-safe access to storage, metadata catalog and enables you to
/// create lightweight connections to the database.
#[derive(Debug)]
pub struct Database {
    buffer_pool: Arc<BufferPool>,
    catalog: Arc<RwLock<CatalogManager>>,
    lock_manager: Arc<LockManager>,
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
            lock_manager: Arc::new(LockManager::new()),
            next_txn_id: AtomicU64::new(1),
            _flusher: flusher,
        })
    }

    /// Spawns a new session connected to this database instance.
    pub fn connect(&self) -> Connection<'_> {
        Connection {
            database: self,
            active_txn: None,
        }
    }
}

/// A stateful session for executing SQL queries. Holds the active transaction state
/// allowing multi statement transaction.
pub struct Connection<'a> {
    database: &'a Database,
    active_txn: Option<Transaction>,
}

impl<'a> Connection<'a> {
    /// Parses and executes a raw SQL script/cli-string containing multiple statements.
    /// Returns a collection of results corresponding to each executed statement.
    pub fn execute_batch(&mut self, script: &str) -> Result<Vec<ResultSet>, Error> {
        let lexer = Lexer::new(script);
        let mut parser = Parser::new(lexer)?;

        let statements = parser.parse_script()?;
        let mut results = Vec::new();

        for stmt in statements {
            let result = self.execute_statement(stmt)?;
            results.push(result);
        }
        Ok(results)
    }

    /// Executes a single already parsed Ast statement. It manages transaction boundaries,
    /// auto-commits, and volcano pipeline.
    pub fn execute_statement(&mut self, stmt: Statement) -> Result<ResultSet, Error> {
        match stmt {
            Statement::Commit => return self.handle_commit(),
            Statement::Begin => return self.handle_begin(),
            Statement::Rollback => return self.handle_rollback(),
            _ => {}
        }
        let query_plan = {
            let catalog = self.database.catalog.read();
            let planner = Planner::new(&catalog);
            planner.plan_statement(stmt)?
        };
        let mut executor = query_plan.executor;

        // Determine if this is an explicit transaction or auto transaction.
        let is_auto_txn = self.active_txn.is_none();
        if is_auto_txn {
            self.handle_begin()?;
        }
        // We can unwrap because we just confirmed above that active_txn is_some;
        let txn_id = self.active_txn.as_ref().unwrap().txn_id;

        let mut ctx = ExecutionContext::new(
            &self.database.buffer_pool,
            &self.database.catalog,
            &self.database.lock_manager,
            txn_id,
        );
        let mut rows = Vec::new();
        let mut rows_affected = 0;
        loop {
            match executor.next(&mut ctx) {
                Err(exec_err) => {
                    // If any error does occur during a transaction we must abort
                    // the transaction.
                    if let Err(abort_err) = self.handle_rollback() {
                        return Err(Error::Io(io::Error::other(format!(
                            "CRITICAL: execution failed: {:?}, AND rollback failed: {:?}",
                            exec_err, abort_err,
                        ))));
                    }
                    return Err(exec_err);
                }
                Ok(None) => break,
                Ok(Some(tuple)) => {
                    rows.push(tuple);
                    rows_affected += 1;
                }
            }
        }
        // If not an explicit transaction handle the auto commit.
        if is_auto_txn {
            self.handle_commit()?;
        }
        let result_set = if query_plan.is_query {
            ResultSet::Query {
                rows,
                schema: query_plan.schema,
            }
        } else {
            ResultSet::Mutation { rows_affected }
        };
        Ok(result_set)
    }

    /// Initializes a `Transaction` with the next logical txn_id.
    fn handle_begin(&mut self) -> Result<ResultSet, Error> {
        if self.active_txn.is_some() {
            return Err(Error::ActionNotAllowed(
                "Transaction already active.".into(),
            ));
        }
        let txn_id = self.database.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let txn = Transaction::new(txn_id, self.database.buffer_pool.clone());
        self.active_txn = Some(txn);
        Ok(ResultSet::Mutation { rows_affected: 0 })
    }

    /// Commit the active transaction if there is any.
    fn handle_commit(&mut self) -> Result<ResultSet, Error> {
        if let Some(mut txn) = self.active_txn.take() {
            txn.commit()?;
        } else {
            return Err(Error::ActionNotAllowed("No active transaction".into()));
        }
        Ok(ResultSet::Mutation { rows_affected: 0 })
    }

    /// Rollback the active transaction if there is any.
    fn handle_rollback(&mut self) -> Result<ResultSet, Error> {
        if let Some(mut txn) = self.active_txn.take() {
            txn.abort()?;
        } else {
            return Err(Error::ActionNotAllowed("No active transaction".into()));
        }
        Ok(ResultSet::Mutation { rows_affected: 0 })
    }
}

