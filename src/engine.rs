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
    execution::ExecutionContext,
    planner::Planner,
    relation::{schema::Schema, tuple::Tuple},
    sql::{lexer::Lexer, parser::Parser},
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
    pub fn execute(&mut self, query: &str) -> Result<ResultSet, Error> {
        let lexer = Lexer::new(query);

        let mut parser = Parser::new(lexer)?;
        let stmt = parser.parse_statement()?;

        let query_plan = {
            let catalog_guard = self.db.catalog.read();
            let planner = Planner::new(&catalog_guard);
            planner.plan_statement(stmt)?
        };
        let mut executor = query_plan.executor;

        let txn_id = self.db.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let mut ctx = ExecutionContext::new(&self.db.buffer_pool, &self.db.catalog, txn_id);

        let mut rows = Vec::new();
        let mut rows_affected = 0;

        loop {
            match executor.next(&mut ctx) {
                Ok(None) => {
                    // pipeline exhausted/finished successfully.
                    self.db.buffer_pool.commit_transaction(txn_id)?;
                    break;
                }
                Ok(Some(tuple)) => {
                    rows.push(tuple);
                    rows_affected += 1;
                }
                Err(exec_err) => {
                    if let Err(abort_err) = self.db.buffer_pool.abort_transaction(txn_id) {
                        return Err(Error::Io(io::Error::other(format!(
                            "transaction failed: {:?}, AND rollback failed {:?}",
                            exec_err, abort_err,
                        ))));
                    }
                    return Err(exec_err);
                }
            }
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
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use crate::{
        engine::{Database, ResultSet},
        error::Error,
        relation::types::Value,
    };

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Generate unique db paths and if some already exists delete them.
    fn setup_db_test(test_name: &str) -> (String, String) {
        let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let db_path = format!("/Volumes/External T7/test_{}_{}.db", test_name, count);
        let wal_path = format!("/Volumes/External T7/test_{}_{}.wal", test_name, count);
        let _ = fs::remove_file(&db_path);
        let _ = fs::remove_file(&wal_path);
        (db_path, wal_path)
    }

    fn cleanup_files(db_path: &str, wal_path: &str) {
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
    }

    #[test]
    fn visualize_create_table_index_tuple() {
        let wal_path = format!("/Volumes/External T7/create_table.wal");
        let db_path = format!("/Volumes/External T7/create_table.db");
        cleanup_files(&db_path, &wal_path);

        let db = Database::open(&db_path, &wal_path, 100).unwrap();
        let mut conn = db.connect();

        let mut query = "CREATE TABLE users (id INT, name VARCHAR(100))";
        let result_set = conn.execute(query).expect("CREATE TABLE failed");

        dbg!(result_set);

        query = "CREATE INDEX idx_name ON users(name)";
        let result_set = conn.execute(query).expect("CREATE INDEX failed");

        dbg!(result_set);

        query = "INSERT INTO users VALUES (1, 'parth'), (2, 'juhi');";
        let result_set = conn.execute(query).expect("INSERT INTO failed");

        dbg!(result_set);

        query = "SELECT id AS my_id, name AS not_my_name FROM users";
        let result_set = conn.execute(query).expect("INSERT INTO failed");

        dbg!(result_set);

        cleanup_files(&db_path, &wal_path);
    }

    #[test]
    fn test_fail_insert_varchar_limit_exceeded() {
        let wal_path = format!("/Volumes/External T7/create_table.wal");
        let db_path = format!("/Volumes/External T7/create_table.db");
        cleanup_files(&db_path, &wal_path);

        let db = Database::open(&db_path, &wal_path, 100).unwrap();
        let mut conn = db.connect();

        let mut query = "CREATE TABLE users (id INT, name VARCHAR(4))";
        let result_set = conn.execute(query).expect("CREATE TABLE failed");

        dbg!(result_set);

        query = "CREATE INDEX idx_name ON users(name)";
        let result_set = conn.execute(query).expect("CREATE INDEX failed");

        dbg!(result_set);

        query = "INSERT INTO users VALUES (1, 'parth'), (2, 'juhi');";
        let result_set = conn.execute(query);
        assert!(
            result_set.is_err(),
            "inserting longer VARCHAR should have errored"
        );
        assert!(
            matches!(
                result_set.as_ref().err(),
                Some(Error::ConstraintViolation(_))
            ),
            "Wrong error type {:?}, should be Error::ConstraintViolation",
            &result_set
        );
        query = "SELECT id AS my_id, name AS not_my_name FROM users";
        let result_set = conn.execute(query).expect("SELECT failed");

        dbg!(result_set);

        cleanup_files(&db_path, &wal_path);
    }

    #[test]
    fn end_to_end_create_and_insert() {
        let wal_path = format!("/Volumes/External T7/create_and_insert.wal");
        let db_path = format!("/Volumes/External T7/create_and_insert.db");
        cleanup_files(&db_path, &wal_path);

        let db = Database::open(&db_path, &wal_path, 100).expect("Failed to boot database");
        let mut conn = db.connect();

        let create_table_sql = "CREATE TABLE users (id INT, name VARCHAR(50), infinite_money BIGINT, is_broke BOOLEAN);";

        let ResultSet::Mutation { rows_affected } = conn
            .execute(create_table_sql)
            .expect("CREATE TABLE failed")
        else {
            panic!("Expected Mutation result")
        };
        assert_eq!(rows_affected, 0, "DDL should return zero rows affected");

        let create_index_name = "CREATE INDEX idx_name ON users (name)";
        let ResultSet::Mutation { rows_affected } = conn
            .execute(create_index_name)
            .expect("CREATE INDEX failed")
        else {
            panic!("Expected Mutation result")
        };
        assert_eq!(rows_affected, 0, "DDL should return zero rows affected");

        let create_index_id = "CREATE INDEX idx_id ON users (id)";
        let ResultSet::Mutation { rows_affected } = conn
            .execute(create_index_id)
            .expect("CREATE INDEX failed")
        else {
            panic!("Expected Mutation result")
        };
        assert_eq!(rows_affected, 0, "DDL should return zero rows affected");

        let insert_sql =
            "INSERT INTO users (id, name, is_broke) VALUES (1, 'Parth', true), (2, 'Juhi', false)";
        let ResultSet::Mutation { rows_affected } =
            conn.execute(insert_sql).expect("INSERT failed")
        else {
            panic!("Expected Mutation result")
        };
        assert_eq!(rows_affected, 2);

        // Fetch the inserted rows to verify NULL padding
        let ResultSet::Query { rows: insert_res, .. } = conn
            .execute("SELECT * FROM users WHERE id <= 2;")
            .unwrap()
        else {
            panic!("Expected Query result")
        };

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

        let ResultSet::Mutation { rows_affected } =
            conn.execute(insert_sql).expect("INSERT failed")
        else {
            panic!("Expected Mutation result")
        };
        assert_eq!(rows_affected, 2);

        // Fetch the newly inserted rows to verify direct insertion
        let ResultSet::Query {
            rows: insert_res2, ..
        } = conn
            .execute("SELECT * FROM users WHERE id > 2;")
            .unwrap()
        else {
            panic!("Expected Query result")
        };
        assert_eq!(insert_res2.len(), 2);

        assert_eq!(insert_res2[0].values[0], Value::Int(3));
        assert_eq!(insert_res2[0].values[1], Value::Varchar("Parth2".into()));
        assert_eq!(insert_res2[0].values[2], Value::BigInt(0));
        assert_eq!(insert_res2[0].values[3], Value::Boolean(true));

        assert_eq!(insert_res2[1].values[0], Value::Int(4));
        assert_eq!(insert_res2[1].values[1], Value::Varchar("Juhi2".into()));
        assert_eq!(insert_res2[1].values[2], Value::BigInt(100));
        assert_eq!(insert_res2[1].values[3], Value::Boolean(false));

        let select_sql = "SELECT * FROM users WHERE id > 1;";
        let ResultSet::Query { rows: select_res, .. } =
            conn.execute(select_sql).expect("SELECT failed")
        else {
            panic!("Expected Query result")
        };

        assert_eq!(select_res.len(), 3);
        assert_eq!(select_res[0].values[1], Value::Varchar("Juhi".into()));
        assert_eq!(select_res[1].values[1], Value::Varchar("Parth2".into()));
        assert_eq!(select_res[2].values[1], Value::Varchar("Juhi2".into()));

        cleanup_files(&db_path, &wal_path);
    }

    #[test]
    fn test_crud_and_secondary_index_maintenance() {
        let (db_path, wal_path) = setup_db_test("crud");

        let db = Database::open(&db_path, &wal_path, 20).unwrap();
        let mut conn = db.connect();

        let mut query = "CREATE TABLE users (id INT, name VARCHAR(255), age INT);";
        conn.execute(query).unwrap();

        query = "CREATE INDEX idx_age ON users(age);";
        conn.execute(query).unwrap();

        query = "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 30), (3, 'Charlie', 40);";
        conn.execute(query).unwrap();

        query = "SELECT * FROM users WHERE age = 30;";
        let ResultSet::Query { rows: res, .. } = conn.execute(query).unwrap() else {
            panic!("Expected Query")
        };
        assert_eq!(res.len(), 2, "Should find two users with age 30");

        // This forces the UpdateExecutor to remove 'Bob' from the '30' posting list
        // and add him to '35'
        query = "UPDATE users SET age = 35 WHERE name = 'Bob';";
        let ResultSet::Mutation { rows_affected } = conn.execute(query).unwrap() else {
            panic!("Expected Mutation")
        };
        assert_eq!(rows_affected, 1);

        query = "SELECT * FROM users WHERE age = 30;";
        let ResultSet::Query { rows: res_30, .. } = conn.execute(query).unwrap() else {
            panic!("Expected Query")
        };

        assert_eq!(res_30.len(), 1, "Only Alice should remain at age 30");
        assert_eq!(res_30[0].values[1], Value::Varchar("Alice".into()));

        query = "SELECT * FROM users WHERE age = 35;";
        let ResultSet::Query { rows: res_35, .. } = conn.execute(query).unwrap() else {
            panic!("Expected Query")
        };

        assert_eq!(res_35.len(), 1, "Bob should now be at age 35");
        assert_eq!(res_35[0].values[1], Value::Varchar("Bob".into()));

        query = "DELETE FROM users WHERE name = 'Charlie';";
        let ResultSet::Mutation { rows_affected } = conn.execute(query).unwrap() else {
            panic!("Expected Mutation")
        };
        assert_eq!(rows_affected, 1);

        let ResultSet::Query { rows: res_all, .. } = conn.execute("SELECT * FROM users;").unwrap()
        else {
            panic!("Expected Query")
        };
        assert_eq!(res_all.len(), 2, "Charlie should be deleted");

        cleanup_files(&db_path, &wal_path);
    }

    #[test]
    fn test_unique_constraint_and_runtime_rollback() {
        let (db_path, wal_path) = setup_db_test("rollback");
        let db = Database::open(&db_path, &wal_path, 50).unwrap();
        let mut conn = db.connect();

        conn.execute("CREATE TABLE accounts (id INT, email VARCHAR(255));")
            .expect("CREATE TABLE accounts failed");

        conn.execute("CREATE UNIQUE INDEX idx_email ON accounts (email);")
            .expect("CREATE UNIQUE index failed");

        conn.execute("INSERT INTO accounts VALUES (1, 'test@example.com');")
            .expect("INSERT INTO accounts failed");

        // Attempt to insert duplicate email, should fail.
        let err_res = conn.execute("INSERT INTO accounts VALUES (2, 'test@example.com');");
        assert!(err_res.is_err(), "Expected unique constraint violation");

        if let Err(Error::ConstraintViolation(msg)) = err_res {
            assert!(
                msg.contains("unique constraint"),
                "Wrong error type returned"
            );
        } else {
            panic!("Expected ConstraintViolation error, got {:#?}", err_res);
        }

        // VERIFY ROLLBACK: The primary table should NOT contain the aborted record (id = 2)
        let ResultSet::Query { rows: res, .. } = conn.execute("SELECT * FROM accounts;").unwrap()
        else {
            panic!("Expected Query")
        };
        assert_eq!(
            res.len(),
            1,
            "Transaction rollback failed: partial insert detected!"
        );
        assert_eq!(res[0].values[0], Value::Int(1));

        cleanup_files(&db_path, &wal_path);
    }

    #[test]
    fn test_planner_semantic_failures() {
        let (db_path, wal_path) = setup_db_test("semantics");
        let db = Database::open(&db_path, &wal_path, 50).unwrap();
        let mut conn = db.connect();

        conn.execute("CREATE TABLE t1 (a INT, b BOOLEAN);")
            .expect("CREATE TABLE failed");

        let err_arity = conn.execute("INSERT INTO t1 VALUES (1, TRUE, 99);");
        assert!(matches!(err_arity, Err(Error::SyntaxErr(_))));

        let err_type = conn.execute("INSERT INTO t1 VALUES ('string', TRUE);");
        assert!(matches!(err_type, Err(Error::SyntaxErr(_))));

        // Invalid Column in UPDATE
        let err_col = conn.execute("UPDATE t1 SET fake_col = 5 WHERE a = 1;");
        assert!(matches!(err_col, Err(Error::ColumnNotFound(_))));

        // 4. Type Mismatch in UPDATE
        let err_type = conn.execute("UPDATE t1 SET b = 100 WHERE a = 1;");
        assert!(matches!(err_type, Err(Error::SyntaxErr(_))));
        cleanup_files(&db_path, &wal_path);
    }

    #[test]
    fn test_index_scan_all_operators() {
        let (db_path, wal_path) = setup_db_test("index_scan_ops");

        let db = Database::open(&db_path, &wal_path, 20).unwrap();
        let mut conn = db.connect();

        // 1. Setup Table and Index
        conn.execute("CREATE TABLE employees (id INT, name VARCHAR(255), salary INT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_salary ON employees(salary);")
            .unwrap();

        // 2. Insert Data
        // Salaries are 50, 60, 70, 80, 90
        let insert_query = "INSERT INTO employees VALUES
                (1, 'Alice', 50),
                (2, 'Bob', 60),
                (3, 'Charlie', 70),
                (4, 'Diana', 80),
                (5, 'Eve', 90);";
        conn.execute(insert_query).unwrap();

        // Helper closure to extract rows and keep the test clean
        let mut run_query = |sql: &str| {
            let ResultSet::Query { rows, .. } = conn.execute(sql).unwrap() else {
                panic!("Expected Query ResultSet for SQL: {}", sql)
            };
            rows
        };

        // --- TEST 1: Exact Match (Eq) ---
        let res_eq = run_query("SELECT * FROM employees WHERE salary = 70;");
        assert_eq!(res_eq.len(), 1, "Eq should find exactly 1 record");
        assert_eq!(res_eq[0].values[1], Value::Varchar("Charlie".into()));

        // --- TEST 2: Greater Than (Gt) ---
        // Should skip 70, and return 80 and 90
        let res_gt = run_query("SELECT * FROM employees WHERE salary > 70;");
        assert_eq!(res_gt.len(), 2, "Gt should find 2 records");
        assert_eq!(res_gt[0].values[1], Value::Varchar("Diana".into()));
        assert_eq!(res_gt[1].values[1], Value::Varchar("Eve".into()));

        // --- TEST 3: Greater Than or Equal (Gte) ---
        // Should include 70, 80, and 90
        let res_gte = run_query("SELECT * FROM employees WHERE 70 <= salary;");
        assert_eq!(res_gte.len(), 3, "Gte should find 3 records");
        assert_eq!(res_gte[0].values[1], Value::Varchar("Charlie".into()));
        assert_eq!(res_gte[2].values[1], Value::Varchar("Eve".into()));

        // --- TEST 4: Less Than (Lt) ---
        // Should start from the beginning and stop BEFORE 70 (returns 50, 60)
        let res_lt = run_query("SELECT * FROM employees WHERE salary < 70;");
        assert_eq!(res_lt.len(), 2, "Lt should find 2 records");
        assert_eq!(res_lt[0].values[1], Value::Varchar("Alice".into()));
        assert_eq!(res_lt[1].values[1], Value::Varchar("Bob".into()));

        // --- TEST 5: Less Than or Equal (Lte) ---
        // Should start from the beginning and stop AFTER 70 (returns 50, 60, 70)
        let res_lte = run_query("SELECT * FROM employees WHERE salary <= 70;");
        assert_eq!(res_lte.len(), 3, "Lte should find 3 records");
        assert_eq!(res_lte[0].values[1], Value::Varchar("Alice".into()));
        assert_eq!(res_lte[2].values[1], Value::Varchar("Charlie".into()));

        // --- TEST 6: Empty Result Guard ---
        // Ensures Point Lookup safely returns 0 rows if key doesn't exist
        let res_empty = run_query("SELECT * FROM employees WHERE salary = 999;");
        assert_eq!(
            res_empty.len(),
            0,
            "Eq on non-existent key should return 0 rows"
        );

        // --- TEST 7: Range Scan Miss (Gte) ---
        // Ensures Range Scan safely handles falling off the end of the B-Tree
        let res_out_of_bounds = run_query("SELECT * FROM employees WHERE salary >= 999;");
        assert_eq!(
            res_out_of_bounds.len(),
            0,
            "Gte out of bounds should return 0 rows"
        );

        cleanup_files(&db_path, &wal_path);
    }
}
