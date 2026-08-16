use std::{
    cmp,
    collections::HashMap,
    io::Cursor,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    catalog::{sys_index_schema, sys_pages_schema, sys_schema_schema},
    error::Error,
    relation::{
        schema::{Column, Schema},
        tuple::Tuple,
        types::{DataType, Value},
    },
    storage::{
        bptree::BpTree,
        buffer_pool::BufferPool,
        page::{BTreeNode, PageId},
    },
};

pub const SYS_TABLE_ROOTS_ROOT_ID: PageId = PageId(8192);
pub const SYS_SCHEMAS_ROOT_ID: PageId = PageId(16384);
pub const SYS_INDEXES_ROOT_ID: PageId = PageId(24576);

/// Represents the physical and logical properties of a secondary index.
#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub root_page_id: PageId,
    pub index_name: String,
    pub is_unique: bool,
    pub column_name: String,
}

/// The in-memory metadata cache for the entire database.
///
/// The `CatalogManager` is responsible for bootstrapping the database on startup,
/// reading the physical `sys_pages` and `sys_schema` BpTrees, and caching their
/// contents to provide constant time lookups for the Binder and Planner layer, when
/// queries are run.
#[derive(Debug)]
pub struct CatalogManager {
    /// Maps a table name to its physical B-Tree root [PageId].
    table_roots: HashMap<String, PageId>,

    /// Maps a table name to its logical [Schema] definition.
    table_schemas: HashMap<String, Schema>,

    /// Tracks secondary indexes: Map<TableName, Map<IndexName, IndexMeta>>
    index_roots: HashMap<String, HashMap<String, IndexMeta>>,

    /// Monotonic counters for primary key generation per table. Atomic for
    /// interior mutability across thread boundaries.
    table_row_ids: HashMap<String, AtomicU64>,

    /// Monotonic counter for inserting into the system catalog themselves.
    /// Only a single counter for all the 3 pages because it doesn't really
    /// matter for our use case since they only need to be unique and this
    /// will be unique. Plus its simpler this way, instead of having to manage
    /// 3 different ids.
    next_sys_row_id: AtomicU64,
}

impl CatalogManager {
    /// Initializes an empty CatalogManager.
    pub fn new() -> Self {
        Self {
            table_roots: HashMap::new(),
            table_schemas: HashMap::new(),
            index_roots: HashMap::new(),
            table_row_ids: HashMap::new(),
            next_sys_row_id: AtomicU64::new(1),
        }
    }

    /// Bootstraps the catalog from disk. If the database is empty, it allocates and
    /// initializes the system catalog pages.
    pub fn bootstrap(&mut self, pool: &BufferPool) -> Result<(), Error> {
        if pool.is_empty() {
            self.initialize_new_database(pool)?;
        }
        // Load `sys_pages` as {table_name -> root_page_id} in the catalog map
        Self::scan_system_table(
            pool,
            SYS_TABLE_ROOTS_ROOT_ID,
            &sys_pages_schema(),
            |tuple| {
                let table_name = tuple.values[0].varchar_to_str().ok_or_else(|| {
                    Error::CorruptPage("sys_pages table_name is not Varchar".into())
                })?;
                let root_page_id = tuple.values[1].bigint_to_i64().ok_or_else(|| {
                    Error::CorruptPage("sys_pages root_page_id is not BigInt".into())
                })?;
                self.table_roots
                    .insert(table_name.to_string(), PageId(root_page_id as u64));
                Ok(())
            },
        )?;
        let mut raw_columns: HashMap<String, Vec<Column>> = HashMap::new();

        // Load `sys_schema` as {table_name -> vec[columns]} in temp raw_columns
        Self::scan_system_table(pool, SYS_SCHEMAS_ROOT_ID, &sys_schema_schema(), |tuple| {
            let table_name = tuple.values[0]
                .varchar_to_str()
                .ok_or_else(|| Error::CorruptPage("sys_schema table_name is not Varchar".into()))?;

            let field_name = tuple.values[1]
                .varchar_to_str()
                .ok_or_else(|| Error::CorruptPage("sys_schema field_name is not Varchar".into()))?;

            let field_type = tuple.values[2]
                .int_to_i32()
                .ok_or_else(|| Error::CorruptPage("sys_schema field_type is not Int".into()))?;

            let field_length = tuple.values[3]
                .int_to_i32()
                .ok_or_else(|| Error::CorruptPage("sys_schema field_length is not Int".into()))?;

            let length = (field_length > 0).then_some(field_length as u32);
            let data_type = DataType::from_u8(field_type as u8)?;

            let column = Column::new(field_name, data_type, length);
            raw_columns
                .entry(table_name.to_string())
                .or_default()
                .push(column);
            Ok(())
        })?;
        let raw_col_iterator = raw_columns
            .into_iter()
            .map(|(name, cols)| (name, Schema::new(cols)));

        self.table_schemas.extend(raw_col_iterator);

        Self::scan_system_table(pool, SYS_INDEXES_ROOT_ID, &sys_index_schema(), |tuple| {
            let table_name = tuple.values[0]
                .varchar_to_str()
                .ok_or_else(|| Error::CorruptPage("sys_index table_name is not varchar".into()))?;

            let index_name = tuple.values[1]
                .varchar_to_str()
                .ok_or_else(|| Error::CorruptPage("sys_index index_name is not Varchar".into()))?;

            let col_name = tuple.values[2]
                .varchar_to_str()
                .ok_or_else(|| Error::CorruptPage("sys_index col_name is not Varchar".into()))?;

            let root_page_id = tuple.values[3]
                .bigint_to_i64()
                .ok_or_else(|| Error::CorruptPage("sys_index root_page_id is not BigInt".into()))?;

            let is_unique = tuple.values[4]
                .boolean_to_bool()
                .ok_or_else(|| Error::CorruptPage("sys_index is_unique is not Boolean".into()))?;

            let index_meta = IndexMeta {
                index_name: index_name.to_string(),
                column_name: col_name.to_string(),
                root_page_id: PageId(root_page_id as u64),
                is_unique,
            };
            self.index_roots
                .entry(table_name.into())
                .or_default()
                .insert(index_name.into(), index_meta);
            Ok(())
        })?;
        let mut max_sys_id = 0;
        for &sys_id in &[
            SYS_TABLE_ROOTS_ROOT_ID,
            SYS_SCHEMAS_ROOT_ID,
            SYS_INDEXES_ROOT_ID,
        ] {
            let max_val = BpTree::new(pool, sys_id).get_max_row_id()?;
            max_sys_id = cmp::max(max_sys_id, max_val);
        }
        self.next_sys_row_id = AtomicU64::new(max_sys_id + 1);

        for (name, &root_id) in &self.table_roots {
            let max_val = BpTree::new(pool, root_id).get_max_row_id()?;
            self.table_row_ids
                .insert(name.clone(), AtomicU64::new(max_val + 1));
        }
        Ok(())
    }

    /// Thread-safe generator for the next monotonic primary key of a table.
    pub fn generate_next_row_id(&self, table_name: &str) -> Result<u64, Error> {
        let counter = self
            .table_row_ids
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        Ok(counter.fetch_add(1, Ordering::SeqCst))
    }

    /// Retrieves the physical root PageId for a given table name.
    pub fn get_table_root(&self, table_name: &str) -> Result<PageId, Error> {
        self.table_roots
            .get(table_name)
            .copied()
            .ok_or_else(|| Error::TableNotFound(table_name.into()))
    }

    /// Retrieves the logical Schema for a given table name.
    pub fn get_table_schema(&self, table_name: &str) -> Result<&Schema, Error> {
        self.table_schemas
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.into()))
    }

    /// Retrieves all secondary indexes associated with a specific table.
    pub fn get_table_indexes(&self, table_name: &str) -> Option<&HashMap<String, IndexMeta>> {
        self.index_roots.get(table_name)
    }

    /// Allocates a new table, writing its metadata to the system catalogs:
    /// `sys_table_roots and sys_schemas`.
    pub fn create_table(
        &mut self,
        pool: &BufferPool,
        table_name: String,
        schema: Schema,
        txn_id: u64,
    ) -> Result<(), Error> {
        if self.table_roots.contains_key(&table_name) {
            return Err(Error::Duplicate(format!(
                "table '{}' already exists",
                table_name
            )));
        }
        let (root_page_id, root_frame) = pool.new_page(true)?;
        pool.begin_page_mutation(root_page_id, txn_id)?;
        root_frame.write().mark_dirty();

        // Inserting into sys_table_roots_page
        let sys_table_roots_tree = BpTree::new(pool, SYS_TABLE_ROOTS_ROOT_ID);
        let page_root_tuple = Tuple::new(vec![
            Value::Varchar(table_name.clone()),
            Value::BigInt(
                i64::try_from(root_page_id.0).expect("new table's page id exceeded i64 Max"),
            ),
        ]);
        let mut roots_buffer = Vec::new();
        page_root_tuple.encode(&sys_pages_schema(), &mut roots_buffer)?;

        let sys_row_id = self
            .next_sys_row_id
            .fetch_add(1, Ordering::SeqCst);

        sys_table_roots_tree.insert(sys_row_id, roots_buffer, txn_id)?;

        // Insert into sys_schema_page
        let sys_schema_tree = BpTree::new(pool, SYS_SCHEMAS_ROOT_ID);
        let sys_schema = sys_schema_schema();

        for col in &schema.columns {
            let schema_tuple = Tuple::new(vec![
                Value::Varchar(table_name.clone()),
                Value::Varchar(col.name.clone()),
                Value::Int(col.data_type as i32),
                Value::Int(col.length.unwrap_or(0) as i32),
            ]);
            let mut schema_buffer = Vec::new();
            schema_tuple.encode(&sys_schema, &mut schema_buffer)?;

            let sys_row_id = self
                .next_sys_row_id
                .fetch_add(1, Ordering::SeqCst);

            sys_schema_tree.insert(sys_row_id, schema_buffer, txn_id)?;
        }
        // Update in-memory cache
        self.table_roots
            .insert(table_name.clone(), root_page_id);
        self.table_schemas
            .insert(table_name.clone(), schema);
        self.table_row_ids
            .insert(table_name, AtomicU64::new(1));
        Ok(())
    }

    /// Allocates a new secondary index for a table, writing metadata to `sys_indexes`.
    pub fn create_index(
        &mut self,
        pool: &BufferPool,
        index_name: String,
        table_name: String,
        is_unique: bool,
        column_name: String,
        txn_id: u64,
    ) -> Result<(), Error> {
        let table_schema = self.get_table_schema(&table_name)?;
        if table_schema.get_col_idx(&column_name).is_err() {
            return Err(Error::ColumnNotFound(format!(
                "column '{}' does not exist in table '{}'",
                column_name, table_name
            )));
        }
        let table_indexes = self
            .index_roots
            .entry(table_name.clone())
            .or_default();

        if table_indexes.contains_key(&index_name) {
            return Err(Error::Duplicate(format!(
                "index '{}' already exists in table '{}'",
                index_name, table_name
            )));
        }
        let (root_page_id, root_frame) = pool.new_page(true)?;
        pool.begin_page_mutation(root_page_id, txn_id)?;
        root_frame.write().mark_dirty();

        let sys_index_tree = BpTree::new(pool, root_page_id);
        let index_tuple = Tuple::new(vec![
            Value::Varchar(table_name.clone()),
            Value::Varchar(index_name.clone()),
            Value::Boolean(is_unique),
            Value::BigInt(
                i64::try_from(root_page_id.0).expect("new index's page id exceeded i64 Max"),
            ),
            Value::Varchar(column_name.clone()),
        ]);
        let mut index_buffer = Vec::new();
        index_tuple.encode(&sys_index_schema(), &mut index_buffer)?;

        let sys_row_id = self
            .next_sys_row_id
            .fetch_add(1, Ordering::SeqCst);
        sys_index_tree.insert(sys_row_id, index_buffer, txn_id)?;

        let index_meta = IndexMeta {
            index_name: index_name.clone(),
            column_name,
            root_page_id,
            is_unique,
        };
        table_indexes.insert(index_name, index_meta);
        Ok(())
    }

    /// Locates and Reads system pages into memory and processes them against the
    /// provided schema before passing them to the given closure.
    fn scan_system_table<F>(
        pool: &BufferPool,
        root_id: PageId,
        schema: &Schema,
        mut process_tuple: F,
    ) -> Result<(), Error>
    where
        F: FnMut(Tuple) -> Result<(), Error>,
    {
        let mut curr_page_id = root_id;
        loop {
            let frame = pool.fetch_page(curr_page_id)?;
            let node_guard = frame.read();

            match &*node_guard {
                BTreeNode::Internal(node) => {
                    if !node.slot_array.is_empty() {
                        let rec_idx = node.slot_array[0] as usize;
                        curr_page_id = node.entries[rec_idx].child_page_id;
                    } else {
                        curr_page_id = node.rightmost_child_id;
                    }
                }
                _ => break,
            }
        }
        loop {
            let frame = pool.fetch_page(curr_page_id)?;
            let node_guard = frame.read();

            let BTreeNode::Leaf(node) = &*node_guard else {
                return Err(Error::CorruptPage(
                    "expected a leaf node during horizontal scan".into(),
                ));
            };
            for &rec_idx in &node.slot_array {
                let record = &node.records[rec_idx as usize];
                if !record.is_deleted {
                    let mut cursor = Cursor::new(&record.data);
                    let tuple = Tuple::decode(schema, &mut cursor)?;
                    process_tuple(tuple)?;
                }
            }
            if !node.has_next {
                break;
            }
            curr_page_id = node.next_page_id;
        }
        Ok(())
    }

    /// Allocates the baseline system pages for a completely fresh database.
    fn initialize_new_database(&mut self, pool: &BufferPool) -> Result<(), Error> {
        let sys_txn_id = 0;

        let (p1_id, p1_frame) = pool.new_page(true)?;
        pool.begin_page_mutation(p1_id, sys_txn_id)?;
        p1_frame.write().mark_dirty();

        let (p2_id, p2_frame) = pool.new_page(true)?;
        pool.begin_page_mutation(p2_id, sys_txn_id)?;
        p2_frame.write().mark_dirty();

        let (p3_id, p3_frame) = pool.new_page(true)?;
        pool.begin_page_mutation(p3_id, sys_txn_id)?;
        p3_frame.write().mark_dirty();

        if p1_id != SYS_TABLE_ROOTS_ROOT_ID
            || p2_id != SYS_SCHEMAS_ROOT_ID
            || p3_id != SYS_INDEXES_ROOT_ID
        {
            return Err(Error::CorruptPage(format!(
                "failed to allocate system pages sequentially
                sys_table_roots = {:?}, sys_schema = {:?},
                sys_index = {:?}",
                p1_id, p2_id, p3_id
            )));
        }
        pool.commit_transaction(sys_txn_id)?;
        pool.flush_all_pages()?;
        Ok(())
    }
}

// TODO: write tests unless it already works correctly :)
