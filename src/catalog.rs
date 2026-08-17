pub(crate) mod manager;

use crate::relation::{
    schema::{Column, Schema},
    types::DataType,
};

/// Generates the hardcoded schema for the `sys_pages` catalog.
/// This tracks the `root_page_id` for every table in the database and also keeps
/// track of the last inserted row_id.
pub fn sys_pages_schema() -> Schema {
    Schema::new(vec![
        Column::new("table_name", DataType::Varchar, Some(255)),
        Column::new("root_page_id", DataType::BigInt, None),
    ])
}

/// Generates the hardcoded schema for `sys_schema` catalog.
/// This tracks the columns layout for every user-created table.
pub fn sys_schema_schema() -> Schema {
    Schema::new(vec![
        Column::new("table_name", DataType::Varchar, Some(255)),
        Column::new("field_name", DataType::Varchar, Some(255)),
        Column::new("field_type", DataType::Int, None),
        Column::new("field_length", DataType::Int, None),
    ])
}

/// Generates the schema for `sys_index` catalog.
/// Tracks secondary indexes mapped to base tables.
pub fn sys_index_schema() -> Schema {
    Schema::new(vec![
        Column::new("index_name", DataType::Varchar, Some(255)),
        Column::new("table_name", DataType::Varchar, Some(255)),
        Column::new("column_name", DataType::Varchar, Some(255)),
        Column::new("is_unique", DataType::Boolean, None),
        Column::new("root_page_id", DataType::BigInt, None),
    ])
}
