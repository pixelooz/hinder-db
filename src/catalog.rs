pub(crate) mod manager;

use crate::relation::{
    schema::{Column, Schema},
    types::DataType,
};

/// Generates the hardcoded schema for the `sys_pages` catalog.
/// This tracks the `root_page_id` for every table in the database and also keeps
/// track of the last inserted row_id.
#[rustfmt::skip]
pub fn sys_pages_schema() -> Schema {
    Schema::new(vec![
        Column::new(Some("sys_pages"), "table_name", DataType::Varchar, Some(255)),
        Column::new(Some("sys_pages"), "root_page_id", DataType::BigInt, None),
    ])
}

/// Generates the hardcoded schema for `sys_schema` catalog.
/// This tracks the columns layout for every user-created table.
#[rustfmt::skip]
pub fn sys_schema_schema() -> Schema {
    Schema::new(vec![
        Column::new(Some("sys_schema"), "table_name", DataType::Varchar, Some(255)),
        Column::new(Some("sys_schema"), "field_name", DataType::Varchar, Some(255)),
        Column::new(Some("sys_schema"), "field_type", DataType::Int, None),
        Column::new(Some("sys_schema"), "field_length", DataType::Int, None),
    ])
}

/// Generates the schema for `sys_index` catalog.
/// Tracks secondary indexes mapped to base tables.
#[rustfmt::skip]
pub fn sys_index_schema() -> Schema {
    Schema::new(vec![
        Column::new(Some("sys_index"), "index_name", DataType::Varchar, Some(255)),
        Column::new(Some("sys_index"), "table_name", DataType::Varchar, Some(255)),
        Column::new(Some("sys_index"), "column_name", DataType::Varchar, Some(255)),
        Column::new(Some("sys_index"), "is_unique", DataType::Boolean, None),
        Column::new(Some("sys_index"), "root_page_id", DataType::BigInt, None),
    ])
}
