use crate::{error::Error, relation::types::DataType};

/// Represents a single column definition within a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub length: Option<u32>,
}

impl Column {
    /// Creates an initialized Column with the given parameters.
    pub fn new(name: impl Into<String>, data_type: DataType, length: Option<u32>) -> Self {
        Self {
            name: name.into(),
            data_type,
            length,
        }
    }
}

/// The blueprint for a database table, mapping column names to vector indexes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    /// Creates an initialized Schema with the given columns.
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    /// Finds the vector index of a column by its string name.
    ///
    /// # Note
    /// This could be a HashMap but since row widths are minuscule, contiguous
    /// memory iterations will generally outperform Map look ups.
    pub fn get_col_idx(&self, column_name: &str) -> Result<usize, Error> {
        self.columns
            .iter()
            .position(|col| col.name == column_name)
            .ok_or_else(|| Error::ColumnNotFound(column_name.to_string()))
    }
}
