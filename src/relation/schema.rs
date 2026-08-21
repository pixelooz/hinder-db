use crate::{error::Error, relation::types::DataType};

/// Represents a single column definition within a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The table this column belongs to; needed for disambiguating column names during
    /// JOIN operations. Ex: users.id vs posts.id.
    pub table_name: Option<String>,
    pub name: String,
    pub data_type: DataType,
    pub length: Option<u32>,
}

impl Column {
    /// Creates an initialized Column with the given parameters.
    #[rustfmt::skip]
    pub fn new<T>(table_name: Option<T>, name: T, data_type: DataType, length: Option<u32>) -> Self
    where
        T: Into<String>
    {
        Self {
            table_name: table_name.map(Into::into),
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

    /// Finds the index of a column by its string name.
    pub fn get_col_idx(&self, column_name: &str) -> Result<usize, Error> {
        self.get_col_idx_with_qualifier(None, column_name)
    }

    /// Finds the index of the column by its name, optionally filtered by table
    /// qualifier.
    pub fn get_col_idx_with_qualifier(
        &self,
        qualifier: Option<&str>,
        column_name: &str,
    ) -> Result<usize, Error> {
        let mut found_idx = None;
        for (idx, col) in self.columns.iter().enumerate() {
            if col.name != column_name {
                continue;
            }
            // If qualifier is empty, it defaults to true; accepts any table.
            // If qualifier is given, it strictly compares against the col's table name.
            let qualifier_matches = qualifier.is_none_or(|q| col.table_name.as_deref() == Some(q));

            if qualifier_matches {
                // Checking for ambiguity. Duplicates aren't allowed as valid results.
                if found_idx.is_some() {
                    return Err(Error::SyntaxErr(format!(
                        "column reference '{}' is ambiguous, add a table name qualifier",
                        column_name
                    )));
                }
                found_idx = Some(idx);
            }
        }
        found_idx.ok_or_else(|| Error::ColumnNotFound(column_name.into()))
    }
}
