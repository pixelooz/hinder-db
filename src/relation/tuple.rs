use std::io::{Read, Write};

use crate::{
    error::Error,
    relation::{
        schema::Schema,
        types::{DataType, Value},
    },
};

/// The in-memory representation of a single database row.
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple {
    /// The values/columns of a table's row.
    pub values: Vec<Value>,

    /// The row identifier, also could be the primary key if configured.
    /// The encoder/decoder ignore this field cause its technically not
    /// part of tuple. This is only so the executors can have row_id
    /// when needed.
    pub row_id: Option<u64>,
}

impl Tuple {
    /// Constructs an initialized Tuple with the provided `values`.
    pub fn new(values: Vec<Value>) -> Self {
        Self { values, row_id: None }
    }

    /// Extracts a reference to a specific column's value by name, or an error
    /// if the column does not exist in the provided schema.
    pub fn get_value(&self, schema: &Schema, col_name: &str) -> Result<&Value, Error> {
        let idx = schema.get_col_idx(col_name)?;
        Ok(&self.values[idx])
    }
}

impl Tuple {
    /// Encodes the tuple in little-endian binary format and writes them into
    /// the provided writer.
    pub fn encode<W: Write>(&self, schema: &Schema, writer: &mut W) -> Result<(), Error> {
        // Why they must match is because schema is the high level view and
        // tuple is the low level view of the table data.
        // name | age | address
        // var..| int | varchar
        if self.values.len() != schema.columns.len() {
            return Err(Error::CorruptPage(
                "tuple value count does not match schema column count".into(),
            ));
        }
        for (i, col) in schema.columns.iter().enumerate() {
            let value = &self.values[i];
            let is_null = matches!(value, Value::Null);

            writer
                .write_all(&[is_null as u8])
                .map_err(Error::Io)?;

            if !is_null {
                match (col.data_type, value) {
                    (DataType::BigInt, Value::BigInt(v)) => {
                        writer
                            .write_all(&v.to_le_bytes())
                            .map_err(Error::Io)?;
                    }
                    (DataType::Int, Value::Int(v)) => {
                        writer
                            .write_all(&v.to_le_bytes())
                            .map_err(Error::Io)?;
                    }
                    (DataType::Boolean, Value::Boolean(v)) => {
                        writer.write_all(&[*v as u8]).map_err(Error::Io)?;
                    }
                    (DataType::Varchar, Value::Varchar(v)) => {
                        let length = v.len() as u32;
                        writer
                            .write_all(&length.to_le_bytes())
                            .map_err(Error::Io)?;
                        writer
                            .write_all(v.as_bytes())
                            .map_err(Error::Io)?;
                    }
                    _ => {
                        return Err(Error::CorruptPage(format!(
                            "value type {:?} does not match schema type {:?} definition",
                            value, col.data_type
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Decodes a `Tuple` from little-endian bytes using the provided schema
    /// blueprint.
    pub fn decode<R: Read>(schema: &Schema, reader: &mut R) -> Result<Self, Error> {
        let mut values = Vec::with_capacity(schema.columns.len());
        let mut is_null = [0u8; 1];

        for col in &schema.columns {
            reader
                .read_exact(&mut is_null)
                .map_err(Error::Io)?;

            if is_null[0] == 0 {
                let value = match col.data_type {
                    DataType::BigInt => {
                        let mut buffer = [0u8; 8];

                        reader
                            .read_exact(&mut buffer)
                            .map_err(Error::Io)?;
                        Value::BigInt(i64::from_le_bytes(buffer))
                    }
                    DataType::Int => {
                        let mut buffer = [0u8; 4];

                        reader
                            .read_exact(&mut buffer)
                            .map_err(Error::Io)?;
                        Value::Int(i32::from_le_bytes(buffer))
                    }
                    DataType::Boolean => {
                        let mut buffer = [0u8; 1];

                        reader
                            .read_exact(&mut buffer)
                            .map_err(Error::Io)?;

                        Value::Boolean(buffer[0] == 1)
                    }
                    DataType::Varchar => {
                        let mut len_buf = [0u8; 4];
                        reader
                            .read_exact(&mut len_buf)
                            .map_err(Error::Io)?;

                        let str_len = u32::from_le_bytes(len_buf) as usize;
                        let mut buffer = vec![0u8; str_len];

                        reader
                            .read_exact(&mut buffer)
                            .map_err(Error::Io)?;

                        let parsed_str = String::from_utf8(buffer)
                            .map_err(|err| Error::CorruptPage(format!("invalid utf-8: {}", err)))?;
                        Value::Varchar(parsed_str)
                    }
                };
                values.push(value);
                continue;
            }
            values.push(Value::Null);
        }
        Ok(Self { values, row_id: None })
    }
}

#[cfg(test)]
mod tests {
    use std::{error, io::Cursor};

    use crate::{
        error::Error,
        relation::{
            schema::{Column, Schema},
            tuple::Tuple,
            types::{DataType, Value},
        },
    };

    /// Creates a dummy schema with the provided collection.
    fn mock_schema(types: Vec<(&str, DataType, Option<u32>)>) -> Schema {
        Schema {
            columns: types
                .into_iter()
                .map(|x| Column::new(None, x.0, x.1, x.2))
                .collect(),
        }
    }

    /// Validates a complete round-trip for every supported DataType.
    #[test]
    fn test_tuple_roundtrip_all_types() -> Result<(), Box<dyn error::Error>> {
        let schema = mock_schema(vec![
            ("bigint", DataType::BigInt, None),
            ("int", DataType::Int, None),
            ("boolean", DataType::Boolean, None),
            ("varchar", DataType::Varchar, Some(255)),
        ]);
        let original_tuple = Tuple::new(vec![
            Value::BigInt(123456789012345),
            Value::Int(-32),
            Value::Boolean(true),
            Value::Varchar("pellet-db".to_string()),
        ]);
        let mut buffer = Cursor::new(Vec::new());
        original_tuple.encode(&schema, &mut buffer)?;

        dbg!(&buffer);

        buffer.set_position(0);

        let decoded_tuple = Tuple::decode(&schema, &mut buffer)?;
        assert_eq!(original_tuple, decoded_tuple);
        Ok(())
    }

    /// Verifies the 1-byte null indicator logic.
    #[test]
    fn test_tuple_roundtrip_with_nulls() -> Result<(), Box<dyn error::Error>> {
        let schema = mock_schema(vec![
            ("bigint", DataType::BigInt, None),
            ("int", DataType::Int, None),
            ("boolean", DataType::Boolean, None),
            ("varchar", DataType::Varchar, Some(255)),
        ]);
        let original_tuple = Tuple::new(vec![
            Value::BigInt(123456789012345),
            Value::Int(100),
            Value::Null,
            Value::Varchar("pellet-db".to_string()),
        ]);
        let mut buffer = Cursor::new(Vec::new());
        original_tuple.encode(&schema, &mut buffer)?;

        dbg!(&buffer);

        buffer.set_position(0);

        let decoded_tuple = Tuple::decode(&schema, &mut buffer)?;
        assert_eq!(original_tuple, decoded_tuple);
        assert!(matches!(decoded_tuple.values[2], Value::Null));

        Ok(())
    }

    /// Ensures that multibyte Unicode characters (e.g., emojis) are accurately written
    /// and read based on their byte length, not their character count.
    #[test]
    fn test_varchar_multibyte_unicode() -> Result<(), Box<dyn std::error::Error>> {
        let schema = mock_schema(vec![("varchar", DataType::Varchar, Some(255))]);
        let original_tuple = Tuple::new(vec![Value::Varchar("🦀 database 🚀".to_string())]);

        let mut buffer = Cursor::new(Vec::new());
        original_tuple.encode(&schema, &mut buffer)?;

        dbg!(&buffer);

        buffer.set_position(0);
        let decoded_tuple = Tuple::decode(&schema, &mut buffer)?;

        assert_eq!(original_tuple, decoded_tuple);
        Ok(())
    }

    /// If schema and value do not match, encode should return error.
    #[test]
    fn test_encode_schema_count_mismatch() {
        let schema = mock_schema(vec![
            ("int", DataType::Int, None),
            ("int", DataType::Int, None),
        ]);
        let original_tuple = Tuple::new(vec![Value::Int(400)]);

        let mut buffer = Cursor::new(Vec::new());
        let result = original_tuple.encode(&schema, &mut buffer);

        assert!(matches!(result, Err(Error::CorruptPage(_))));
    }

    /// A Tuple providing the wrong Value variant  for a Schema's DataType must be caught
    /// and rejected.
    #[test]
    fn test_encode_type_mismatch() {
        let schema = mock_schema(vec![("int", DataType::Int, None)]);
        // Schema wants Int, Tuple provides Varchar
        let tuple = Tuple::new(vec![Value::Varchar("wrong".to_string())]);

        let mut buffer = Cursor::new(Vec::new());
        let result = tuple.encode(&schema, &mut buffer);

        assert!(matches!(result, Err(Error::CorruptPage(_))));
    }

    /// Verifies that decoding stops and surfaces an IO error if the underlying buffer
    /// is abruptly truncated (e.g., corrupted disk sector or partial page read).
    #[test]
    fn test_decode_unexpected_eof() -> Result<(), Box<dyn std::error::Error>> {
        let schema = mock_schema(vec![("bigint", DataType::BigInt, None)]);
        let tuple = Tuple::new(vec![Value::BigInt(9999)]);

        let mut buffer = Cursor::new(Vec::new());
        tuple.encode(&schema, &mut buffer)?;

        // Truncate the buffer artificially to simulate corruption
        let mut corrupted_bytes = buffer.into_inner();
        corrupted_bytes.truncate(4); // BigInt requires 8 bytes + 1 null byte

        let mut read_buffer = Cursor::new(corrupted_bytes);
        let result = Tuple::decode(&schema, &mut read_buffer);

        assert!(matches!(result, Err(Error::Io(_))));
        Ok(())
    }

    /// Ensures that if a string payload is corrupted with invalid UTF-8 bytes,
    /// the decoder safely returns a CorruptPage error rather than panicking.
    #[test]
    fn test_decode_invalid_utf8_varchar() {
        let schema = mock_schema(vec![("bigint", DataType::Varchar, None)]);

        let mut buffer = Vec::new();
        // 1. Write Not-Null indicator
        buffer.push(0u8);
        // 2. Write String length (4 bytes)
        buffer.extend_from_slice(&4u32.to_le_bytes());
        // 3. Write invalid UTF-8 sequence
        buffer.extend_from_slice(&[0, 159, 146, 150]);

        let mut read_buffer = Cursor::new(buffer);
        let result = Tuple::decode(&schema, &mut read_buffer);

        assert!(matches!(result, Err(Error::CorruptPage(_))));
    }
}
