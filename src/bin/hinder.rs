use std::{env, ffi::OsStr, fs};

use anyhow::{Context, Result, bail};
use hinderdb::engine::{Database, ResultSet};
use rustyline::{DefaultEditor, error::ReadlineError};

const DEFAULT_DB_NAME: &str = "hinderdb_default";
const HISTORY_FILE_NAME: &str = ".hinderdb_history";

fn main() {
    println!("Welcome to hinderdb monitor.");
    println!("Commands ends with ;");
    println!("Type .help for instructions, .exit to quit.\n");

    let base_dir = match ensure_base_dir() {
        Ok(dir) => dir,
        Err(dir_err) => {
            return eprintln!("couldn't set up '.hinder_db' directory: {}", dir_err);
        }
    };

    let mut current_db = DEFAULT_DB_NAME.to_string();
    let mut db = match open_database(&base_dir, &current_db) {
        Ok(db) => db,
        Err(db_err) => {
            return eprintln!("{}", db_err);
        }
    };
    let mut conn = Some(db.connect());

    let history_path = format!("{}/{}", base_dir, HISTORY_FILE_NAME);
    let mut rl = DefaultEditor::new().expect("Failed to initialize rustyline");
    let _ = rl.load_history(&history_path);
    let mut query_buffer = String::new();

    loop {
        let prompt = if query_buffer.is_empty() {
            format!("hinder ({}) >> ", current_db)
        } else {
            "       ->".to_string()
        };

        let readline = rl.readline(&prompt);
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                // Handle meta commands.
                if query_buffer.is_empty() && line.starts_with('.') {
                    match line.to_uppercase().as_str() {
                        ".EXIT" | ".QUIT" => break,
                        ".HELP" => {
                            println!("Meta Commands:");
                            println!("To Exit the REPL   : .exit / .quit");
                            println!("Show this message  : .help");
                            println!();
                            println!("SQL Commands:");
                            println!("CREATE DATABASE <name>;");
                            println!("USE <db_name>;");
                            println!("SHOW DATABASES;");
                            println!();
                            println!("Inside a database instance:");
                            println!(" >> Standard SQL commands...");
                            continue;
                        }
                        _ => {
                            println!("Unrecognized command: {}. Type .help for help.", line);
                            continue;
                        }
                    }
                }

                query_buffer.push_str(line);
                query_buffer.push(' ');

                // Wait until the user terminates with a semicolon.
                if !query_buffer.trim().ends_with(';') {
                    continue;
                }

                let query = query_buffer.trim().to_string();
                let _ = rl.add_history_entry(query.as_str());
                query_buffer.clear();

                // Intercept file-per-database commands.
                let upper_query = query.to_uppercase();

                if upper_query.starts_with("CREATE DATABASE") {
                    if let Err(create_db_err) = handle_create_database(&base_dir, &upper_query) {
                        eprintln!("{}", create_db_err);
                    }
                    continue;
                } else if upper_query.starts_with("USE") {
                    match handle_use_database(&base_dir, &upper_query) {
                        Ok(new_db) => {
                            drop(db);

                            current_db = new_db;
                            db = match open_database(&base_dir, &current_db) {
                                Ok(db) => db,
                                Err(db_err) => {
                                    return eprintln!("{}", db_err);
                                }
                            };
                            // Re-establish the stateful session on the new database
                            conn = Some(db.connect());
                        }
                        Err(use_db_err) => eprintln!("{}", use_db_err),
                    }
                    continue;
                } else if upper_query.starts_with("SHOW DATABASES") {
                    match handle_show_database(&base_dir) {
                        Ok(list) => {
                            print_lists(list);
                        }
                        Err(err) => eprintln!("{}", err),
                    }
                    continue;
                }
                let start = std::time::Instant::now();

                // Safe to unwrap because we guarantee `conn` is `Some` unless we panicked during `USE`.
                match conn.as_mut().unwrap().execute_batch(&query) {
                    Ok(result_sets) => {
                        for result_set in result_sets {
                            print_result_set(result_set);
                        }
                        println!("({:.3} sec)", start.elapsed().as_secs_f64());
                    }
                    Err(exec_err) => {
                        eprintln!("Error: {:?}", exec_err);
                    }
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                eprintln!("Error reading input: {:?}", err);
                break;
            }
        }
    }
    let _ = rl.save_history(&history_path);
    println!("see you again!");
}

/// Resolves the base '.hinder_db' directory — inside the user's home
/// directory, falling back to the current directory if $HOME isn't set —
/// and makes sure it exists on disk. Runs once at startup so every later
/// database operation can assume the directory is already there.
fn ensure_base_dir() -> Result<String> {
    let home = env::var("HOME").unwrap_or_default();
    let base_dir = if home.is_empty() {
        ".hinder_db".to_string()
    } else {
        format!("{}/.hinder_db", home)
    };
    fs::create_dir_all(&base_dir).context("couldn't create base '.hinder_db' directory")?;
    Ok(base_dir)
}

/// Builds the `.db` / `.wal` file paths for a given database name, rooted at `base_dir`.
fn db_file_paths(base_dir: &str, name: &str) -> (String, String) {
    let db_path = format!("{}/{}.db", base_dir, name);
    let wal_path = format!("{}/{}.wal", base_dir, name);
    (db_path, wal_path)
}

/// Helper to instantiate a Database instance.
fn open_database(base_dir: &str, name: &str) -> Result<Database> {
    let (db_path, wal_path) = db_file_paths(base_dir, name);
    // 1000 pages = ~8MB Buffer Pool cache.
    match Database::open(db_path, wal_path, 1000) {
        Ok(database) => Ok(database),
        Err(db_err) => bail!("Failed to boot database! Some luck you have :). {}", db_err),
    }
}

/// Helper to collect the list of database names.
fn handle_show_database(base_dir: &str) -> Result<Vec<String>> {
    let mut databases = Vec::new();
    for entry in fs::read_dir(base_dir)? {
        let path = entry?.path();
        if path.is_file()
            && path.extension() == Some(OsStr::new("db"))
            && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
        {
            databases.push(stem.to_string());
        }
    }
    Ok(databases)
}

/// Prints the formatted list provided.
fn print_lists(list: Vec<String>) {
    let Some(width) = list.iter().map(|item| item.len()).max() else {
        println!("No elements in the list");
        return;
    };
    let print_separator = || {
        print!("+");
        print!("{}+", "-".repeat(width + 2));
        println!();
    };
    print_separator();
    for item in list {
        println!("| {:width$} |", item);
    }
    print_separator();
}

/// Intercepts and processes `CREATE DATABASE <name>`.
fn handle_create_database(base_dir: &str, query: &str) -> Result<()> {
    let parts: Vec<&str> = query.trim_end_matches(';').split_whitespace().collect();
    if parts.len() != 3 {
        bail!("Syntax Error: Expected CREATE DATABASE <name>;");
    }
    let db_name = parts[2];
    let (db_path, wal_path) = db_file_paths(base_dir, db_name);

    if fs::metadata(&db_path).is_ok() {
        bail!("Error: Database '{}' already exists", db_name);
    }
    fs::File::create(&db_path)?;
    fs::File::create(&wal_path)?;
    println!("Database '{}' created successfully", db_name);
    Ok(())
}

/// Intercepts and processes `USE <name>`.
fn handle_use_database(base_dir: &str, query: &str) -> Result<String> {
    let parts: Vec<&str> = query.trim_end_matches(';').split_whitespace().collect();
    if parts.len() != 2 {
        bail!("Syntax Error: Expected USE <name>;");
    }
    let db_name = parts[1];
    let (db_path, _wal_path) = db_file_paths(base_dir, db_name);

    if fs::metadata(&db_path).is_err() {
        bail!("Error: Database '{}' does not exist", db_name);
    }
    println!("Switched to database '{}'.", db_name);
    Ok(db_name.to_string())
}

/// Dynamically formats and prints the ResultSet as an ASCII table.
fn print_result_set(result: ResultSet) {
    match result {
        ResultSet::Mutation { rows_affected } => {
            println!("Query OK, {} rows affected", rows_affected);
        }
        ResultSet::Query { rows, schema } => {
            if rows.is_empty() {
                println!("Empty set");
                return;
            }
            let mut col_width: Vec<usize> =
                schema.columns.iter().map(|col| col.name.len()).collect();

            let mut formatted_rows = Vec::with_capacity(rows.len());

            for row in &rows {
                let mut string_row = Vec::with_capacity(row.values.len());
                for (i, val) in row.values.iter().enumerate() {
                    let str_val = val.to_string();

                    if str_val.len() > col_width[i] {
                        col_width[i] = str_val.len();
                    }
                    string_row.push(str_val);
                }
                formatted_rows.push(string_row);
            }
            // Helper closure to draw horizontal separators.
            let print_separator = || {
                print!("+");
                for width in &col_width {
                    print!("{}+", "-".repeat(*width + 2));
                }
                println!();
            };
            // Start printing the table.
            print_separator();

            // Print header
            print!("|");
            for (i, col) in schema.columns.iter().enumerate() {
                print!(" {:width$} |", col.name, width = col_width[i]);
            }
            println!();
            print_separator();

            // Print rows
            for row in formatted_rows {
                print!("|");
                for (i, val_str) in row.iter().enumerate() {
                    print!(" {:width$} |", val_str, width = col_width[i]);
                }
                println!();
            }
            print_separator();
            println!("{} rows in set", rows.len());
        }
    }
}
