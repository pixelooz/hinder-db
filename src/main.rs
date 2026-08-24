#![allow(dead_code)]

mod catalog;
mod engine;
mod error;
mod execution;
mod manager;
mod planner;
mod relation;
mod sql;
mod storage;

fn main() {
    let boolean = false as u8;
    println!("bool = {}", boolean);
}
