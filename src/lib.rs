#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub mod canonical;
pub mod coalesce;
pub mod db;
pub mod error;
pub mod proxy;
pub mod vector;
