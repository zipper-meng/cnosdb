#![recursion_limit = "256"]
#![allow(clippy::result_large_err)] // TODO(zipper: fix large enums)
#![allow(clippy::large_enum_variant)] // TODO(zipper: fix large enums)

extern crate core;

pub mod auth;
pub mod data_source;
pub mod dispatcher;
mod execution;
pub mod extension;
pub mod function;
pub mod instance;
pub mod metadata;
pub mod prom;
pub mod sql;
pub mod stream;
mod utils;
pub mod variable;
