//! CRDT's based on ["A Low-Cost Set CRDT Based on Causal Lengths"](https://dl.acm.org/doi/pdf/10.1145/3380787.3393678)
//! combined with an optional tag. The tag can be any type that satisfies `Ord + Copy`. A simple
//! integer, wall clock, lamport timestamp, or even a hybrid logical clock from ["Logical Physical Clocks and Consistent Snapshots
//! in Globally Distributed Databases"](http://www.cse.buffalo.edu/tech-reports/2014-04.pdf) may
//! be used.

use num_integer::Integer;
use num_traits::One;
use std::hash::Hash;

/// Causal length Map
pub mod map;
pub use self::map::*;
/// Causal length Register
pub mod register;
pub use self::register::*;

/// Causal length Set
pub mod set;
pub use self::set::*;

/// CausalLength is abstracted to allow any of Rust's integer types to be used.
pub trait CausalLength: Integer + One + Ord + Copy + Eq {}
impl<T> CausalLength for T where T: Integer + One + Ord + Copy + Eq {}

/// Key type used in the CRDTs
pub trait Key: Eq + Hash + Clone {}
impl<T> Key for T where T: Eq + Hash + Clone {}

/// Value type used in the CRDTs
pub trait Value: Clone + Eq {}
impl<T> Value for T where T: Clone + Eq {}

/// Tag type used in the CRDTs
pub trait TagT: Eq + Ord + Copy + Default {}
impl<T> TagT for T where T: Eq + Ord + Copy + Default {}
