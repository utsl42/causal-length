//! CRDT's based on ["A Low-Cost Set CRDT Based on Causal Lengths"](https://dl.acm.org/doi/pdf/10.1145/3380787.3393678)
//! combined with an optional tag. The tag can be any type that satisfies `Ord + Copy`. A simple
//! integer, wall clock, lamport timestamp, or even a hybrid logical clock from ["Logical Physical Clocks and Consistent Snapshots
//! in Globally Distributed Databases"](http://www.cse.buffalo.edu/tech-reports/2014-04.pdf) may
//! be used.

use num_integer::Integer;
use num_traits::One;

pub mod map;
pub mod set;

pub trait CausalLength: Integer + One + Ord + Copy {}
impl<T> CausalLength for T where T: Integer + One + Ord + Copy {}
