CRDT's based on ["A Low-Cost Set CRDT Based on Causal Lengths"](https://dl.acm.org/doi/pdf/10.1145/3380787.3393678)
combined with an optional tag. The tag can be any type that satisfies [TagT]. A simple
integer, wall clock, lamport timestamp, or even a hybrid logical clock from ["Logical Physical Clocks and Consistent Snapshots
in Globally Distributed Databases"](http://www.cse.buffalo.edu/tech-reports/2014-04.pdf) may
be used.

# Distinctive features of this group of CRDTs
- No need for a pre-shared, globally unique identifier
- While it can use a clock, it is not last-write-wins, and does not require one
- Updates can be merged out of order or redundantly, so it appears to be a [Delta CRDT](https://arxiv.org/abs/1603.01529)

# Status
- Set - Fairly solid, based directly on the paper
- Map - Based on Set, but not in the paper. Could be described as most updated wins. New and not super well tested.
- Register - Can be regarded as either a single set member (therefore tied to the paper), a delta for either
  Set or Map, or a CRDT equivalent to Option.

