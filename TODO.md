# TODO

## MapBuilder: use typed raw builder internally

`MapBuilder<'bump, W, V>` currently wraps `raw::Builder<'bump, W>` (always u64)
and converts V↔u64 at the API boundary. This means using `Map<D, u32>` saves no
memory during construction — `Transition<u64>` (24 bytes) is allocated internally
instead of `Transition<u32>` (16 bytes).

Fix: make `MapBuilder<'bump, W, V>` wrap `raw::Builder<'bump, W, V>` so the
smaller output type flows through to the internal data structures. This gives a
33% reduction in per-transition memory during construction for any V smaller than
u64, which matters when building FSTs with millions of keys.

## Update doc comments

`src/lib.rs` line ~294 still says "values are limited to unsigned 64 bit
integers". Update to reflect that `u8`, `u16`, `u32`, and `u64` are now
supported via the `FstOutput` trait.
