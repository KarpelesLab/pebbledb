# pebbledb fuzz targets

Coverage-guided [libFuzzer](https://llvm.org/docs/LibFuzzer.html) targets for pebbledb's
on-disk decoders, driven by [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz).

This is a **standalone crate** (its own `[workspace]`) that deliberately lives outside the
main `pebbledb` crate: it requires the nightly toolchain and the libFuzzer runtime, neither of
which the single-crate, MSRV-1.88 main crate depends on. The parent crate's `cargo build`,
`cargo test`, `cargo clippy`, `cargo doc`, and MSRV check never descend into `fuzz/`.

The stable-toolchain, single-crate analogue of these targets is
[`tests/fuzz_decoders.rs`](../tests/fuzz_decoders.rs), which drives the same entry points under
a deterministic in-crate PRNG and runs as part of the normal `cargo test` suite.

## Setup

```sh
rustup toolchain install nightly
cargo install cargo-fuzz
```

## Targets

| target           | entry point                                  |
| ---------------- | -------------------------------------------- |
| `batch_decode`   | `batch::Batch::from_bytes` + op iteration    |
| `record_log`     | `record::Reader::read_record` (WAL/MANIFEST) |
| `version_edit`   | `manifest::VersionEdit::decode`              |
| `sstable_reader` | `sstable::Reader::open` + table iteration    |

Each target asserts the decoder never panics, hangs, or reads out of bounds on arbitrary
input — only `Ok`/`Err`/clean EOF.

## Run

```sh
cargo +nightly fuzz run batch_decode
cargo +nightly fuzz run sstable_reader -- -max_total_time=60
cargo +nightly fuzz list          # show all targets
```

Crashes are written to `fuzz/artifacts/<target>/`; reproduce with
`cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<crash-file>`.
