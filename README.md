# Motiva

From the Greek _μοτίβα_, meaning _patterns_, or the recognization of similar features between objects.

This is a scoped-down reimplementation of [Yente](https://github.com/opensanctions/yente) and [nomenklatura](https://github.com/opensanctions/nomenklatura), used to match entities against sanctions lists.

Most of the algorithms are taken directly from those repositories, and simply reimplemented, and the credit should go to the OpenSanctions's team.

**Work in progress**

## Scope and goals

Not all of Yente is going to be implemented here. Notably, none of the index updates feature are going to their way into this repository. We will focus on the request part (search and matching).

Even through we will strive to produce matching scores in the vicinity of those of Yente, exact scores are not a goal. In particular, the Rust implementations of some algorithms will produce slightly different results, resulting in different overall scores.

All implemented algorithm will feature an integration test comparing Motiva's score with Yente's and check they are within a _reasonable_ epsilon of each other.

If at all possible, this project will try to use only Rust-native dependencies, and stay clear of integrating with C libraries through FFI.

## Run

Right now, there are _no configuration_ possible on this project, and it will remain that way until it is in a good enough shape to be used widely.

```
$ cargo run --release
$ echo '{"queries":{"test":{"schema":"Person","properties":{"name":["Vladimir Putin"]}}}}' | curl -XPOST 127.0.0.1:8080/match/sanctions -H content-type:application/json -d @-
```

To run the tests, a Python environment must be set up with the required dependencies (this include `libicu`). You can install it in a virtualenv by using the Poetry file at the root of this repository and (manually) setting the `PYTHONPATH`:

```
$ poetry install
$ export PYTHONPATH='.venv/lib/python3.13/site-packages'
$ cargo test
```

This is a bit convoluted, but it will test motiva's scoring against nomeklatura's.
