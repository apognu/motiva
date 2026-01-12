# Motiva

From the Greek _μοτίβα_, meaning _patterns_, or the recognization of similar features between objects.

[![Crates.io](https://img.shields.io/crates/v/libmotiva)](https://crates.io/crates/libmotiva)
[![Documentation](https://docs.rs/libmotiva/badge.svg)](https://docs.rs/libmotiva)
[![Coverage](https://codecov.io/gh/apognu/motiva/graph/badge.svg?token=YVR2HP5E2U)](https://codecov.io/gh/apognu/motiva)

This is a scoped-down reimplementation of [Yente](https://github.com/opensanctions/yente) and [nomenklatura](https://github.com/opensanctions/nomenklatura), used to match entities against sanctions lists.

Most of the algorithms are taken directly from those repositories, and simply reimplemented, and the credit should go to the Open Sanctions's team.

Note that this piece of software requires Yente to run beside it, including Elasticsearch and a valid, licensed, collection of dataset obtained from [Open Sanctions](https://www.opensanctions.org/licensing/).

**Work in progress**

## Scope and goals

Not all of Yente is going to be implemented here. Notably, none of the index updates feature are going to their way into this repository. We will focus on the request part (search and matching).

Even through we will strive to produce matching scores in the vicinity of those of Yente, exact scores are not a goal. In particular, the Rust implementations of some algorithms will produce slightly different results, resulting in different overall scores. This is, for example, the case of the algorithm transliterating scripts into latin, which do not use `libicu` by default, and might therefore produce slightly different results <sup>[1]</sup>.

All implemented algorithms will feature an integration test comparing Motiva's score with Yente's and check they are within a _reasonable_ epsilon of each other.

If at all possible, this project will try to use only Rust-native dependencies, and stay clear of integrating with C libraries through FFI <sup>[2]</sup>.

Some liberty was taken to adapt some logic and algorithms from Yente, so do not expect fully-compliant API or behavior.

<sup>[1]</sup>: Motiva can be compiled with the `icu` feature to use the same transliteration library as yente. This will require `libicu` development headers and shared libraries.

<sup>[2]</sup>: With the default features configuration.

### Implementation matrix

- [x] POST /match/{dataset}
- [x] GET /entities/{id}
- [x] GET /algorithms
- [x] GET /catalog
- [x] name-based
- [x] name-qualified
- [x] logic-v1 <sup>[1]</sup>
- [ ] logic-v2

<sup>[1]</sup>: Features that are disabled by default were omited for now.

## Configuration

Motiva is configured via environment variables. The following variables are supported:

| Variable                   | Description                                                                            | Default / Example       |
| -------------------------- | -------------------------------------------------------------------------------------- | ----------------------- |
| `ENV`                      | Environment (`dev` or `production`)                                                    | `dev`                   |
| `LISTEN_ADDR`              | Address to bind the API server                                                         | `0.0.0.0:8000`          |
| `API_KEY`                  | Bearer token used to authenticate requests                                             | _(none)_                |
| `INDEX_URL`                | Elasticsearch URL                                                                      | `http://localhost:9200` |
| `INDEX_AUTH_METHOD`        | Elasticsearch authentication (`none`, `basic`, `bearer`, `api_key`, `encoded_api_key`) | `none`                  |
| `INDEX_CLIENT_ID`          | Elasticsearch client ID (required for `basic` or `api_key`)                            | _(none)_                |
| `INDEX_CLIENT_SECRET`      | Elasticsearch client secret (required for `basic`, `api_key` or `encoded_api_key`)     | _(none)_                |
| `MANIFEST_URL`             | Optional URL to a custom manifest JSON file                                            | _(none)_                |
| `CATALOG_REFRESH_INTERVAL` | Interval at which to pull the manifest and catalogs                                    | _1h_                    |
| `MATCH_CANDIDATES`         | Number of candidates to consider for matching                                          | `10`                    |
| `ENABLE_PROMETHEUS`        | Enable Prometheus metrics collection and /metrics endpoint                             | `0`                     |
| `ENABLE_TRACING`           | Set to `1` to enable tracing                                                           | _(none)_                |
| `TRACING_EXPORTER`         | Tracing exporter kind (`otlp`, or `gcp` if compiled with the `gcp` feature)            | `otlp`                  |

Setting `MANIFEST_FILE` is required if you use a customized dataset list and would like your own manifest to be used for catalog generation. If omitted, the default manifest provided by Yente will be used. It requires either an HTTP URL or a local file path ending in `.json`, `.yml` or `.yaml`.

## Run

Right now, there are _no configuration_ possible on this project, and it will remain that way until it is in a good enough shape to be used widely.

```sh
$ cargo run --release
$ echo '{"queries":{"test":{"schema":"Person","properties":{"name":["Vladimir Putin"]}}}}' | curl -XPOST 127.0.0.1:8080/match/sanctions -H content-type:application/json -d @-
```

## Development

### Test suite

To run the tests, a Python environment must be set up with the required dependencies (this include `libicu`). You can install it in a virtualenv by using the Poetry file at the root of this repository and (manually) setting the `PYTHONPATH`:

```sh
$ poetry install
$ export PYTHONPATH="$(pwd).venv/lib/python3.13/site-packages"
$ cargo test
```

One quite lengthy test is ignored by default (scoring the cartesian product of 50x50 entities against each other) and compare it against nomenklatura. You can still run this test by running `cargo test -- --include-ignored`.

### Contributing

Motiva is a work in progress. Contributions and feedback are welcome!
