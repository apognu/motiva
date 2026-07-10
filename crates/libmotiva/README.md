# Motiva

From the Greek _μοτίβα_, meaning _patterns_, or the recognization of similar features between objects.

[![Crates.io](https://img.shields.io/crates/v/libmotiva)](https://crates.io/crates/libmotiva)
[![Documentation](https://docs.rs/libmotiva/badge.svg)](https://docs.rs/libmotiva)
[![Coverage](https://coveralls.io/repos/github/apognu/motiva/badge.svg?branch=main)](https://coveralls.io/github/apognu/motiva?branch=main)

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

#### Yente version compatibility

Before v0.5.0, motiva is only compatible with data indexer with Yente v4.x. Starting with v0.5.0, it will try to determine, at startup, which version of Yente was used to index the data (v4.x or v5.x), and adapt its queries to support it.

## Configuration

Motiva is configured via environment variables. The following variables are supported:

| Variable                   | Description                                                                            | Default / Example         |
| -------------------------- | -------------------------------------------------------------------------------------- | ------------------------- |
| `ENV`                      | Environment (`dev` or `production`)                                                    | `dev`                     |
| `LISTEN_ADDR`              | Address to bind the API server                                                         | `0.0.0.0:8000`            |
| `API_KEY`                  | Bearer token used to authenticate requests                                             | _(none)_                  |
| `INDEX_URL`                | Elasticsearch URL                                                                      | `http://localhost:9200`   |
| `INDEX_AUTH_METHOD`        | Elasticsearch authentication (`none`, `basic`, `bearer`, `api_key`, `encoded_api_key`) | `none`                    |
| `INDEX_CLIENT_ID`          | Elasticsearch client ID (required for `basic` or `api_key`)                            | _(none)_                  |
| `INDEX_CLIENT_SECRET`      | Elasticsearch client secret (required for `basic`, `api_key` or `encoded_api_key`)     | _(none)_                  |
| `INDEX_TLS_CA_CERT`        | Path to a PEM-encoded certificate chain to use for TLS validation                      | _(none)_                  |
| `INDEX_TLS_SKIP_VERIFY`    | If `1`, do not validate the TLS certificate served by the Elasticsearch cluster        | `0`                       |
| `INDEX_NAME`               | Index prefix under which data was indexed (suffixed by `-entities`)                    | `yente`                   |
| `MANIFEST_URL`             | Optional URL to a custom manifest JSON file                                            | _(none)_                  |
| `CATALOG_REFRESH_INTERVAL` | Interval at which to pull the manifest and catalogs                                    | _1h_                      |
| `MATCH_CANDIDATES`         | Number of candidates to consider for matching                                          | `10`                      |
| `WEIGHT_<FEATURE_NAME>`    | Custom weight for a given feature (e.g. `WEIGHT_PERSON_NAME_JARO_WINKLER`)             | _(none)_                  |
| `ENRICHMENT_MAX_RECURSION` | Maximum recursion levels when enriching entities with relations                        | `2`                       |
| `ENRICHMENT_QUERY_LIMIT`   | Maximum relation documents to fetch from Elasticsearch when building relation graphs   | `200`                     |
| `ENABLE_PROMETHEUS`        | Enable Prometheus metrics collection and /metrics endpoint                             | `0`                       |
| `ENABLE_TRACING`           | Set to `1` to enable tracing                                                           | _(none)_                  |
| `TRACING_EXPORTER`         | Tracing exporter kind (`otlp`, or `gcp` if compiled with the `gcp` feature)            | `otlp`                    |
| `REQUEST_TIMEOUT`          | Maximum duration for a match request                                                   | _10s_                     |
| `SCOPED_INDEX_QUERY`       | Query used to scope down the index used for match queries                              | [see here](#scoped-index) |

Setting `MANIFEST_FILE` is required if you use a customized dataset list and would like your own manifest to be used for catalog generation. If omitted, the default manifest provided by Yente will be used. It requires either an HTTP URL or a local file path ending in `.json`, `.yml` or `.yaml`.

## Motiva-specific features

### Scope-partitioned queries

In those cases where the requested scope exactly matches an indexed scope, you can pass `?partition=true` to your query to add a filter on that particular index prefix instead of only using a post-scan datasets filter. This has the potential to greatly increase performance when the data distribution between your indexes is highly imbalanced.

**Note:** this would not work if your query scope does not match your indexed scope. For example, if you index part of the data (with `"scope": "us_sanctions"` for example), but still query it with `/match/default`, no results would ever be returned where it would have without partitioning.

### Query options passed in body

Some unbounded-in-size query parameters can be passed in the request body instead of through the URL query. This prevents, for some of them taking in unbounded lists, to overflow the maximum length of URLs. Namely, you can now pass the following parameters in the body:

- `include_dataset`
- `exclude_dataset`
- `exclude_entity_ids`

The match endpoint body now takes a `params` object at its root:

```json
{
  "queries": [...],
  "params": {
    "include_datasets": [...],
    "exclude_datasets": [...],
    "exclude_entity_ids": [...]
  }
}
```

Also, the `include_datasets` and `exclude_datasets` parameters can be overriden per-query with a nested `params` field, alongside the other search entity fields:

```json
{
  "queries": {
    "first": {
      "schema": "Person",
      "properties": {...},
      "params": {
        "include_datasets": ["one", "two"]
      }
    }
  }
}
```

### Advanced boolean filters

If you need to add advanced boolean logic to your search on `keyword` fields, you can add a `filters` field to your queries. Those take, for each attribute, an array of arrays of strings.

The inner arrays are merged with a boolean `OR`, whereas the outer one and `AND`ed.

```json
{
  "queries": {
    "first": {
      "schema": "Person",
      "properties": {
        "name": ["..."]
      },
      "filters": {
        "topics": [
          ["wanted", "crime"],
          ["role.pol"]
        ],
        "properties.citizenship": [
          ["ru"]
        ]
      }
    }
  }
}
```

This query will perform the usual matching in Elasticsearch, but only return those entities which:

 - Have either the `wanted` **OR** `crime` topic
 - **AND** have the `role.pol` topic
 - **AND** have the `ru` citizenship

### Scoped index

Motiva supports generating and using a trimmed down index for match queries, while keeping the full index for entity relation queries. This could allow improving performance of match queries if you are only interested in a subset of it, while keeping the full datasets for queries that are less time-sensitive.

For example, you could have a search index that only contains `Person`'s that have `sanction` in their `topics`, while keeping the full index to retrieve details of an entity, enriched with all its relations. Depending on the query you use for the scoped index, you could see a great reduction in latency and resource consumption.

Motiva can be run with the `create-scoped-index` subcommand, which will take care of creating the scoped index and its aliases. Once it is done, restarting motiva will make it effective.

```bash
$ motiva create-scoped-index
2026-03-05T16:56:14.439865Z  INFO libmotiva::index::elastic::scoped: found previous scoped index index="motiva-w4xgo6jh"
2026-03-05T16:56:14.546981Z  INFO libmotiva::index::elastic::scoped: created new index, starting reindexing data index="motiva-9xtyeclx"
2026-03-05T16:56:24.030717Z  INFO libmotiva::index::elastic::scoped: reindexed data index="motiva-9xtyeclx"
2026-03-05T16:56:24.041981Z  INFO libmotiva::index::elastic::scoped: atomically swapped index from="motiva-w4xgo6jh" to="motiva-9xtyeclx"
2026-03-05T16:56:24.071765Z  INFO libmotiva::index::elastic::scoped: deleted old index index="motiva-w4xgo6jh"
```

The default scoped query is listed below, but can be customized through `SCOPED_INDEX_QUERY`.

```json
{
  "bool": {
    "must": [
      {
        "terms": {
          "schema": [
            "Person",
            "LegalEntity",
            "Organization",
            "Company",
            "Airplane",
            "Vessel"
          ]
        }
      },
      { "term": { "topics": "sanction" } }
    ]
  }
}
```

The scoped index is not kept automatically in sync with the full index, you would need to run `motiva create-scoped-index` again when you need to update it. We suggest running it after your regular indexing operations.

Once your scoped index is created, you can perform a `/match` request with the Motiva-specific `?index_type=scoped` parameters for the new index to be used.

## Run

```sh
$ cargo run --release
$ echo '{"queries":{"test":{"schema":"Person","properties":{"name":["Vladimir Putin"]}}}}' | curl -XPOST 127.0.0.1:8080/match/sanctions -H content-type:application/json -d @-
```

## Development

### Building

```bash
$ git clone --recurse-submodules git@github.com:apognu/motiva.git
$ cd motiva
```

## Building

```bash
# Standard build
$ cargo build
# Build with libicu support (requires libicu-dev)
$ cargo build --release --features icu
# Build with GCP tracing support
$ cargo build --release --features gcp
```

### Docker

Pre-built images are available in this repositor's packages section, at `ghcr.io/apognu/motiva`, for each combination of features. Alternatively, you can build the image thus:

```bash
# Build without libicu
$ docker build -t motiva .
# Build without standalone features
$ docker build --build-arg CARGO_ARGS="--features gcp" -t motiva:gcp .
# Build with libicu support
$ docker build --build-arg BASE=icu --build-arg CARGO_ARGS="--features icu" -t motiva:icu .
```

### Test suite

To run the tests, a Python 3.13+ environment must be set up with the required dependencies (this include `libicu`). You can install it in a virtualenv by using the `uv` file at the root of this repository:

```sh
$ uv sync
$ cargo test
```

One quite lengthy test is ignored by default (scoring the cartesian product of 50x50 entities against each other) and compare it against nomenklatura. You can still run this test by running `cargo test -- --include-ignored`.

### Contributing

Motiva is a work in progress.

Contributions and feedback are welcome! Please familiarize yourself with the [`CONTRIBUTING.md`](./CONTRIBUTING.md) guidelines beforehand.
