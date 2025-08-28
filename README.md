# Motiva

From the Greek _μοτίβα_, meaning _patterns_, or the recognization of similar features between objects.

This is a scoped-down reimplementation of [Yente](https://github.com/opensanctions/yente) and [nomenklatura](https://github.com/opensanctions/nomenklatura), used to match entities against sanctions lists.

Most of the algorithms are taken directly from those repositories, and simply reimplemented, and the credit should go to the Open Sanctions's team.

Note that this piece of software requires Yente to run beside it, including Elasticsearch and a valid, licensed, collection of dataset obtained from [Open Sanctions](https://www.opensanctions.org/licensing/).

**Work in progress**

## Scope and goals

Not all of Yente is going to be implemented here. Notably, none of the index updates feature are going to their way into this repository. We will focus on the request part (search and matching).

Even through we will strive to produce matching scores in the vicinity of those of Yente, exact scores are not a goal. In particular, the Rust implementations of some algorithms will produce slightly different results, resulting in different overall scores.

All implemented algorithms will feature an integration test comparing Motiva's score with Yente's and check they are within a _reasonable_ epsilon of each other.

If at all possible, this project will try to use only Rust-native dependencies, and stay clear of integrating with C libraries through FFI.

Some liberty was taken to adapt some logic and algorithms from Yente, so do not expect fully-compliant API or behavior.

### Implementation matrix

 - [x] POST /match/{dataset}
 - [x] GET /entities/{id}
 - [x] GET /catalog _(proxy)_
 - [x] name-based
 - [x] name-qualified
 - [x] logic-v1 <sup>[1]</sup>

<sup>[1]</sup>: Features that are disabled by default were omited for now.

## Configuration

Motiva is configured via environment variables. The following variables are supported:

| Variable              | Description                                                                            | Default / Example       |
| --------------------- | -------------------------------------------------------------------------------------- | ----------------------- |
| `ENV`                 | Environment (`dev` or `production`)                                                    | `dev`                   |
| `LISTEN_ADDR`         | Address to bind the API server                                                         | `0.0.0.0:8000`          |
| `INDEX_URL`           | Elasticsearch URL                                                                      | `http://localhost:9200` |
| `INDEX_AUTH_METHOD`   | Elasticsearch authentication (`none`, `basic`, `bearer`, `api_key`, `encoded_api_key`) | `none`                  |
| `INDEX_CLIENT_ID`     | Elasticsearch client ID (required for `basic` or `api_key`)                            | _(none)_                |
| `INDEX_CLIENT_SECRET` | Elasticsearch client secret (required for `basic`, `api_key` or `encoded_api_key`)     | _(none)_                |
| `YENTE_URL`           | Optional URL to a Yente instance for score comparison                                  | _(none)_                |
| `CATALOG_URL`         | Optional URL to a catalog service                                                      | _(none)_                |
| `MATCH_CANDIDATES`    | Number of candidates to consider for matching                                          | `10`                    |
| `ENABLE_TRACING`      | Set to `1` to enable tracing                                                           | _(none)_                |
| `TRACING_EXPORTER`    | Tracing exporter kind (`otlp`, or `gcp` if compiled with the `gcp` feature)            | `otlp`                  |

`YENTE_URL` is required if your client needs to retrieve the actual catalog _through_ motiva. The `/catalog` request will be proxied to Yente.

You might want to use `CATALOG_URL` if you customized Yente's catalog in any way, so motiva can pull it regularly instead of Open Sanctions's default catalog.

## Run

Right now, there are _no configuration_ possible on this project, and it will remain that way until it is in a good enough shape to be used widely.

```
$ cargo run --release
$ echo '{"queries":{"test":{"schema":"Person","properties":{"name":["Vladimir Putin"]}}}}' | curl -XPOST 127.0.0.1:8080/match/sanctions -H content-type:application/json -d @-
```

## Development

### Test suite

To run the tests, a Python environment must be set up with the required dependencies (this include `libicu`). You can install it in a virtualenv by using the Poetry file at the root of this repository and (manually) setting the `PYTHONPATH`:

```
$ poetry install
$ export PYTHONPATH='.venv/lib/python3.13/site-packages'
$ cargo test
```

Note that this will run a pretty lengty test by default (scoring the cartesian product of 50x50 entities against each other) and compare it against nomenklatura. You can skip this test by running `cargo test -- --skip extensive`.

### Contributing

Motiva is a work in progress. Contributions and feedback are welcome!
