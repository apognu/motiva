use std::{
  sync::{Arc, Mutex},
  time::Duration,
};

use axum_test::TestServer;
use libmotiva::{MockedElasticsearch, TestFetcher, prelude::*};
use nix::{sys::signal, unistd::Pid};
use reqwest::{StatusCode, header::AUTHORIZATION};
use rusty_fork::rusty_fork_test;

use crate::{
  api::{self, AppState, config::Config},
  tests::log_writer::VecLogWriter,
  trace::{build_prometheus, init_tracing},
};

#[tokio::test]
async fn api_invalid_credentials() {
  let index = MockedElasticsearch::builder().healthy(false).build();

  let state = AppState {
    config: Config {
      api_key: Some("myapikey".into()),
      ..Default::default()
    },
    prometheus: None,
    motiva: Motiva::with_fetcher(index, TestFetcher::default()).await.unwrap(),
  };

  let app = api::router(state);
  let server = TestServer::new(app).unwrap();
  let response = server.post("/match/default").await;

  assert_eq!(response.status_code(), 401);

  response.assert_text_contains("invalid credentials");

  let response = server.post("/match/default").add_header(AUTHORIZATION, "Bearer invalidkey").await;

  assert_eq!(response.status_code(), 401);

  response.assert_text_contains("invalid credentials");
}

#[tokio::test]
async fn api_valid_credentials() {
  let index = MockedElasticsearch::builder().healthy(false).build();

  let state = AppState {
    config: Config {
      api_key: Some("myapikey".into()),
      ..Default::default()
    },
    prometheus: None,
    motiva: Motiva::with_fetcher(index, TestFetcher::default()).await.unwrap(),
  };

  let app = api::router(state);
  let server = TestServer::new(app).unwrap();
  let response = server.post("/match/default").add_header(AUTHORIZATION, "Bearer myapikey").await;

  assert_eq!(response.status_code(), 415);
}

// The following tests need to be run into a fork because the tracing framework
// sets up global state that cannot be duplicated.
rusty_fork_test! {
    #[test]
    fn server() {
        let rt  = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let index = MockedElasticsearch::builder().healthy(true).build();

            let config = Config {
                index_url: "http://localhost:9200".into(),
                listen_addr: "0.0.0.0:8080".into(),
                ..Default::default()
            };

            tokio::task::spawn(async {
                crate::run(config, index).await.unwrap();
            });

            tokio::select! {
                response = async {
                    loop {
                        match reqwest::get("http://localhost:8080/healthz").await {
                            Ok(response) => return response,
                            Err(_) => continue,
                        };
                    }
                } => {
                    assert_eq!(response.status(), StatusCode::OK);
                }

                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    panic!("server did not respond within deadline");
                }
            };

            signal::kill(Pid::from_raw(std::process::id() as i32), Some(signal::Signal::SIGINT)).unwrap();

            assert!(matches!(reqwest::get("http://localhost:8080/healthz").await, Err(_)));
        });
    }

    #[test]
    fn logging() {
        let rt  = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let index = MockedElasticsearch::builder().healthy(true).build();

            let state = AppState {
                config: Config {
                    enable_tracing: true,
                    ..Default::default()
                },
                prometheus: None,
                motiva: Motiva::with_fetcher(index, TestFetcher::default()).await.unwrap(),
            };

            let buf = Arc::new(Mutex::new(Vec::default()));
            let (writer, wait) = VecLogWriter::new(Arc::clone(&buf));
            let (_guard, _) = init_tracing(&state.config, writer).await;

            let app = api::router(state);
            let server = TestServer::new(app).unwrap();
            let _ = server.post("/match/default").add_header("traceparent", "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01").await;

            wait.recv().unwrap();

            let lines = buf.lock().unwrap().clone();

            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("POST http://localhost/match/default"));
            assert!(lines[0].contains("request_id="));
            assert!(lines[0].contains("trace_id=0af7651916cd43dd8448eb211c80319c"));
            assert!(lines[0].contains(r#"remote="-" method=POST path="/match/default" status=415"#));
        });
    }

    #[test]
    fn metrics() {
        let rt  = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let index = MockedElasticsearch::builder().healthy(true).build();

            let state = AppState {
                config: Config {
                    enable_prometheus: true,
                    ..Default::default()
                },
                prometheus: Some(build_prometheus().unwrap()),
                motiva: Motiva::with_fetcher(index, TestFetcher::default()).await.unwrap(),
            };

            let app = api::router(state);
            let server = TestServer::new(app).unwrap();
            let _ = server.post("/match/default").await;
            let resp = server.get("/metrics").await;

            assert!(resp.text().contains(r#"http_requests_total{service="motiva",status="415"}"#))
        });
    }
}
