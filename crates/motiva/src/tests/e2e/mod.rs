use std::{collections::HashMap, str::FromStr};

use hurl::{
  runner::{self, RunnerOptionsBuilder, Value, VariableSet},
  util::logger::{ErrorFormat, LoggerOptionsBuilder},
};
use hurl_core::input::Input;

use jiff::Span;
use libmotiva::{ElasticsearchProvider, EsOptions};
use tokio::net::TcpListener;

use crate::{api::config::Config, run};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "end-to-end tests can only be run manually"]
async fn e2e() {
  let provider = ElasticsearchProvider::new(option_env!("INDEX_URL").unwrap_or_else(|| "http://localhost:9200".into()), EsOptions::default())
    .await
    .unwrap();

  let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
  let addr = listener.local_addr().unwrap();

  let mut config = Config::default();
  config.listener = Some(listener);
  config.request_timeout = Span::from_str("10s").unwrap();

  tokio::spawn(async move {
    run(config, provider).await.unwrap();
  });

  let logger = LoggerOptionsBuilder::new().color(true).error_format(ErrorFormat::Short).build();
  let vars = VariableSet::from(&HashMap::from_iter([("baseUrl".into(), Value::String(addr.to_string()))]));
  let opts = RunnerOptionsBuilder::default().build();

  for path in glob::glob(&format!("{}/src/tests/e2e/specs/*.hurl", env!("CARGO_MANIFEST_DIR"))).unwrap() {
    let Ok(path) = path else {
      continue;
    };

    let input = Input::new(&path.display().to_string());
    let content = input.read_to_string().unwrap();

    let result = runner::run(&content, Some(&input), &opts, &vars, &logger).unwrap();

    assert_eq!(result.errors().len(), 0);
  }
}
