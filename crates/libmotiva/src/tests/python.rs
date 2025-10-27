use std::{env, path::PathBuf};

use anyhow::Context;
use pyo3::{prelude::*, types::IntoPyDict};

use crate::{
  matching::Algorithm,
  model::{Entity, Schema, SearchEntity},
};

impl<'py> IntoPyObject<'py> for Schema {
  type Target = PyAny;
  type Output = Bound<'py, Self::Target>;
  type Error = PyErr;

  fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
    py.import("followthemoney.model")?
      .getattr("Model")?
      .call1((&PathBuf::new().join(env::var("PYTHONPATH").unwrap()).join("followthemoney/schema").display().to_string(),))?
      .getattr("get")?
      .call1((self.as_str(),))
  }
}

impl Algorithm {
  pub(crate) fn as_nomenklatura(&self) -> &'static str {
    match self {
      Algorithm::NameBased => "NameMatcher",
      Algorithm::NameQualified => "NameQualifiedMatcher",
      Algorithm::LogicV1 => "LogicV1",
    }
  }
}

#[derive(Clone, FromPyObject)]
struct PyMatchingResult {
  pub score: f64,
}

pub(crate) struct MatchResults(pub Vec<(Entity, f64)>);

impl IntoIterator for MatchResults {
  type Item = (Entity, f64);
  type IntoIter = std::vec::IntoIter<Self::Item>;

  fn into_iter(self) -> Self::IntoIter {
    self.0.into_iter()
  }
}

pub(crate) fn nomenklatura_score(matcher: Algorithm, query: &SearchEntity, hits: Vec<Entity>) -> anyhow::Result<MatchResults> {
  let result = Python::attach::<_, PyResult<MatchResults>>(|py| {
    let ftm = py.import("followthemoney.proxy")?;
    let matching = py.import("nomenklatura.matching")?;

    let query = {
      let data = vec![("properties", query.properties.clone())].into_py_dict(py)?;
      ftm.getattr("EntityProxy")?.call1((query.schema.clone(), data))?
    };

    let mut scores: Vec<(Entity, f64)> = Vec::with_capacity(hits.len());

    for hit in hits {
      let entity = {
        let data = vec![("properties", hit.properties.strings.clone())].into_py_dict(py)?;
        ftm.getattr("EntityProxy")?.call1((hit.schema.clone(), data))?
      };

      let config = py.import("nomenklatura.matching.types")?.getattr("ScoringConfig")?.getattr("defaults")?.call0()?;

      let matcher = matching.getattr(matcher.as_nomenklatura())?.getattr("compare")?;
      let score: PyMatchingResult = matcher.call1((&query, entity, config))?.extract()?;

      scores.push((hit, score.score));
    }

    Ok(MatchResults(scores))
  });

  result.context("could not compute score")
}

pub(crate) fn nomenklatura_comparer(path: &str, function: &str, query: &SearchEntity, entity: &Entity) -> anyhow::Result<f64> {
  let result = Python::attach::<_, PyResult<f64>>(|py| {
    let ftm = py.import("followthemoney.proxy")?;

    let query = {
      let data = vec![("properties", query.properties.clone())].into_py_dict(py)?;
      ftm.getattr("EntityProxy")?.call1((query.schema.clone(), data))?
    };

    let entity = {
      let data = vec![("properties", entity.properties.strings.clone())].into_py_dict(py)?;
      ftm.getattr("EntityProxy")?.call1((entity.schema.clone(), data))?
    };

    let inspect = py.import("inspect")?.getattr("signature")?;
    let matcher = py.import(&format!("nomenklatura.matching.{path}"))?.getattr(function)?;

    let score: f64 = match inspect.call1((matcher.clone(),))?.getattr("parameters")?.len()? {
      2 => matcher.call1((&query, entity))?.extract()?,
      3 => {
        let config = py.import("nomenklatura.matching.types")?.getattr("ScoringConfig")?.getattr("defaults")?.call0()?;

        matcher.call1((&query, entity, config))?.getattr("score")?.extract()?
      }

      _ => panic!("unexpected comparer method type"),
    };

    Ok(score)
  });

  result.context("could not compute score")
}

pub(crate) fn nomenklatura_str_list(path: &str, function: &str, query: &[&str], entity: &[&str]) -> anyhow::Result<f64> {
  let result = Python::attach::<_, PyResult<f64>>(|py| {
    let matcher = py.import(&format!("nomenklatura.matching.{path}"))?.getattr(function)?;
    let score = matcher.call1((query, entity))?.extract()?;

    Ok(score)
  });

  result.context("could not compute score")
}
