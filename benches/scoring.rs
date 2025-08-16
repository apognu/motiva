#![cfg(feature = "benchmarks")]

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use motiva::{LogicV1, NameBased, NameQualified, scoring, tests};

pub fn name_based(c: &mut Criterion) {
  let lhs = tests::se("Person").properties(&[("name", &["Vladimir Putin"])]).call();
  let rhs = std::iter::repeat(vec![tests::e("Person").properties(&[("name", &["Vladimir Putin"])]).call()])
    .take(10)
    .flatten()
    .collect::<Vec<_>>();

  c.bench_function("name_based", |b| b.iter(|| black_box(scoring::score::<NameBased>(&lhs, rhs.clone(), 0.5))));
}

pub fn name_qualified(c: &mut Criterion) {
  let lhs = tests::se("Person").properties(&[("name", &["Vladimir Putin"])]).call();
  let rhs = std::iter::repeat(vec![tests::e("Person").properties(&[("name", &["Vladimir Putin"])]).call()])
    .take(10)
    .flatten()
    .collect::<Vec<_>>();

  c.bench_function("name_qualified", |b| b.iter(|| black_box(scoring::score::<NameQualified>(&lhs, rhs.clone(), 0.5))));
}

pub fn logic_v1(c: &mut Criterion) {
  let lhs = tests::se("Person").properties(&[("name", &["Vladimir Putin"])]).call();
  let rhs = std::iter::repeat(vec![tests::e("Person").properties(&[("name", &["Vladimir Putin"])]).call()])
    .take(10)
    .flatten()
    .collect::<Vec<_>>();

  c.bench_function("logic_v1", |b| b.iter(|| black_box(scoring::score::<LogicV1>(&lhs, rhs.clone(), 0.5))));
}

criterion_group!(benches, name_based, name_qualified, logic_v1);
criterion_main!(benches);
