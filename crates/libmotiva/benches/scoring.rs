use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use libmotiva::{prelude::*, scoring};

fn name_based(c: &mut Criterion) {
  let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
  let rhs = std::iter::repeat(vec![Entity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build()])
    .take(10)
    .flatten()
    .collect::<Vec<_>>();

  c.bench_function("name_based", |b| b.iter(|| black_box(scoring::score::<NameBased>(&lhs, rhs.clone(), 0.5))));
}

fn name_qualified(c: &mut Criterion) {
  let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
  let rhs = std::iter::repeat(vec![Entity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build()])
    .take(10)
    .flatten()
    .collect::<Vec<_>>();

  c.bench_function("name_qualified", |b| b.iter(|| black_box(scoring::score::<NameQualified>(&lhs, rhs.clone(), 0.5))));
}

fn logic_v1(c: &mut Criterion) {
  let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
  let rhs = std::iter::repeat(vec![Entity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build()])
    .take(10)
    .flatten()
    .collect::<Vec<_>>();

  c.bench_function("logic_v1", |b| b.iter(|| black_box(scoring::score::<LogicV1>(&lhs, rhs.clone(), 0.5))));
}

criterion_group!(benches, name_based, name_qualified, logic_v1);
criterion_main!(benches);
