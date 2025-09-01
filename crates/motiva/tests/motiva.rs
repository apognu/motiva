use libmotiva::prelude::*;

#[tokio::test]
async fn plop() {
  let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();

  let rhs = vec![
    Entity::builder("Person").id("Q7747").properties(&[("name", &["Vladimir Putin"])]).build(),
    Entity::builder("Person").id("A1234").properties(&[("name", &["Bob the Builder"])]).build(),
  ];

  let motiva = Motiva::new(MockedElasticsearch::with_entities(rhs), None).await.unwrap();
  let rhs = motiva.search(&lhs, &MatchParams::default()).await.unwrap();
  let scores = motiva.score::<LogicV1>(&lhs, rhs, 0.5).unwrap();

  assert_eq!(scores.len(), 2);

  assert_eq!(scores[0].1, 1.0);
  assert_eq!(scores[0].0.id, "Q7747");
  assert_eq!(scores[0].0.property("name"), ["Vladimir Putin"]);

  assert_eq!(scores[1].1, 0.0);
  assert_eq!(scores[1].0.id, "A1234");
  assert_eq!(scores[1].0.property("name"), ["Bob the Builder"]);
}
