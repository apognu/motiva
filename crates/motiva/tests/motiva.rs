use libmotiva::{MockedElasticsearch, prelude::*};

#[tokio::test]
async fn scoring() {
  let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();

  let rhs = vec![
    Entity::builder("Person").id("Q7747").properties(&[("name", &["Vladimir Putin"])]).build(),
    Entity::builder("Person").id("A1234").properties(&[("name", &["Bob the Builder"])]).build(),
  ];

  let motiva = Motiva::new(MockedElasticsearch::builder().entities(rhs).build(), None).await.unwrap();
  let rhs = motiva.search(&lhs, &MatchParams::default()).await.unwrap();
  let scores = motiva.score::<LogicV1>(&lhs, rhs, 0.5).unwrap();

  assert_eq!(scores.len(), 2);

  assert_eq!(scores[0].1, 1.0);
  assert_eq!(scores[0].0.id, "Q7747");
  assert_eq!(scores[0].0.props(&["name"]).as_ref(), ["Vladimir Putin"]);

  assert_eq!(scores[1].1, 0.0);
  assert_eq!(scores[1].0.id, "A1234");
  assert_eq!(scores[1].0.props(&["name"]).as_ref(), ["Bob the Builder"]);
}

#[tokio::test]
async fn health() {
  let motiva = Motiva::new(MockedElasticsearch::builder().healthy(true).build(), None).await.unwrap();

  assert!(matches!(motiva.health().await, Ok(true)));

  let motiva = Motiva::new(MockedElasticsearch::builder().healthy(false).build(), None).await.unwrap();

  assert!(matches!(motiva.health().await, Ok(false)));

  let motiva = Motiva::new(MockedElasticsearch::default(), None).await.unwrap();

  assert!(matches!(motiva.health().await, Err(_)));
}

#[tokio::test]
#[should_panic]
// Should panic because mock function is not implemented.
async fn get_entity() {
  let motiva = Motiva::new(MockedElasticsearch::default(), None).await.unwrap();
  let _ = motiva.get_entity("", GetEntityBehavior::FetchNestedEntity).await;
}

#[tokio::test]
#[should_panic]
// Should panic because mock function is not implemented.
async fn get_related_entities() {
  let motiva = Motiva::new(MockedElasticsearch::default(), None).await.unwrap();
  let _ = motiva.get_entity("", GetEntityBehavior::RootOnly).await;
}
