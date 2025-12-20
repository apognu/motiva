use std::{
  collections::HashSet,
  sync::{Arc, Mutex},
};

use ahash::{HashMap, RandomState};
use itertools::Itertools;

use crate::{Entity, IndexProvider, MotivaError, model::HasProperties, schemas::SCHEMAS};

const MAX_ITERATIONS: usize = 3;

pub(crate) async fn fetch_nested_entities<P: IndexProvider>(index: &P, root_entity: &mut Entity, root_id: &str) -> Result<(), MotivaError> {
  let mut all_entities: HashMap<String, Arc<Mutex<Entity>>> = HashMap::default();
  let mut seen = HashSet::<_, RandomState>::from_iter([root_id.to_string()]);
  let mut queue: Vec<(String, String)> = Vec::new();

  if let Some(properties) = root_entity.schema.properties() {
    for (name, property) in properties {
      if property._type != "entity" {
        continue;
      }

      for entity_id in root_entity.props(&[&name]).iter() {
        queue.push((entity_id.to_string(), name.clone()));
      }
    }
  }

  for iteration in 0..MAX_ITERATIONS {
    if queue.is_empty() && iteration > 0 {
      break;
    }

    let to_fetch_ids: Vec<String> = queue.iter().map(|(id, _)| id.clone()).unique().collect();
    let root_id_string = root_id.to_string();
    let root = if iteration == 0 { Some(&root_id_string) } else { None };

    let associations = index.get_related_entities(root, &to_fetch_ids, &seen).await?;

    let mut next: Vec<(String, String)> = Vec::new();

    for association in associations {
      let Some(schema) = SCHEMAS.get(association.schema.as_str()) else {
        continue;
      };

      let node = Arc::new(Mutex::new(association.clone()));
      all_entities.insert(association.id.clone(), Arc::clone(&node));
      seen.insert(association.id.clone());

      link_entity_to_parents(root_entity, &all_entities, &queue, &association, &node);
      link_reverse_properties(root_entity, &all_entities, &association, schema, &node);

      if iteration == 0 || association.schema.is_edge() {
        queue_entity_references(&association, schema, &seen, &mut next);
      }
    }

    queue = next;
  }

  Ok(())
}

fn link_entity_to_parents(root: &mut Entity, all_entities: &HashMap<String, Arc<Mutex<Entity>>>, queue: &[(String, String)], association: &Entity, node: &Arc<Mutex<Entity>>) {
  for (fetch_id, prop) in queue {
    if fetch_id != &association.id {
      continue;
    }

    let mut linked = false;

    if root.props(&[prop]).contains(&association.id) {
      root.properties.entities.entry(prop.clone()).or_default().push(Arc::clone(node));
      linked = true;
    }

    if !linked {
      for (parent_id, parent) in all_entities {
        if parent_id == &association.id {
          continue;
        }

        if let Ok(entity) = parent.lock()
          && entity.props(&[prop]).contains(&association.id)
        {
          drop(entity);

          if let Ok(mut parent_entity) = parent.lock() {
            parent_entity.properties.entities.entry(prop.clone()).or_default().push(Arc::clone(node));
          }
          break;
        }
      }
    }
  }
}

fn link_reverse_properties(root: &mut Entity, all_entities: &HashMap<String, Arc<Mutex<Entity>>>, association: &Entity, schema: &crate::schemas::FtmSchema, node: &Arc<Mutex<Entity>>) {
  for (prop, values) in &association.properties.strings {
    let Some(property) = schema.properties.get(prop) else {
      continue;
    };

    if property._type != "entity" {
      continue;
    }

    if values.contains(&root.id)
      && let Some(reverse) = property.reverse.as_ref()
    {
      root.properties.entities.entry(reverse.name.clone()).or_default().push(Arc::clone(node));
    }

    for value in values {
      if let Some(target) = all_entities.get(value)
        && let Some(reverse) = property.reverse.as_ref()
        && let Ok(mut entity) = target.lock()
      {
        entity.properties.entities.entry(reverse.name.clone()).or_default().push(Arc::clone(node));
      }
    }
  }
}

fn queue_entity_references(association: &Entity, schema: &crate::schemas::FtmSchema, seen: &HashSet<String, RandomState>, next: &mut Vec<(String, String)>) {
  for (prop, values) in &association.properties.strings {
    let Some(property) = schema.properties.get(prop) else {
      continue;
    };

    if property._type != "entity" {
      continue;
    }

    for value in values {
      if !seen.contains(value) {
        next.push((value.clone(), prop.clone()));
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use std_macro_extensions::{hash_set, string};

  use crate::{Entity, MockedElasticsearch};

  #[tokio::test]
  async fn no_references() {
    let mut root = Entity::builder("Person").id("person-1").build();
    let index = MockedElasticsearch::builder().build();

    super::fetch_nested_entities(&index, &mut root, "person-1").await.unwrap();

    assert!(root.properties.entities.is_empty());
  }

  #[tokio::test]
  async fn unknown_schemas() {
    let mut root = Entity::builder("Person").id("person-1").properties(&[("addressEntity", &["wizard-1"])]).build();
    let wizard = Entity::builder("Wizard").id("wizard-1").build();

    let index = MockedElasticsearch::builder()
      .related_entitites(vec![((Some(string!("person-1")), vec![string!("wizard-1")], hash_set!(string!("person-1"))), vec![wizard.clone()])])
      .build();

    super::fetch_nested_entities(&index, &mut root, "person-1").await.unwrap();

    assert!(!root.properties.entities.contains_key("addressEntity"));
  }

  #[tokio::test]
  async fn single_address() {
    let mut root = Entity::builder("Person").id("person-1").properties(&[("addressEntity", &["addr-1"])]).build();
    let address = Entity::builder("Address").id("addr-1").build();

    let index = MockedElasticsearch::builder()
      .related_entitites(vec![((Some(string!("person-1")), vec![string!("addr-1")], hash_set!(string!("person-1"))), vec![address.clone()])])
      .build();

    super::fetch_nested_entities(&index, &mut root, "person-1").await.unwrap();

    assert!(root.properties.entities.contains_key("addressEntity"));

    let addresses = &root.properties.entities["addressEntity"];
    assert_eq!(addresses.len(), 1);
    assert_eq!(addresses[0].lock().unwrap().id, "addr-1");
  }

  #[tokio::test]
  async fn multiple_values_same_property() {
    let mut root = Entity::builder("Person").id("person-1").properties(&[("addressEntity", &["addr-1", "addr-2"])]).build();
    let address1 = Entity::builder("Address").id("addr-1").build();
    let address2 = Entity::builder("Address").id("addr-2").build();

    let index = MockedElasticsearch::builder()
      .related_entitites(vec![(
        (Some(string!("person-1")), vec![string!("addr-1"), string!("addr-2")], hash_set!(string!("person-1"))),
        vec![address1.clone(), address2.clone()],
      )])
      .build();

    super::fetch_nested_entities(&index, &mut root, "person-1").await.unwrap();

    assert!(root.properties.entities.contains_key("addressEntity"));
    let addresses = &root.properties.entities["addressEntity"];
    assert_eq!(addresses.len(), 2);

    let ids: Vec<String> = addresses.iter().map(|a| a.lock().unwrap().id.clone()).collect();
    assert!(ids.contains(&string!("addr-1")));
    assert!(ids.contains(&string!("addr-2")));
  }

  #[tokio::test]
  async fn two_levels() {
    let mut root = Entity::builder("Person").id("person-1").build();
    let person = Entity::builder("Person").id("person-2").build();
    let relative1 = Entity::builder("Family").id("relative-1").properties(&[("relative", &["person-1"]), ("person", &["person-2"])]).build();

    let index = MockedElasticsearch::builder()
      .related_entitites(vec![
        ((Some(string!("person-1")), vec![], hash_set!(string!("person-1"))), vec![relative1.clone()]),
        ((None, vec![string!("person-2")], hash_set!(string!("person-1"), string!("relative-1"))), vec![person.clone()]),
      ])
      .build();

    super::fetch_nested_entities(&index, &mut root, "person-1").await.unwrap();

    assert!(root.properties.entities.contains_key("familyRelative"));
    let relatives = &root.properties.entities["familyRelative"];

    assert_eq!(relatives.len(), 1);

    let relative = relatives[0].lock().unwrap();
    let person = relative.properties.entities["person"][0].lock().unwrap();

    assert_eq!(person.id, "person-2");
  }

  #[tokio::test]
  async fn reverse_relationships() {
    let mut root = Entity::builder("Person").id("person-1").build();
    let person = Entity::builder("Person").id("person-2").build();
    let relative1 = Entity::builder("Family").id("relative-1").properties(&[("relative", &["person-1"]), ("person", &["person-2"])]).build();
    let relative2 = Entity::builder("Family").id("relative-2").properties(&[("relative", &["person-1"])]).build();

    let index = MockedElasticsearch::builder()
      .related_entitites(vec![
        ((Some(string!("person-1")), vec![], hash_set!(string!("person-1"))), vec![relative1.clone(), relative2.clone()]),
        (
          (None, vec![string!("person-2")], hash_set!(string!("person-1"), string!("relative-1"), string!("relative-2"))),
          vec![person.clone()],
        ),
      ])
      .build();

    super::fetch_nested_entities(&index, &mut root, "person-1").await.unwrap();

    assert!(root.properties.entities.contains_key("familyRelative"));
    let relatives = &root.properties.entities["familyRelative"];

    assert_eq!(relatives.len(), 2);

    let ids: Vec<String> = relatives.iter().map(|a| a.lock().unwrap().id.clone()).collect();

    assert_eq!(ids[0], "relative-1");
    assert_eq!(ids[1], "relative-2");
  }

  #[tokio::test]
  async fn circular_reference_prevention() {
    let mut root = Entity::builder("Company").id("company-1").properties(&[("parent", &["company-2"])]).build();
    let company2 = Entity::builder("Company").id("company-2").properties(&[("subsidiaries", &["company-1"])]).build();

    let index = MockedElasticsearch::builder()
      .related_entitites(vec![(
        (Some(string!("company-1")), vec![string!("company-2")], hash_set!(string!("company-1"))),
        vec![company2.clone()],
      )])
      .build();

    super::fetch_nested_entities(&index, &mut root, "company-1").await.unwrap();

    assert!(root.properties.entities.contains_key("parent"));
    let parents = &root.properties.entities["parent"];
    assert_eq!(parents.len(), 1);
    assert_eq!(parents[0].lock().unwrap().id, "company-2");

    let company2_entity = parents[0].lock().unwrap();
    assert!(!company2_entity.properties.entities.contains_key("subsidiaries"));
  }

  #[tokio::test]
  async fn edge_entity_handling() {
    let mut root = Entity::builder("Person").id("person-1").properties(&[("proof", &["doc-1"])]).build();
    let doc = Entity::builder("Documentation").id("doc-1").properties(&[("document", &["doc-2"]), ("entity", &["person-1"])]).build();
    let document = Entity::builder("Document").id("doc-2").build();

    let index = MockedElasticsearch::builder()
      .related_entitites(vec![
        ((Some(string!("person-1")), vec![string!("doc-1")], hash_set!(string!("person-1"))), vec![doc.clone()]),
        ((None, vec![string!("doc-2")], hash_set!(string!("person-1"), string!("doc-1"))), vec![document.clone()]),
      ])
      .build();

    super::fetch_nested_entities(&index, &mut root, "person-1").await.unwrap();

    assert!(root.properties.entities.contains_key("proof"));
    let proof = &root.properties.entities["proof"];
    assert_eq!(proof.len(), 1);
    assert_eq!(proof[0].lock().unwrap().id, "doc-1");

    let doc_entity = proof[0].lock().unwrap();
    assert!(doc_entity.properties.entities.contains_key("document"));
    assert_eq!(doc_entity.properties.entities["document"][0].lock().unwrap().id, "doc-2");
  }

  #[tokio::test]
  async fn empty_results() {
    let mut root = Entity::builder("Person").id("person-1").properties(&[("parent", &["company-missing"])]).build();

    let index = MockedElasticsearch::builder()
      .related_entitites(vec![((Some(string!("person-1")), vec![string!("company-missing")], hash_set!(string!("person-1"))), vec![])])
      .build();

    super::fetch_nested_entities(&index, &mut root, "person-1").await.unwrap();

    assert!(root.properties.entities.is_empty());
  }

  #[tokio::test]
  async fn self_reference() {
    let mut root = Entity::builder("Company").id("company-1").properties(&[("parent", &["company-1"])]).build();

    let index = MockedElasticsearch::builder()
      .related_entitites(vec![((Some(string!("company-1")), vec![string!("company-1")], hash_set!(string!("company-1"))), vec![])])
      .build();

    super::fetch_nested_entities(&index, &mut root, "company-1").await.unwrap();

    assert!(!root.properties.entities.contains_key("parent"));
  }

  #[tokio::test]
  async fn early_termination() {
    let mut root = Entity::builder("Person").id("person-1").properties(&[("addressEntity", &["addr-1"])]).build();
    let address = Entity::builder("Address").id("addr-1").build();

    let index = MockedElasticsearch::builder()
      .related_entitites(vec![((Some(string!("person-1")), vec![string!("addr-1")], hash_set!(string!("person-1"))), vec![address.clone()])])
      .build();

    super::fetch_nested_entities(&index, &mut root, "person-1").await.unwrap();

    assert!(root.properties.entities.contains_key("addressEntity"));
    let addresses = &root.properties.entities["addressEntity"];
    assert_eq!(addresses.len(), 1);
    assert_eq!(addresses[0].lock().unwrap().id, "addr-1");
  }
}
