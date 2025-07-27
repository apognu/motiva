use std::collections::HashSet;

use any_ascii::any_ascii;
use itertools::Itertools;
use rphonetic::{Encoder, Metaphone};

pub fn tokenize_names<S: AsRef<str>>(names: &[S]) -> impl Iterator<Item = impl Iterator<Item = &str>> {
  names.iter().map(|s| s.as_ref().split_whitespace())
}

pub fn clean_names<S: AsRef<str>>(names: &[S]) -> HashSet<String> {
  names
    .iter()
    .map(|s| any_ascii(s.as_ref()).to_lowercase().chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
    .collect()
}

pub fn tokenize_clean_names<S: AsRef<str>>(names: &[S]) -> impl Iterator<Item = String> {
  names
    .iter()
    .flat_map(|s| s.as_ref().split_whitespace())
    .map(|s| any_ascii(s).to_lowercase().chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
    .unique()
}

pub fn phonetic_name<S: AsRef<str>>(metaphone: &Metaphone, names: &[S]) -> impl Iterator<Item = String> {
  tokenize_names(names).flat_map(|s| s.map(|s| metaphone.encode(s)))
}

pub fn name_keys<S: AsRef<str>>(names: &[S]) -> impl Iterator<Item = String> {
  tokenize_names(names).map(|tokens| {
    let mut tokens = tokens.map(|token| any_ascii(token).to_lowercase()).collect::<Vec<_>>();

    tokens.sort();
    tokens.join("")
  })
}

pub fn name_parts<S: AsRef<str>>(names: &[S]) -> impl Iterator<Item = String> {
  tokenize_names(names)
    .flatten()
    .filter(|s| s.len() > 1)
    .map(|s| any_ascii(s).to_lowercase().chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
    .unique()
}

pub fn is_disjoint(lhs: &[String], rhs: &[String]) -> bool {
  HashSet::<String>::from_iter(lhs.to_vec()).is_disjoint(&HashSet::from_iter(rhs.to_vec()))
}

pub fn flip_date(mut date: Vec<char>) -> Vec<char> {
  let (m1, m2) = (date[0], date[1]);
  let (d1, d2) = (date[3], date[4]);

  (date[0], date[1]) = (d1, d2);
  (date[3], date[4]) = (m1, m2);

  date
}

#[cfg(test)]
mod tests {
  use std::collections::HashSet;

  use rphonetic::Metaphone;

  #[test]
  fn tokenize_names() {
    let names = super::tokenize_names(&["Barack Hussein Obama"]).map(|n| n.collect::<Vec<_>>()).collect::<Vec<_>>();

    assert_eq!(names, vec![vec!["Barack", "Hussein", "Obama"]]);

    let names = super::tokenize_clean_names(&["POLIN, Vladimir Anatolevich", "Владимир Анатольевич Полин", "Vladimir Anatolevich Polin"]);

    assert_eq!(
      HashSet::<String>::from_iter(names),
      HashSet::from_iter(vec!["polin".to_string(), "anatolevich".to_string(), "vladimir".to_string()])
    );
  }

  #[test]
  fn phonetic_name() {
    let names = super::phonetic_name(&Metaphone::default(), &["Vladimir Putin", "Saddam Hussein", "Barack Hussein Obama"]).collect::<Vec<_>>();

    assert_eq!(names, vec!["FLTM", "PTN", "STM", "HSN", "BRK", "HSN", "OBM"]);
  }

  #[test]
  fn name_keys() {
    let names = super::name_keys(&["Владимир Путин"]).collect::<Vec<_>>();

    assert_eq!(names, vec!["putinvladimir"]);
  }

  #[test]
  fn name_parts() {
    let names = super::name_parts(&["Vladimir Vladimorovich Putin", "Barack Hussein Obama"]).collect::<Vec<_>>();

    assert_eq!(names, vec!["vladimir", "vladimorovich", "putin", "barack", "hussein", "obama"]);
  }
}
