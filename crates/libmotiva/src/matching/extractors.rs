use std::{borrow::Borrow, sync::LazyLock};

use any_ascii::any_ascii;
use itertools::Itertools;
use regex::Regex;
use rphonetic::{Encoder, Metaphone};

pub(crate) fn tokenize_names<'s, I, S>(names: I) -> impl Iterator<Item = impl Iterator<Item = &'s str>>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  names.map(|s| s.borrow().split_whitespace())
}

#[inline(always)]
pub(crate) fn clean_names<'s, I, S>(names: I) -> impl Iterator<Item = String> + Clone
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + Clone + 's,
{
  names
    .map(|s| any_ascii(s.borrow()).to_lowercase().chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
    .unique()
}

#[inline(always)]
pub(crate) fn clean_literal_names<'s, I, S>(names: I) -> impl Iterator<Item = String> + Clone
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + Clone + 's,
{
  names
    .map(|s| s.borrow().to_lowercase().chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
    .unique()
}

#[inline(always)]
pub(crate) fn clean_address_parts<'s, I, S>(names: I) -> impl Iterator<Item = String> + Clone
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + Clone + 's,
{
  names
    .map(|s| {
      any_ascii(s.borrow())
        .to_lowercase()
        .chars()
        .map(|c| match c {
          c if c.is_alphanumeric() || c.is_whitespace() => c,
          _ => ' ',
        })
        .collect::<String>()
    })
    .unique()
}

#[inline(always)]
pub(crate) fn tokenize_clean_names<'s, I, S>(names: I) -> impl Iterator<Item = String>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  names
    .flat_map(|s| s.borrow().split_whitespace())
    .map(|s| any_ascii(s).to_lowercase().chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
    .unique()
}

pub(crate) fn phonetic_name<'s, I, S>(metaphone: &Metaphone, names: I) -> impl Iterator<Item = String>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  tokenize_names(names).flat_map(|s| s.map(|s| metaphone.encode(s)))
}

pub(crate) fn phonetic_names_tuples<'s, I, S>(metaphone: &Metaphone, names: I) -> Vec<Vec<(&'s str, Option<String>)>>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  tokenize_names(names)
    .map(|s| {
      s.filter(|name| name.len() >= 2)
        .map(|s| {
          (s, {
            let phoneme = metaphone.encode(s);

            if phoneme.len() < 3 { None } else { Some(phoneme) }
          })
        })
        .collect()
    })
    .collect()
}

pub(crate) fn name_keys<'s, I, S>(names: I) -> impl Iterator<Item = String>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  tokenize_names(names).map(|tokens| {
    let mut tokens = tokens.map(|token| any_ascii(token).to_lowercase()).collect::<Vec<_>>();

    tokens.sort();
    tokens.join("")
  })
}

pub(crate) fn name_parts_flat<'s, I, S>(names: I) -> impl Iterator<Item = String>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  tokenize_names(names)
    .flatten()
    .filter(|s| s.len() > 1)
    .map(|s| any_ascii(s).to_lowercase().chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
    .unique()
}

pub(crate) fn name_parts<'s, I, S>(names: I) -> impl Iterator<Item = Vec<String>>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  tokenize_names(names)
    .map(|s| {
      s.map(|s| any_ascii(s).to_lowercase().chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
        .collect::<Vec<_>>()
    })
    .unique()
}

pub(crate) fn flip_date(mut date: Vec<char>) -> Vec<char> {
  let (m1, m2) = (date[0], date[1]);
  let (d1, d2) = (date[3], date[4]);

  (date[0], date[1]) = (d1, d2);
  (date[3], date[4]) = (m1, m2);

  date
}

static NUMBERS_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\d+").unwrap());

pub(crate) fn extract_numbers<'s, I, S>(haystack: I) -> impl Iterator<Item = &'s str>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  haystack.flat_map(|value| NUMBERS_REGEX.find_iter(value.borrow()).map(|number| number.as_str()))
}

#[cfg(test)]
mod tests {
  use std::collections::HashSet;

  use rphonetic::Metaphone;

  #[test]
  fn tokenize_names() {
    let names = super::tokenize_names(["Barack Hussein Obama"].iter()).map(|n| n.collect::<Vec<_>>()).collect::<Vec<_>>();

    assert_eq!(names, vec![vec!["Barack", "Hussein", "Obama"]]);

    let names = super::tokenize_clean_names(["POLIN, Vladimir Anatolevich", "Владимир Анатольевич Полин", "Vladimir Anatolevich Polin"].iter());

    assert_eq!(
      HashSet::<String>::from_iter(names),
      HashSet::from_iter(vec!["polin".to_string(), "anatolevich".to_string(), "vladimir".to_string()])
    );
  }

  #[test]
  fn phonetic_name() {
    let names = super::phonetic_name(&Metaphone::default(), ["Vladimir Putin", "Saddam Hussein", "Barack Hussein Obama"].iter()).collect::<Vec<_>>();

    assert_eq!(names, vec!["FLTM", "PTN", "STM", "HSN", "BRK", "HSN", "OBM"]);
  }

  #[test]
  fn name_keys() {
    let names = super::name_keys(["Владимир Путин"].iter()).collect::<Vec<_>>();

    assert_eq!(names, vec!["putinvladimir"]);
  }

  #[test]
  fn name_parts() {
    let names = super::name_parts_flat(["Vladimir Vladimorovich Putin", "Barack Hussein Obama"].iter()).collect::<Vec<_>>();

    assert_eq!(names, vec!["vladimir", "vladimorovich", "putin", "barack", "hussein", "obama"]);
  }
}
