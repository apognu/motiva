use std::{borrow::Borrow, sync::LazyLock};

use any_ascii::any_ascii;
use itertools::Itertools;
use regex::Regex;
use rphonetic::{Encoder, Metaphone};
use whatlang::Script;

use crate::matching::latinize::latinize;

const NAME_SEPARATORS: &[char] = &['-'];

// TODO: better support for separators
fn is_name_separator(c: char) -> bool {
  NAME_SEPARATORS.contains(&c) || c.is_whitespace()
}

fn is_modern_alphabet(input: &str) -> bool {
  let Some(info) = whatlang::detect(input) else {
    return true;
  };

  matches!(info.script(), Script::Latin | Script::Greek | Script::Armenian | Script::Cyrillic)
}

pub(crate) fn tokenize_names<'s, I, S>(names: I) -> impl Iterator<Item = impl Iterator<Item = &'s str>>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  names.map(|s| s.borrow().split(is_name_separator))
}

#[inline(always)]
pub(crate) fn clean_names<'s, I, S>(names: I) -> impl Iterator<Item = String> + Clone
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + Clone + 's,
{
  names
    .map(|s| {
      latinize(s.borrow())
        .to_lowercase()
        .split(is_name_separator)
        .map(|s| s.chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
        .join(" ")
    })
    .unique()
}

#[inline(always)]
pub(crate) fn normalize_identifiers<'s, I, S>(ids: I) -> impl Iterator<Item = String> + Clone
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + Clone + 's,
{
  ids
    .map(|s| latinize(s.borrow()).chars().filter(|c| c.is_alphanumeric()).collect::<String>().to_uppercase())
    .filter(|s| s.len() >= 2)
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
      latinize(s.borrow())
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
    .map(|s| latinize(s).to_lowercase().chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
    .filter(|s| s.len() >= 2)
    .unique()
}

pub(crate) fn phonetic_name<'s, I, S>(metaphone: &Metaphone, names: I) -> impl Iterator<Item = String>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  tokenize_names(names)
    .flat_map(|s| s.filter(|s| is_modern_alphabet(s) && s.chars().count() >= 3).map(|s| metaphone.encode(&any_ascii(s))))
    .filter(|phoneme| phoneme.len() > 2)
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

pub(crate) fn index_name_keys<'s, I, S>(names: I) -> impl Iterator<Item = String>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  tokenize_names(names)
    .map(|tokens| {
      let mut tokens = tokens
        .map(|token| if is_modern_alphabet(token) { latinize(token).to_lowercase() } else { token.to_lowercase() })
        .collect::<Vec<_>>();

      tokens.sort();
      tokens.join("")
    })
    .filter(|keys| keys.len() > 5)
}

pub(crate) fn index_name_parts<'s, I, S>(names: I) -> impl Iterator<Item = String>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  tokenize_names(names)
    .flatten()
    .filter(|s| s.chars().count() > 1)
    .map(|s| match is_modern_alphabet(s) {
      true => latinize(s).to_lowercase(),
      false => s.to_lowercase(),
    })
    .unique()
}

pub(crate) fn name_parts_flat<'s, I, S>(names: I) -> impl Iterator<Item = String>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  tokenize_names(names)
    .flatten()
    .filter(|s| s.chars().count() > 1)
    .map(|s| latinize(s).to_lowercase().chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
    .unique()
}

pub(crate) fn name_parts<'s, I, S>(names: I) -> impl Iterator<Item = Vec<String>>
where
  S: Borrow<str> + 's,
  I: Iterator<Item = &'s S> + 's,
{
  tokenize_names(names)
    .map(|s| {
      s.map(|s| latinize(s).to_lowercase().chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>())
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
  fn is_modern_alphabet() {
    let input = &[
      ("Nicolas Sarkozy", true),
      ("Μιχαήλ Στασινόπουλος", true),
      ("Владимир Путин", true),
      ("Czas do szkoły", true),
      ("標準語", false),
      ("ในหนึ่งสัปดาห์มีเจ็ดวัน", false),
    ];

    for (text, expected) in input {
      assert_eq!(super::is_modern_alphabet(text), *expected);
    }
  }

  #[test]
  fn clean_names() {
    assert_eq!(super::clean_names(vec!["Bob-a O'Brien#"].iter()).collect::<Vec<_>>(), vec!["bob a obrien"]);
  }

  #[test]
  fn normalize_identifiers() {
    assert_eq!(super::normalize_identifiers(vec!["FR12-34/uc12.3 (d)"].iter()).collect::<Vec<_>>(), vec!["FR1234UC123D"]);
  }

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
    let names = super::index_name_keys(["Владимир Путин"].iter()).collect::<Vec<_>>();

    assert_eq!(names, vec!["putinvladimir"]);
  }

  #[test]
  fn name_parts() {
    let names = super::name_parts_flat(["Vladimir Vladimorovich Putin", "Barack Hussein Obama"].iter()).collect::<Vec<_>>();

    assert_eq!(names, vec!["vladimir", "vladimorovich", "putin", "barack", "hussein", "obama"]);
  }
}
