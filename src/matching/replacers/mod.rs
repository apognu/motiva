use aho_corasick::AhoCorasick;
use rust_embed::Embed;

pub mod addresses;
pub mod company_types;
pub mod ordinals;

#[derive(Embed)]
#[folder = "assets/rigour/resources"]
pub struct Dictionaries;

pub fn replace<R>(aho: &AhoCorasick, replacements: &[R], haystack: &str) -> String
where
  R: AsRef<str>,
{
  let mut out = String::with_capacity(haystack.len());
  let mut cursor = 0;

  for mat in aho.find_iter(haystack) {
    let start_is_boundary = mat.start() == 0 || !haystack[..mat.start()].chars().next_back().unwrap().is_alphanumeric();
    let end_is_boundary = mat.end() == haystack.len() || !haystack[mat.end()..].chars().next().unwrap().is_alphanumeric();

    if start_is_boundary && end_is_boundary {
      out.push_str(&haystack[cursor..mat.start()]);
      out.push_str(replacements[mat.pattern().as_usize()].as_ref());

      cursor = mat.end();
    }
  }

  out.push_str(&haystack[cursor..]);
  out
}
