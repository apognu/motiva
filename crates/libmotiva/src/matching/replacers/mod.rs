use aho_corasick::AhoCorasick;
use rust_embed::Embed;

pub(crate) mod addresses;
pub(crate) mod company_types;
pub(crate) mod ordinals;
pub(crate) mod stopwords;
pub(crate) mod symbols;

#[derive(Embed)]
#[folder = "./assets/rigour/resources"]
struct Dictionaries;

#[derive(Embed)]
#[folder = "./assets/fingerprints/fingerprints"]
struct Fingerprints;

pub(crate) fn replace<R>(aho: &AhoCorasick, replacements: &[R], haystack: &str) -> String
where
  R: AsRef<str>,
{
  let bytes = haystack.as_bytes();
  let mut out = String::with_capacity(haystack.len());
  let mut cursor = 0;

  for mat in aho.find_iter(haystack) {
    let start_is_boundary = mat.start() == 0 || {
      if let Some(&byte) = bytes.get(mat.start().wrapping_sub(1)) {
        !(byte as char).is_alphanumeric()
      } else {
        true
      }
    };

    let end_is_boundary = mat.end() == haystack.len() || { if let Some(&byte) = bytes.get(mat.end()) { !(byte as char).is_alphanumeric() } else { true } };

    if start_is_boundary && end_is_boundary {
      out.push_str(&haystack[cursor..mat.start()]);
      out.push_str(replacements[mat.pattern().as_usize()].as_ref());

      cursor = mat.end();
    }
  }

  out.push_str(&haystack[cursor..]);
  out
}
