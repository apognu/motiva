#[cfg(not(feature = "icu"))]
use any_ascii::any_ascii;

#[cfg(feature = "icu")]
thread_local! {
    static TRANSLITERATOR: rust_icu_utrans::UTransliterator = {
        use rust_icu_sys;
        use rust_icu_utrans;

        rust_icu_utrans::UTransliterator::new(
            "Any-Latin; NFKD; [:Nonspacing Mark:] Remove; Accents-Any; [:Symbol:] Remove; [:Nonspacing Mark:] Remove; Latin-ASCII",
            None,
            rust_icu_sys::UTransDirection::UTRANS_FORWARD,
        )
        .unwrap()
    };
}

#[cfg(feature = "icu")]
pub(crate) fn latinize(value: &str) -> String {
  if value.is_ascii() {
    return value.to_string();
  }

  TRANSLITERATOR.with(|t| t.transliterate(value).unwrap_or_else(|_| value.to_string()))
}

#[cfg(not(feature = "icu"))]
pub(crate) fn latinize(value: &str) -> String {
  if value.is_ascii() {
    return value.to_string();
  }

  any_ascii(value)
}

#[cfg(test)]
mod tests {
  #[test]
  fn latinize() {
    assert_eq!(super::latinize("Светлана"), "Svetlana");

    #[cfg(feature = "icu")]
    assert_eq!(super::latinize("Наталья"), "Natal'a");
    #[cfg(not(feature = "icu"))]
    assert_eq!(super::latinize("Наталья"), "Natal'ya");
  }
}
