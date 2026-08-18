use any_ascii::any_ascii;

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
    assert_eq!(super::latinize("Наталья"), "Natal'ya");
  }
}
