pub fn validate_ogrn(code: &str) -> bool {
  if code.len() != 13 || !code.chars().all(|c| c.is_ascii_digit()) {
    return false;
  }

  let number: u64 = match code[..12].parse() {
    Ok(n) => n,
    Err(_) => return false,
  };

  let check_digit = ((number % 11) % 10) as u8;
  let last_digit = code.as_bytes()[12] - b'0';

  check_digit == last_digit
}

pub fn validate_inn(code: &str) -> bool {
  let digits: Vec<u8> = match code.chars().map(|c| c.to_digit(10).map(|d| d as u8)).collect() {
    Some(d) => d,
    None => return false,
  };

  match digits.len() {
    10 => {
      let coeffs = [2, 4, 10, 3, 5, 9, 4, 6, 8];
      inn_check_digit(&digits[..9], &coeffs) == digits[9]
    }
    12 => {
      let coeffs1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8, 0];
      let coeffs2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8];
      let n11 = inn_check_digit(&digits[..10], &coeffs1);
      let n12 = inn_check_digit(&digits[..11], &coeffs2);
      n11 == digits[10] && n12 == digits[11]
    }
    _ => false,
  }
}

fn inn_check_digit(digits: &[u8], coeffs: &[u8]) -> u8 {
  let sum: u32 = digits.iter().zip(coeffs.iter()).map(|(&d, &c)| (d as u32) * (c as u32)).sum();

  ((sum % 11) % 10) as u8
}

pub fn validate_mmsi(code: &str) -> bool {
  code.len() == 9 && code.chars().all(|c| c.is_ascii_digit())
}

pub fn validate_bic(code: &str) -> bool {
  if code.len() != 8 && code.len() != 11 {
    return false;
  }

  let chars: Vec<char> = code.chars().collect();

  if !chars[..4].iter().all(|c| c.is_ascii_alphabetic()) {
    return false;
  }
  if !chars[4..6].iter().all(|c| c.is_ascii_alphabetic()) {
    return false;
  }
  if !chars[6..8].iter().all(|c| c.is_ascii_alphanumeric()) {
    return false;
  }
  if code.len() == 11 && !chars[8..11].iter().all(|c| c.is_ascii_alphanumeric()) {
    return false;
  }
  true
}

pub fn validate_isin(code: &str) -> bool {
  if code.len() != 12 {
    return false;
  }

  let chars: Vec<char> = code.chars().collect();

  if !chars[..2].iter().all(|c| c.is_ascii_alphabetic()) {
    return false;
  }
  if !chars[2..11].iter().all(|c| c.is_ascii_alphanumeric()) {
    return false;
  }
  if !chars[11].is_ascii_digit() {
    return false;
  }

  let code = format!("{}{}{}", chars[0] as u8 - 55, chars[1] as u8 - 55, &code[2..]);

  luhn::valid(&code)
}

#[cfg(test)]
mod tests {
  #[test]
  fn validate_ogrn() {
    assert!(super::validate_ogrn("1027700132195"));
    assert!(!super::validate_ogrn("1027700132194"));
    assert!(!super::validate_ogrn("123456789012"));
    assert!(!super::validate_ogrn("abcdefghijklm"));
  }

  #[test]
  fn validate_inn() {
    assert!(super::validate_inn("7707083893"));
    assert!(super::validate_inn("500100732259"));
    assert!(!super::validate_inn("7707083894"));
    assert!(!super::validate_inn("abcdefghij"));
  }

  #[test]
  fn validate_mmo_mmsi() {
    assert!(super::validate_mmsi("366123456"));
    assert!(!super::validate_mmsi("12345678"));
    assert!(!super::validate_mmsi("1234567890"));
    assert!(!super::validate_mmsi("12345abc9"));
  }

  #[test]
  fn validate_bic() {
    assert!(super::validate_bic("DEUTDEFF"));
    assert!(super::validate_bic("DEUTDEFF500"));
    assert!(!super::validate_bic("DEUTDEFF50"));
    assert!(!super::validate_bic("DEUT12FF500"));
    assert!(!super::validate_bic("DEUTDE@F500"));
  }

  #[test]
  fn validate_isin() {
    assert!(super::validate_isin("US0378331005"));
    assert!(super::validate_isin("GB0002634946"));
    assert!(!super::validate_isin("US0378331006"));
    assert!(!super::validate_isin("US03783310A5"));
    assert!(!super::validate_isin("U0378331005"));
  }
}
