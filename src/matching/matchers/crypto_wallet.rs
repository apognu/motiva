use std::collections::HashSet;

use bumpalo::Bump;
use macros::scoring_feature;

use crate::{
  matching::Feature,
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(CryptoWalletMatch, name = "crypto_wallet_match")]
fn score_feature(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if !lhs.schema.is_a("CryptoWallet") || !rhs.schema.is_a("CryptoWallet") {
    return 0.0;
  }

  let lhs_addresses = HashSet::<&String>::from_iter(lhs.property("publicKey"));
  let rhs_addresses = HashSet::<&String>::from_iter(rhs.property("publicKey"));

  for address in lhs_addresses.intersection(&rhs_addresses) {
    if address.len() > 10 {
      return 1.0;
    }
  }

  0.0
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;

  use crate::{
    matching::Feature,
    tests::{e, se},
  };

  #[test]
  fn crypto_wallet_match() {
    let lhs = se("CryptoWallet").properties(&[("publicKey", &["TFXGYjZhsTLXz6ncMxtVQfwddJG5LoHcvZ", "383439889396444390"])]).call();
    let rhs = e("CryptoWallet").properties(&[("publicKey", &["1234", "TFXGYjZhsTLXz6ncMxtVQfwddJG5LoHcvZ"])]).call();

    assert_eq!(super::CryptoWalletMatch.score_feature(&Bump::new(), &lhs, &rhs), 1.0);
  }
}
