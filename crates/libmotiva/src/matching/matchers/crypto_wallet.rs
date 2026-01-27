use bumpalo::Bump;
use libmotiva_macros::scoring_feature;

use crate::{
  matching::Feature,
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(CryptoWalletMatch, name = "crypto_wallet_match")]
fn score_feature(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if !lhs.schema.is_a("CryptoWallet") || !rhs.schema.is_a("CryptoWallet") {
    return 0.0;
  }

  let lhs_props = lhs.props(&["publicKey"]);
  let rhs_props = rhs.props(&["publicKey"]);

  let (bigger, smaller) = if lhs_props.len() > rhs_props.len() { (&lhs_props, &rhs_props) } else { (&rhs_props, &lhs_props) };

  for a in smaller.iter() {
    if a.len() > 10 {
      for b in bigger.iter() {
        if b.len() > 10 && a == b {
          return 1.0;
        }
      }
    }
  }

  0.0
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;

  use crate::{
    matching::Feature,
    model::{Entity, SearchEntity},
  };

  #[test]
  fn crypto_wallet_match() {
    let lhs = SearchEntity::builder("CryptoWallet")
      .properties(&[("publicKey", &["TFXGYjZhsTLXz6ncMxtVQfwddJG5LoHcvZ", "383439889396444390"])])
      .build();
    let rhs = Entity::builder("CryptoWallet").properties(&[("publicKey", &["1234", "TFXGYjZhsTLXz6ncMxtVQfwddJG5LoHcvZ"])]).build();

    assert_eq!(super::CryptoWalletMatch.score_feature(&Bump::new(), &lhs, &rhs), 1.0);

    let lhs = SearchEntity::builder("CryptoWallet").properties(&[("publicKey", &["1234"])]).build();
    let rhs = Entity::builder("CryptoWallet").properties(&[("publicKey", &["1234"])]).build();

    assert_eq!(super::CryptoWalletMatch.score_feature(&Bump::new(), &lhs, &rhs), 0.0);
  }
}
