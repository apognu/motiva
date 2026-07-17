use bumpalo::Bump;
use libmotiva_macros::scoring_feature;

use crate::{
  matching::{Detail, Feature},
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(CryptoWalletMatch, name = "crypto_wallet_match")]
fn score(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> (f64, Option<Detail>) {
  if !lhs.schema.is_a("CryptoWallet") || !rhs.schema.is_a("CryptoWallet") {
    return (0.0, explain.then_some(Detail::Note("not a crypto wallet")));
  }

  let lhs_props = lhs.props(&["publicKey"]);
  let rhs_props = rhs.props(&["publicKey"]);

  let (bigger, smaller) = if lhs_props.len() > rhs_props.len() { (&lhs_props, &rhs_props) } else { (&rhs_props, &lhs_props) };

  for a in smaller.iter() {
    if a.len() > 10 {
      for b in bigger.iter() {
        if b.len() > 10 && a == b {
          return (1.0, explain.then(|| Detail::Labeled("matched public key", a.as_str().into())));
        }
      }
    }
  }

  (0.0, explain.then_some(Detail::Note("no matching public key")))
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

    assert_eq!(super::CryptoWalletMatch.score_scalar(&Bump::new(), &lhs, &rhs), 1.0);

    let lhs = SearchEntity::builder("CryptoWallet").properties(&[("publicKey", &["1234"])]).build();
    let rhs = Entity::builder("CryptoWallet").properties(&[("publicKey", &["1234"])]).build();

    assert_eq!(super::CryptoWalletMatch.score_scalar(&Bump::new(), &lhs, &rhs), 0.0);
  }

  #[test]
  fn crypto_wallet_match_details() {
    fn detail(lhs: &SearchEntity, rhs: &Entity) -> Option<String> {
      super::CryptoWalletMatch.score(&Bump::new(), lhs, rhs, true).1.map(|detail| detail.to_string())
    }

    // Not a crypto wallet.
    let lhs = SearchEntity::builder("Person").properties(&[("publicKey", &["TFXGYjZhsTLXz6ncMxtVQfwddJG5LoHcvZ"])]).build();
    let rhs = Entity::builder("CryptoWallet").properties(&[("publicKey", &["TFXGYjZhsTLXz6ncMxtVQfwddJG5LoHcvZ"])]).build();
    assert_eq!(detail(&lhs, &rhs).as_deref(), Some("not a crypto wallet"));

    // Matched public key.
    let lhs = SearchEntity::builder("CryptoWallet").properties(&[("publicKey", &["TFXGYjZhsTLXz6ncMxtVQfwddJG5LoHcvZ"])]).build();
    let rhs = Entity::builder("CryptoWallet").properties(&[("publicKey", &["TFXGYjZhsTLXz6ncMxtVQfwddJG5LoHcvZ"])]).build();
    assert_eq!(detail(&lhs, &rhs).as_deref(), Some("matched public key: TFXGYjZhsTLXz6ncMxtVQfwddJG5LoHcvZ"));

    // No matching public key.
    let lhs = SearchEntity::builder("CryptoWallet").properties(&[("publicKey", &["AAAAAAAAAAAAAAAAAAAA"])]).build();
    let rhs = Entity::builder("CryptoWallet").properties(&[("publicKey", &["BBBBBBBBBBBBBBBBBBBB"])]).build();
    assert_eq!(detail(&lhs, &rhs).as_deref(), Some("no matching public key"));
  }
}
