pub(crate) mod tagger;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum SymbolCategory {
  Name,
  Nick,
  OrgClass,
  Numeric,
  Location,
  Symbol,
}

impl std::fmt::Display for SymbolCategory {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(match self {
      SymbolCategory::Name => "NAME",
      SymbolCategory::Nick => "NICK",
      SymbolCategory::OrgClass => "ORGCLS",
      SymbolCategory::Numeric => "NUM",
      SymbolCategory::Location => "LOC",
      SymbolCategory::Symbol => "SYMBOL",
    })
  }
}

impl SymbolCategory {
  pub(crate) fn boost(&self) -> Option<f64> {
    match self {
      SymbolCategory::Numeric => Some(1.4),
      SymbolCategory::Location => Some(1.1),
      SymbolCategory::OrgClass => Some(0.7),
      SymbolCategory::Symbol => Some(0.8),
      _ => None,
    }
  }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Symbol {
  pub category: SymbolCategory,
  pub id: String,
}

impl Symbol {
  pub fn new(category: SymbolCategory, id: impl Into<String>) -> Self {
    Symbol { category, id: id.into() }
  }
}
