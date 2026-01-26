use proc_macro::TokenStream;
use quote::quote;

#[derive(deluxe::ParseMetaItem)]
#[deluxe(attributes(scoring_feature))]
struct FeatureAttributes(syn::Ident, #[deluxe(flatten)] FeatureNamedAttributes);

#[derive(deluxe::ParseMetaItem)]
struct FeatureNamedAttributes {
  name: String,
}

#[proc_macro_attribute]
pub fn scoring_feature(attrs: TokenStream, input: TokenStream) -> TokenStream {
  let FeatureAttributes(ident, FeatureNamedAttributes { name }) = deluxe::parse2::<FeatureAttributes>(attrs.into()).unwrap();
  let input = proc_macro2::TokenStream::from(input);

  quote! {
      pub struct #ident;

      impl Feature for #ident {
        fn name(&self) -> &'static str {
            #name
        }

        #[tracing::instrument(level = "trace", name = #name, skip_all, fields(entity_id = rhs.id))]
        #input
      }
  }
  .into()
}
