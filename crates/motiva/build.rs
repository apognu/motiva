fn main() {
  let version = git_version::git_version!(args = ["--tags"], fallback = "dev");

  println!("cargo:rustc-env=VERSION={version}");
}
