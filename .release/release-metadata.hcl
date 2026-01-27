release {
  provider                = "github"
  url                     = "https://github.com/openkcm/krypton-operator/releases"
  changelog               = "CHANGELOG.md"
  tag_prefix              = "v"
  release_name_template   = "Release {{.Version}}"
  assets = [
    "*"
  ]
}
