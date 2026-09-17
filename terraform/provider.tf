provider "aws" {
  region = var.region

  # Every resource gets the four project tags. managed-by was "manual" on
  # the console-built originals; the first apply after import flips it.
  default_tags {
    tags = {
      project     = "steam-player-analytics"
      environment = var.environment
      owner       = var.owner
      managed-by  = "terraform"
    }
  }
}
