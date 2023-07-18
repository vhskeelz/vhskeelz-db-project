terraform {
  cloud {
    organization = "skeelz"
    workspaces {
      name = "vhskeelz-db"
    }
  }

  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.73.2"
    }
  }
}

provider "google" {
  project = "skeelz-retrain-api"
  region  = "europe-west9"
  zone    = "europe-west9-c"
}

resource "google_sql_database_instance" "main" {
  name             = "skeelz-retrain-api"
  database_version = "POSTGRES_15"
  settings {
    tier = "db-f1-micro"
    deletion_protection_enabled = true
    insights_config {
      query_insights_enabled = false
    }
    maintenance_window {
      day = 5
      hour = 22
    }
  }
}
