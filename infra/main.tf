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

locals {
  google_project = "skeelz-retrain-api"
  google_region  = "europe-west9"
  google_zone    = "europe-west9-c"
}

provider "google" {
  project = local.google_project
  region  = local.google_region
  zone    = local.google_zone
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

resource "google_secret_manager_secret" "env" {
  secret_id = "vhskeelz-db-env"
  replication {
    user_managed {
      replicas {
        location = local.google_region
      }
    }
  }
}

resource "google_service_account" "main" {
  account_id = local.google_project
  display_name = local.google_project
  timeouts {}
}

resource "google_service_account_key" "main" {
  service_account_id = google_service_account.main.name
  public_key_type = "TYPE_X509_PEM_FILE"
}

output "google_service_account_key" {
  value = google_service_account_key.main.private_key
  sensitive = true
}

resource "google_storage_bucket" "private" {
  name = "vhskeelz-db-private"
  location = local.google_region
  versioning {
      enabled = false
  }
}
