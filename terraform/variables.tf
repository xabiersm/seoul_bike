variable "credentials" {
  description = "My Credentials"
  default     = "~/Documents/Projects/secrets/useful-circle-430118-u0-3c3daef12ab4.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "useful-circle-430118-u0"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-west9"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "seoul_bike_trips_dataset"
}

variable "gcs_data_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "seoul-bike-trips-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "gce_service_account" {
  description = "Dataproc service account"
  default = "seoul-bike-data@useful-circle-430118-u0.iam.gserviceaccount.com"
}