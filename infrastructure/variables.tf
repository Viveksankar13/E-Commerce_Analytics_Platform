# variables.tf

variable "resource_group_name" {
  type        = string
  description = "The name of the resource group to create."
  default     = "rg-ecommerce-data-platform"
}

variable "location" {
  type        = string
  description = "The Azure region to deploy resources in."
  default     = "eastus2"
}

variable "environment" {
  type        = string
  description = "Deployment environment identifier (e.g. dev, staging, prod)."
  default     = "prod"
}

variable "storage_account_name" {
  type        = string
  description = "The globally unique name of the ADLS Gen2 Storage Account."
  default     = "adls2ecommerceanalytics"
}

variable "databricks_workspace_name" {
  type        = string
  description = "The name of the Azure Databricks workspace."
  default     = "dbw-ecommerce-platform"
}
