# main.tf

# 1. Resource Group
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    Environment = var.environment
    Pipeline    = "E-Commerce-Inventory-Pricing"
    ManagedBy   = "Terraform"
  }
}

# 2. Azure Data Lake Storage Gen2 Account
resource "azurerm_storage_account" "adls" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Essential for ADLS Gen2 hierarchical namespace

  # Security configurations
  enable_https_traffic_only = true
  min_tls_version           = "TLS1_2"

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# 3. Storage Data Lake Gen2 Filesystem Containers (Bronze, Silver, Gold)
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.adls.id
}

# 4. Azure Databricks Workspace (Premium Tier for Unity Catalog compatibility)
resource "azurerm_databricks_workspace" "databricks" {
  name                        = var.databricks_workspace_name
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = azurerm_resource_group.rg.location
  sku                         = "premium"
  managed_resource_group_name = "${var.resource_group_name}-managed-db"

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# --- OUTPUTS ---
output "resource_group_name" {
  value       = azurerm_resource_group.rg.name
  description = "The name of the Resource Group."
}

output "storage_account_id" {
  value       = azurerm_storage_account.adls.id
  description = "The ID of the ADLS Gen2 Storage Account."
}

output "storage_account_primary_dfs_endpoint" {
  value       = azurerm_storage_account.adls.primary_dfs_endpoint
  description = "The primary DFS endpoint for the ADLS Gen2 Storage Account."
}

output "databricks_workspace_url" {
  value       = azurerm_databricks_workspace.databricks.workspace_url
  description = "The workspace URL of the deployed Azure Databricks instance."
}
