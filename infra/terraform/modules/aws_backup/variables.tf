variable "backup_configs" {
  description = "A map of backup configurations."
  type = map(object({
    name               = string
    rule_name          = string
    target_vault_name  = string
    schedule           = string
    start_window       = number
    completion_window  = number
    cold_storage_after = number
    delete_after       = string
    selection_name     = string
    iam_role_arn       = string
    resources          = list(string)
    selection_tags     = map(string)
    force_destroy      = bool
  }))
  default = {}
}
