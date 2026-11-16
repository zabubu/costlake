variable "export_name" {
  description = "Name of the AWS Data Exports job."
  type        = string
  default     = "costlake-focus"
  validation {
    condition     = can(regex("^[0-9A-Za-z!\\-_.*'()]{1,128}$", var.export_name))
    error_message = "Invalid export name."
  }
}

variable "focus_table" {
  description = "FOCUS table exposed by AWS Data Exports (bump when AWS publishes a newer FOCUS version)."
  type        = string
  default     = "FOCUS_1_0_AWS"
}

variable "columns" {
  description = "FOCUS columns selected by the export (those read by costlake)."
  type        = list(string)
  default = [
    "AvailabilityZone", "BilledCost", "BillingAccountId", "BillingAccountName", "BillingCurrency",
    "BillingPeriodEnd", "BillingPeriodStart", "ChargeCategory", "ChargeClass", "ChargeDescription",
    "ChargeFrequency", "ChargePeriodEnd", "ChargePeriodStart", "CommitmentDiscountCategory",
    "CommitmentDiscountId", "CommitmentDiscountName", "CommitmentDiscountStatus",
    "CommitmentDiscountType", "ConsumedQuantity", "ConsumedUnit", "ContractedCost",
    "ContractedUnitPrice", "EffectiveCost", "InvoiceIssuerName", "ListCost", "ListUnitPrice",
    "PricingCategory", "PricingQuantity", "PricingUnit", "ProviderName", "PublisherName",
    "RegionId", "RegionName", "ResourceId", "ResourceName", "ResourceType", "ServiceCategory",
    "ServiceName", "SkuId", "SkuPriceId", "SubAccountId", "SubAccountName", "Tags",
  ]
  validation {
    condition = alltrue([for c in ["BilledCost", "EffectiveCost", "BillingAccountId", "BillingCurrency",
      "BillingPeriodStart", "BillingPeriodEnd", "ChargeCategory", "ChargePeriodStart", "ChargePeriodEnd",
    "ProviderName", "ServiceName"] : contains(var.columns, c)])
    error_message = "The columns required by the costlake silver contract must be exported."
  }
}

variable "landing_bucket_name" {
  description = "Landing bucket receiving the export."
  type        = string
}

variable "landing_bucket_region" {
  description = "Region of the landing bucket."
  type        = string
}

variable "prefix" {
  description = "Key prefix of the export; must match the source prefix in config/environments/*.yaml."
  type        = string
  default     = "aws/focus"
}

variable "tags" {
  description = "Tags for the export."
  type        = map(string)
  default     = {}
}
