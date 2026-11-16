output "export_arn" {
  description = "ARN of the Data Exports job."
  value       = aws_bcmdataexports_export.focus.arn
}

output "query_statement" {
  description = "SQL statement of the export."
  value       = local.query
}
