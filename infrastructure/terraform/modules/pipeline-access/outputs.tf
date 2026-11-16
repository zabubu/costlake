output "role_arn" {
  description = "ARN of the pipeline role."
  value       = aws_iam_role.pipeline.arn
}

output "policy_json" {
  description = "Inline policy attached to the role."
  value       = data.aws_iam_policy_document.access.json
}
