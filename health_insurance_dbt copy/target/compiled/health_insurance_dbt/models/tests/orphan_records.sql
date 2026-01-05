-- Test: Flag orphan records in fact table (for monitoring)
-- This test warns if orphan records exist (not a hard failure)

SELECT
  person_id,
  year,
  'Fact record has no matching person in dim_person' AS issue
FROM `dw-health-insurance-bipm`.`health_insurance_dev_core`.`fact_insurance_yearly`
WHERE orphan_record = TRUE