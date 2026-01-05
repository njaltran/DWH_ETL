

-- Insurance facts by person and year
-- Grain: One row per person_id + year
-- Incremental strategy: Merge on (person_id, year)

WITH facts_raw AS (
  SELECT * FROM `dw-health-insurance-bipm`.`health_insurance_dev_staging`.`stg_insurance_facts_raw`
),

person_dim AS (
  SELECT person_id FROM `dw-health-insurance-bipm`.`health_insurance_dev_core`.`dim_person`
)

SELECT
  f.person_id,
  f.year,
  
  -- Metrics
  f.insurance_cost_year,
  f.annual_doctor_visits,
  f.annual_cost_to_insurance,
  
  -- Data quality flags
  f.insurance_cost_year_parse_error,
  f.annual_doctor_visits_parse_error,
  f.annual_cost_to_insurance_parse_error,
  f.year_parse_error,
  
  -- Referential integrity check
  CASE WHEN p.person_id IS NULL THEN TRUE ELSE FALSE END AS orphan_record,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS fact_updated_at

FROM facts_raw f
LEFT JOIN person_dim p USING (person_id)

WHERE f.year IS NOT NULL
  AND f.person_id IS NOT NULL

