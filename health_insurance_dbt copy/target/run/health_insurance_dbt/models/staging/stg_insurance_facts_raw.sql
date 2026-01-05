

  create or replace view `dw-health-insurance-bipm`.`health_insurance_dev_staging`.`stg_insurance_facts_raw`
  OPTIONS()
  as 

-- Light cleaning and safe type casting of raw insurance facts
-- Preserve all records with parse error flags

WITH source AS (
  SELECT
    TRIM(Person_id) AS person_id,
    insurance_cost_year,
    annual_doctor_visits,
    annual_cost_to_insurance,
    year
  FROM `dw-health-insurance-bipm`.`health_insurance_raw`.`health_insurance_insurance_facts_raw`
  WHERE Person_id IS NOT NULL
)

SELECT
  person_id,
  
  -- Safe numeric conversions with error flags
  SAFE_CAST(insurance_cost_year AS NUMERIC) AS insurance_cost_year,
  CASE WHEN SAFE_CAST(insurance_cost_year AS NUMERIC) IS NULL 
       AND insurance_cost_year IS NOT NULL 
       THEN TRUE ELSE FALSE END AS insurance_cost_year_parse_error,
  
  SAFE_CAST(annual_doctor_visits AS INT64) AS annual_doctor_visits,
  CASE WHEN SAFE_CAST(annual_doctor_visits AS INT64) IS NULL 
       AND annual_doctor_visits IS NOT NULL 
       THEN TRUE ELSE FALSE END AS annual_doctor_visits_parse_error,
  
  SAFE_CAST(annual_cost_to_insurance AS NUMERIC) AS annual_cost_to_insurance,
  CASE WHEN SAFE_CAST(annual_cost_to_insurance AS NUMERIC) IS NULL 
       AND annual_cost_to_insurance IS NOT NULL 
       THEN TRUE ELSE FALSE END AS annual_cost_to_insurance_parse_error,
  
  SAFE_CAST(year AS INT64) AS year,
  CASE WHEN SAFE_CAST(year AS INT64) IS NULL 
       AND year IS NOT NULL 
       THEN TRUE ELSE FALSE END AS year_parse_error

FROM source;

