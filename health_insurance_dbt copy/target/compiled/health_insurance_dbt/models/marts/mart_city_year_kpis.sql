

-- City-level yearly aggregations for geographic analysis
-- Grain: One row per city + year
-- BI Use: Regional performance, geographic segmentation, market analysis

WITH facts AS (
  SELECT * FROM `dw-health-insurance-bipm`.`health_insurance_dev_core`.`fact_insurance_yearly`
  WHERE NOT orphan_record
),

person_dim AS (
  SELECT * FROM `dw-health-insurance-bipm`.`health_insurance_dev_core`.`dim_person`
)

SELECT
  COALESCE(p.city, 'Unknown') AS city,
  p.postal_code,
  f.year,
  
  -- Volume metrics
  COUNT(DISTINCT f.person_id) AS persons_in_city,
  
  -- Cost aggregates
  SUM(f.insurance_cost_year) AS total_insurance_cost,
  SUM(f.annual_cost_to_insurance) AS total_cost_to_insurance,
  AVG(f.insurance_cost_year) AS avg_insurance_cost_per_person,
  AVG(f.annual_cost_to_insurance) AS avg_cost_to_insurance_per_person,
  
  -- Visit aggregates
  SUM(f.annual_doctor_visits) AS total_doctor_visits,
  AVG(f.annual_doctor_visits) AS avg_doctor_visits_per_person,
  
  -- Demographics
  AVG(p.age_years) AS avg_age,
  COUNTIF(p.gender = 'male') AS male_count,
  COUNTIF(p.gender = 'female') AS female_count,
  
  -- Wealth distribution
  COUNTIF(p.wealth_bracket = 'low') AS low_wealth_count,
  COUNTIF(p.wealth_bracket = 'medium') AS medium_wealth_count,
  COUNTIF(p.wealth_bracket = 'upper_middle') AS upper_middle_wealth_count,
  COUNTIF(p.wealth_bracket = 'high') AS high_wealth_count,
  
  -- Insurance status
  COUNTIF(p.insurance_status = 'active') AS active_count,
  COUNTIF(p.insurance_status = 'inactive') AS inactive_count,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS mart_updated_at

FROM person_dim p
INNER JOIN facts f USING (person_id)
WHERE p.city IS NOT NULL
GROUP BY
  p.city,
  p.postal_code,
  f.year