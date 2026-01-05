
  
    

    create or replace table `dw-health-insurance-bipm`.`health_insurance_dev_marts`.`mart_person_kpis`
      
    
    cluster by person_id

    
    OPTIONS()
    as (
      

-- Person-level KPIs aggregated across all years
-- Grain: One row per person_id
-- BI Use: Customer 360 view, lifetime value analysis, customer segmentation

WITH facts AS (
  SELECT * FROM `dw-health-insurance-bipm`.`health_insurance_dev_core`.`fact_insurance_yearly`
  WHERE NOT orphan_record  -- Exclude orphan records
),

person_dim AS (
  SELECT * FROM `dw-health-insurance-bipm`.`health_insurance_dev_core`.`dim_person`
)

SELECT
  p.person_id,
  
  -- Demographic context
  p.gender,
  p.age_years,
  p.family_status,
  p.wealth_bracket,
  p.city,
  p.insurance_status,
  
  -- Temporal coverage
  MIN(f.year) AS first_data_year,
  MAX(f.year) AS last_data_year,
  COUNT(DISTINCT f.year) AS years_with_data,
  
  -- Lifetime aggregates
  SUM(f.insurance_cost_year) AS lifetime_insurance_cost,
  SUM(f.annual_cost_to_insurance) AS lifetime_cost_to_insurance,
  SUM(f.annual_doctor_visits) AS lifetime_doctor_visits,
  
  -- Averages
  AVG(f.insurance_cost_year) AS avg_insurance_cost_per_year,
  AVG(f.annual_cost_to_insurance) AS avg_cost_to_insurance_per_year,
  AVG(f.annual_doctor_visits) AS avg_doctor_visits_per_year,
  
  -- Volatility (stddev)
  STDDEV(f.annual_cost_to_insurance) AS stddev_cost_to_insurance,
  STDDEV(f.annual_doctor_visits) AS stddev_doctor_visits,
  
  -- Trends (most recent vs earliest year)
  ARRAY_AGG(
    f.annual_cost_to_insurance IGNORE NULLS
    ORDER BY f.year DESC
    LIMIT 1
  )[SAFE_OFFSET(0)] AS latest_year_cost,
  ARRAY_AGG(
    f.annual_cost_to_insurance IGNORE NULLS
    ORDER BY f.year ASC
    LIMIT 1
  )[SAFE_OFFSET(0)] AS earliest_year_cost,
-- Metadata
  CURRENT_TIMESTAMP() AS mart_updated_at
FROM person_dim p
LEFT JOIN facts f USING (person_id)
GROUP BY
p.person_id,
p.gender,
p.age_years,
p.family_status,
p.wealth_bracket,
p.city,
p.insurance_status
    );
  