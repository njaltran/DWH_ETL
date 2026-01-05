-- Test: No negative costs in fact table
-- This test fails if any negative values exist in cost columns

SELECT
  person_id,
  year,
  insurance_cost_year,
  annual_cost_to_insurance
FROM {{ ref('fact_insurance_yearly') }}
WHERE
  (insurance_cost_year < 0 AND insurance_cost_year IS NOT NULL)
  OR (annual_cost_to_insurance < 0 AND annual_cost_to_insurance IS NOT NULL)