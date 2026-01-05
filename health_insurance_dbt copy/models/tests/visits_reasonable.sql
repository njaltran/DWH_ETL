-- Test: Doctor visits should be between 0 and 365 per year
-- This test fails if values are outside reasonable bounds

SELECT
  person_id,
  year,
  annual_doctor_visits
FROM {{ ref('fact_insurance_yearly') }}
WHERE
  annual_doctor_visits IS NOT NULL
  AND (annual_doctor_visits < 0 OR annual_doctor_visits > 365)


