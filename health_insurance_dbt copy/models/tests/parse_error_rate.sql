-- Test: Parse error rate should be below threshold (5%)
-- This test fails if too many records have parse errors

WITH error_counts AS (
  SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN birthdate_parse_error THEN 1 ELSE 0 END) AS birthdate_errors,
    SUM(CASE WHEN insurance_sign_up_date_parse_error THEN 1 ELSE 0 END) AS signup_date_errors
  FROM {{ ref('dim_person') }}
),

error_rates AS (
  SELECT
    total_records,
    birthdate_errors,
    signup_date_errors,
    ROUND(100.0 * birthdate_errors / NULLIF(total_records, 0), 2) AS birthdate_error_pct,
    ROUND(100.0 * signup_date_errors / NULLIF(total_records, 0), 2) AS signup_date_error_pct
  FROM error_counts
)

-- Fail if error rate exceeds 5%
SELECT
  'birthdate' AS field,
  birthdate_errors AS error_count,
  total_records,
  birthdate_error_pct AS error_rate_pct
FROM error_rates
WHERE birthdate_error_pct > 5

UNION ALL

SELECT
  'insurance_sign_up_date' AS field,
  signup_date_errors AS error_count,
  total_records,
  signup_date_error_pct AS error_rate_pct
FROM error_rates
WHERE signup_date_error_pct > 5