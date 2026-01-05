{{
  config(
    materialized='table',
    cluster_by=['person_id']
  )
}}

-- Conformed person dimension (SCD Type 1)
-- Grain: One row per person_id
-- Business key: person_id

WITH deduped AS (
  SELECT * FROM {{ ref('int_person_deduped') }}
)

SELECT
  person_id,
  
  -- Demographic attributes
  birthdate_parsed AS birthdate,
  CASE 
    WHEN birthdate_parsed IS NOT NULL 
    THEN DATE_DIFF(CURRENT_DATE(), birthdate_parsed, YEAR)
    ELSE NULL 
  END AS age_years,
  gender_std AS gender,
  family_status_std AS family_status,
  
  -- Insurance attributes
  insurance_status_std AS insurance_status,
  insurance_sign_up_date_parsed AS insurance_sign_up_date,
  EXTRACT(YEAR FROM insurance_sign_up_date_parsed) AS signup_year,
  FORMAT_DATE('%Y-%m', insurance_sign_up_date_parsed) AS signup_month,
  
  -- Economic attributes
  occupational_category_clean AS occupational_category,
  wealth_bracket_std AS wealth_bracket,
  
  -- Address attributes
  address_clean AS address,
  street_and_number,
  postal_code,
  city,
  
  -- Data quality flags
  birthdate_parse_error,
  insurance_sign_up_date_parse_error,
  CURRENT_TIMESTAMP() AS dim_updated_at

FROM deduped