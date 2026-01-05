
    
    

with  __dbt__cte__int_person_deduped as (


-- Parse dates, standardize categories, extract address components
-- Each raw row is cleaned but duplicates remain

WITH source AS (
  SELECT * FROM `dw-health-insurance-bipm`.`health_insurance_dev_staging`.`stg_person_dim_raw`
),

-- Date parsing logic for birthdate
birthdate_parsed AS (
  SELECT
    person_id,
    birthdate_raw,
    
    -- Try multiple date formats
    COALESCE(
      -- ISO format: YYYY-MM-DD
      SAFE.PARSE_DATE('%Y-%m-%d', birthdate_raw),
      
      -- Dot format: DD.MM.YYYY
      SAFE.PARSE_DATE('%d.%m.%Y', birthdate_raw),
      
      -- Dash format: DD-MM-YYYY
      SAFE.PARSE_DATE('%d-%m-%Y', birthdate_raw),
      
      -- Slash format with heuristic (birthdate defaults to MM/DD/...)
      CASE
        -- Check if it's a slash-delimited date
        WHEN REGEXP_CONTAINS(birthdate_raw, r'^\d{1,2}/\d{1,2}/\d{2,4}$') THEN
          CASE
            -- Extract parts
            WHEN SAFE_CAST(SPLIT(birthdate_raw, '/')[OFFSET(1)] AS INT64) > 12 THEN
              -- Second part > 12, must be MM/DD/...
              CASE
                WHEN LENGTH(SPLIT(birthdate_raw, '/')[OFFSET(2)]) = 4 THEN
                  SAFE.PARSE_DATE('%m/%d/%Y', birthdate_raw)
                ELSE
                  -- 2-digit year
                  CASE
                    WHEN SAFE_CAST(SPLIT(birthdate_raw, '/')[OFFSET(2)] AS INT64) BETWEEN 0 AND 29 THEN
                      SAFE.PARSE_DATE('%m/%d/%y', REPLACE(birthdate_raw, '/' || SPLIT(birthdate_raw, '/')[OFFSET(2)], '/20' || SPLIT(birthdate_raw, '/')[OFFSET(2)]))
                    ELSE
                      SAFE.PARSE_DATE('%m/%d/%y', REPLACE(birthdate_raw, '/' || SPLIT(birthdate_raw, '/')[OFFSET(2)], '/19' || SPLIT(birthdate_raw, '/')[OFFSET(2)]))
                  END
              END
            
            WHEN SAFE_CAST(SPLIT(birthdate_raw, '/')[OFFSET(0)] AS INT64) > 12 THEN
              -- First part > 12, must be DD/MM/...
              CASE
                WHEN LENGTH(SPLIT(birthdate_raw, '/')[OFFSET(2)]) = 4 THEN
                  SAFE.PARSE_DATE('%d/%m/%Y', birthdate_raw)
                ELSE
                  -- 2-digit year
                  CASE
                    WHEN SAFE_CAST(SPLIT(birthdate_raw, '/')[OFFSET(2)] AS INT64) BETWEEN 0 AND 29 THEN
                      SAFE.PARSE_DATE('%d/%m/%y', REPLACE(birthdate_raw, '/' || SPLIT(birthdate_raw, '/')[OFFSET(2)], '/20' || SPLIT(birthdate_raw, '/')[OFFSET(2)]))
                    ELSE
                      SAFE.PARSE_DATE('%d/%m/%y', REPLACE(birthdate_raw, '/' || SPLIT(birthdate_raw, '/')[OFFSET(2)], '/19' || SPLIT(birthdate_raw, '/')[OFFSET(2)]))
                  END
              END
            
            ELSE
              -- Ambiguous: default to MM/DD/... for birthdate
              CASE
                WHEN LENGTH(SPLIT(birthdate_raw, '/')[OFFSET(2)]) = 4 THEN
                  SAFE.PARSE_DATE('%m/%d/%Y', birthdate_raw)
                ELSE
                  -- 2-digit year
                  CASE
                    WHEN SAFE_CAST(SPLIT(birthdate_raw, '/')[OFFSET(2)] AS INT64) BETWEEN 0 AND 29 THEN
                      SAFE.PARSE_DATE('%m/%d/%y', REPLACE(birthdate_raw, '/' || SPLIT(birthdate_raw, '/')[OFFSET(2)], '/20' || SPLIT(birthdate_raw, '/')[OFFSET(2)]))
                    ELSE
                      SAFE.PARSE_DATE('%m/%d/%y', REPLACE(birthdate_raw, '/' || SPLIT(birthdate_raw, '/')[OFFSET(2)], '/19' || SPLIT(birthdate_raw, '/')[OFFSET(2)]))
                  END
              END
          END
      END
    ) AS birthdate_parsed
  FROM source
),

-- Date parsing logic for insurance_sign_up_date
signup_date_parsed AS (
  SELECT
    person_id,
    insurance_sign_up_date_raw,
    
    COALESCE(
      -- ISO format: YYYY-MM-DD
      SAFE.PARSE_DATE('%Y-%m-%d', insurance_sign_up_date_raw),
      
      -- Dot format: DD.MM.YYYY
      SAFE.PARSE_DATE('%d.%m.%Y', insurance_sign_up_date_raw),
      
      -- Dash format: DD-MM-YYYY
      SAFE.PARSE_DATE('%d-%m-%Y', insurance_sign_up_date_raw),
      
      -- Slash format with heuristic (signup defaults to DD/MM/...)
      CASE
        WHEN REGEXP_CONTAINS(insurance_sign_up_date_raw, r'^\d{1,2}/\d{1,2}/\d{2,4}$') THEN
          CASE
            WHEN SAFE_CAST(SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(1)] AS INT64) > 12 THEN
              -- Second part > 12, must be MM/DD/...
              CASE
                WHEN LENGTH(SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)]) = 4 THEN
                  SAFE.PARSE_DATE('%m/%d/%Y', insurance_sign_up_date_raw)
                ELSE
                  CASE
                    WHEN SAFE_CAST(SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)] AS INT64) BETWEEN 0 AND 29 THEN
                      SAFE.PARSE_DATE('%m/%d/%y', REPLACE(insurance_sign_up_date_raw, '/' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)], '/20' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)]))
                    ELSE
                      SAFE.PARSE_DATE('%m/%d/%y', REPLACE(insurance_sign_up_date_raw, '/' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)], '/19' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)]))
                  END
              END
            
            WHEN SAFE_CAST(SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(0)] AS INT64) > 12 THEN
              -- First part > 12, must be DD/MM/...
              CASE
                WHEN LENGTH(SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)]) = 4 THEN
                  SAFE.PARSE_DATE('%d/%m/%Y', insurance_sign_up_date_raw)
                ELSE
                  CASE
                    WHEN SAFE_CAST(SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)] AS INT64) BETWEEN 0 AND 29 THEN
                      SAFE.PARSE_DATE('%d/%m/%y', REPLACE(insurance_sign_up_date_raw, '/' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)], '/20' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)]))
                    ELSE
                      SAFE.PARSE_DATE('%d/%m/%y', REPLACE(insurance_sign_up_date_raw, '/' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)], '/19' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)]))
                  END
              END
            
            ELSE
              -- Ambiguous: default to DD/MM/... for signup date
              CASE
                WHEN LENGTH(SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)]) = 4 THEN
                  SAFE.PARSE_DATE('%d/%m/%Y', insurance_sign_up_date_raw)
                ELSE
                  CASE
                    WHEN SAFE_CAST(SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)] AS INT64) BETWEEN 0 AND 29 THEN
                      SAFE.PARSE_DATE('%d/%m/%y', REPLACE(insurance_sign_up_date_raw, '/' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)], '/20' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)]))
                    ELSE
                      SAFE.PARSE_DATE('%d/%m/%y', REPLACE(insurance_sign_up_date_raw, '/' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)], '/19' || SPLIT(insurance_sign_up_date_raw, '/')[OFFSET(2)]))
                  END
              END
          END
      END
    ) AS insurance_sign_up_date_parsed
  FROM source
),

-- Address parsing
address_parsed AS (
  SELECT
    person_id,
    address_raw,
    REGEXP_REPLACE(TRIM(address_raw), r'\s+', ' ') AS address_clean,
    REGEXP_EXTRACT(address_raw, r'\b(\d{5})\b') AS postal_code,
    INITCAP(TRIM(REGEXP_EXTRACT(address_raw, r'\d{5}\s+(.+)$'))) AS city,
    TRIM(SPLIT(address_raw, ',')[SAFE_OFFSET(0)]) AS street_and_number
  FROM source
)

-- Final cleaned output
SELECT
  s.person_id,
  
  -- Dates with parse error flags
  bd.birthdate_parsed,
  CASE WHEN bd.birthdate_parsed IS NULL AND s.birthdate_raw IS NOT NULL 
       THEN TRUE ELSE FALSE END AS birthdate_parse_error,
  
  sd.insurance_sign_up_date_parsed,
  CASE WHEN sd.insurance_sign_up_date_parsed IS NULL AND s.insurance_sign_up_date_raw IS NOT NULL 
       THEN TRUE ELSE FALSE END AS insurance_sign_up_date_parse_error,
  
  -- Address components
  ap.address_raw,
  ap.address_clean,
  ap.postal_code,
  ap.city,
  ap.street_and_number,
  
  -- Standardized gender
  CASE
    WHEN UPPER(TRIM(s.gender_raw)) IN ('M', 'MALE') THEN 'male'
    WHEN UPPER(TRIM(s.gender_raw)) IN ('F', 'FEMALE') THEN 'female'
    ELSE 'unknown'
  END AS gender_std,
  
  -- Standardized family status
  CASE
    WHEN UPPER(TRIM(s.family_status_raw)) = 'SINGLE' THEN 'single'
    WHEN UPPER(TRIM(s.family_status_raw)) = 'MARRIED' THEN 'married'
    WHEN UPPER(TRIM(s.family_status_raw)) = 'DIVORCED' THEN 'divorced'
    WHEN UPPER(TRIM(s.family_status_raw)) = 'WIDOWED' THEN 'widowed'
    ELSE 'unknown'
  END AS family_status_std,
  
  -- Standardized insurance status
  CASE
    WHEN UPPER(TRIM(s.insurance_status_raw)) = 'ACTIVE' THEN 'active'
    WHEN UPPER(TRIM(s.insurance_status_raw)) = 'INACTIVE' THEN 'inactive'
    WHEN UPPER(TRIM(s.insurance_status_raw)) = 'PENDING' THEN 'pending'
    ELSE 'unknown'
  END AS insurance_status_std,
  
  -- Cleaned occupational category
  CASE 
    WHEN TRIM(s.occupational_category_raw) = '' THEN NULL
    ELSE TRIM(s.occupational_category_raw)
  END AS occupational_category_clean,
  
  -- Standardized wealth bracket
  CASE
    WHEN UPPER(TRIM(s.wealth_bracket_raw)) IN ('LOW', 'LOWER') THEN 'low'
    WHEN UPPER(TRIM(s.wealth_bracket_raw)) = 'MEDIUM' THEN 'medium'
    WHEN UPPER(TRIM(s.wealth_bracket_raw)) IN ('UPPER_MIDDLE', 'UPPER MIDDLE', 'UPPERMIDDLE') THEN 'upper_middle'
    WHEN UPPER(TRIM(s.wealth_bracket_raw)) IN ('HIGH', 'UPPER') THEN 'high'
    ELSE 'unknown'
  END AS wealth_bracket_std

FROM source s
LEFT JOIN birthdate_parsed bd USING (person_id)
LEFT JOIN signup_date_parsed sd USING (person_id)
LEFT JOIN address_parsed ap USING (person_id)
), all_values as (

    select
        insurance_status_std as value_field,
        count(*) as n_records

    from __dbt__cte__int_person_deduped
    group by insurance_status_std

)

select *
from all_values
where value_field not in (
    'active','inactive','pending','unknown'
)


