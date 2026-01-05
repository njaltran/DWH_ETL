

  create or replace view `dw-health-insurance-bipm`.`health_insurance_dev_staging`.`stg_person_dim_raw`
  OPTIONS()
  as 

-- Light cleaning and selection of raw person dimension data
-- No deduplication at this stage

SELECT
  TRIM(Person_id) AS person_id,
  TRIM(birthdate) AS birthdate_raw,
  TRIM(address) AS address_raw,
  TRIM(gender) AS gender_raw,
  TRIM(family_status) AS family_status_raw,
  TRIM(insurance_status) AS insurance_status_raw,
  TRIM(insurance_sign_up_date) AS insurance_sign_up_date_raw,
  TRIM(occupational_category) AS occupational_category_raw,
  TRIM(wealth_bracket) AS wealth_bracket_raw
FROM `dw-health-insurance-bipm`.`health_insurance_raw`.`health_insurance_person_dim_raw`
WHERE Person_id IS NOT NULL;

