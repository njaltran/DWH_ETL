
    
    

with dbt_test__target as (

  select person_id as unique_field
  from `dw-health-insurance-bipm`.`health_insurance_dev_marts`.`mart_person_kpis`
  where person_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


