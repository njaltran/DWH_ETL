
    
    

with all_values as (

    select
        insurance_status as value_field,
        count(*) as n_records

    from `dw-health-insurance-bipm`.`health_insurance_dev_core`.`dim_person`
    group by insurance_status

)

select *
from all_values
where value_field not in (
    'active','inactive','pending','unknown'
)


