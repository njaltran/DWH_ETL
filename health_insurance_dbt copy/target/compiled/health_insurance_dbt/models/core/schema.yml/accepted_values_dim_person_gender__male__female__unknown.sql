
    
    

with all_values as (

    select
        gender as value_field,
        count(*) as n_records

    from `dw-health-insurance-bipm`.`health_insurance_dev_core`.`dim_person`
    group by gender

)

select *
from all_values
where value_field not in (
    'male','female','unknown'
)


