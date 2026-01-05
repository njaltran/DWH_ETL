
    
    

with all_values as (

    select
        family_status as value_field,
        count(*) as n_records

    from `dw-health-insurance-bipm`.`health_insurance_dev_core`.`dim_person`
    group by family_status

)

select *
from all_values
where value_field not in (
    'single','married','divorced','widowed','unknown'
)


