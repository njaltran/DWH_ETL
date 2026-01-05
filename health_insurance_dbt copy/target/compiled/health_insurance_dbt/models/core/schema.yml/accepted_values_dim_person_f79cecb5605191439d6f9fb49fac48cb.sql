
    
    

with all_values as (

    select
        wealth_bracket as value_field,
        count(*) as n_records

    from `dw-health-insurance-bipm`.`health_insurance_dev_core`.`dim_person`
    group by wealth_bracket

)

select *
from all_values
where value_field not in (
    'low','medium','upper_middle','high','unknown'
)


