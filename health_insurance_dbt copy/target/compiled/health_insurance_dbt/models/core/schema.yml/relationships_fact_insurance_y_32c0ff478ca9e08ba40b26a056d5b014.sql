
    
    

with child as (
    select person_id as from_field
    from `dw-health-insurance-bipm`.`health_insurance_dev_core`.`fact_insurance_yearly`
    where person_id is not null
),

parent as (
    select person_id as to_field
    from `dw-health-insurance-bipm`.`health_insurance_dev_core`.`dim_person`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


