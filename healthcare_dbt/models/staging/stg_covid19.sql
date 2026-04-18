with source as (
    select * from {{ source('healthcare_staging', 'covid19_raw') }}
),

cleaned as (
    select
        -- identifiers
        date,
        country_code,
        country_name,
        subregion1_name,

        -- cases
        coalesce(new_confirmed, 0)                          as new_confirmed,
        coalesce(new_deceased, 0)                           as new_deceased,
        coalesce(new_recovered, 0)                          as new_recovered,
        coalesce(cumulative_confirmed, 0)                   as cumulative_confirmed,
        coalesce(cumulative_deceased, 0)                    as cumulative_deceased,

        -- hospital
        coalesce(new_hospitalized_patients, 0)              as new_hospitalized_patients,
        coalesce(current_hospitalized_patients, 0)          as current_hospitalized_patients,
        coalesce(new_intensive_care_patients, 0)            as new_intensive_care_patients,
        coalesce(current_intensive_care_patients, 0)        as current_intensive_care_patients,
        coalesce(current_ventilator_patients, 0)            as current_ventilator_patients,
        hospital_beds_per_1000,

        -- vaccination
        coalesce(new_persons_vaccinated, 0)                 as new_persons_vaccinated,
        coalesce(cumulative_persons_vaccinated, 0)          as cumulative_persons_vaccinated,
        coalesce(new_persons_fully_vaccinated, 0)           as new_persons_fully_vaccinated,
        coalesce(cumulative_persons_fully_vaccinated, 0)    as cumulative_persons_fully_vaccinated,
        coalesce(new_vaccine_doses_administered, 0)         as new_vaccine_doses_administered,
        coalesce(cumulative_vaccine_doses_administered, 0)  as cumulative_vaccine_doses_administered,
        coalesce(new_vaccine_doses_administered_pfizer, 0)  as new_doses_pfizer,
        coalesce(new_vaccine_doses_administered_moderna, 0) as new_doses_moderna,
        coalesce(new_vaccine_doses_administered_janssen, 0) as new_doses_janssen,

        -- government response
        coalesce(school_closing, 0)                         as school_closing,
        coalesce(workplace_closing, 0)                      as workplace_closing,
        coalesce(cancel_public_events, 0)                   as cancel_public_events,
        coalesce(restrictions_on_gatherings, 0)             as restrictions_on_gatherings,
        coalesce(stay_at_home_requirements, 0)              as stay_at_home_requirements,
        coalesce(international_travel_controls, 0)          as international_travel_controls,
        coalesce(stringency_index, 0)                       as stringency_index,

        -- demographics
        population,
        gdp_per_capita_usd,
        human_development_index

    from source
    where country_code is not null
    and date is not null
    and date >= '2020-01-01'
    and date <= current_date()
    and new_confirmed >= 0
    and new_deceased >= 0

)

select * from cleaned