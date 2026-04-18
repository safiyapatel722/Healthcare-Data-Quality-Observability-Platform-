with staging as (
    select * from {{ ref('stg_covid19') }}
),

aggregated as (
    select
        date,
        country_code,
        country_name,

        -- case metrics
        sum(new_confirmed)                                      as total_new_cases,
        sum(new_deceased)                                       as total_new_deaths,
        sum(new_recovered)                                      as total_new_recovered,
        max(cumulative_confirmed)                               as total_cumulative_cases,
        max(cumulative_deceased)                                as total_cumulative_deaths,

        -- hospital pressure
        sum(new_hospitalized_patients)                         as total_new_hospitalized,
        max(current_hospitalized_patients)                     as current_hospitalized,
        max(current_intensive_care_patients)                   as current_icu_patients,
        max(current_ventilator_patients)                       as current_ventilator_patients,
        avg(hospital_beds_per_1000)                            as avg_hospital_beds_per_1000,

        -- derived metrics
        round(least(safe_divide(sum(new_deceased), nullif(sum(new_confirmed), 0)) * 100, 100), 2) as case_fatality_rate,

        round(safe_divide(
            max(current_hospitalized_patients),
            nullif(max(cumulative_confirmed), 0)
        ) * 100, 2)                                            as hospitalization_rate,

        max(population)                                        as population

    from staging
    group by date, country_code, country_name
)

select * from aggregated