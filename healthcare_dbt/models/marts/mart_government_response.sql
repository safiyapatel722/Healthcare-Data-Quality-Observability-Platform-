with staging as (
    select * from {{ ref('stg_covid19') }}
),

aggregated as (
    select
        date,
        country_code,
        country_name,

        -- policy measures
        max(school_closing)                                     as school_closing,
        max(workplace_closing)                                  as workplace_closing,
        max(cancel_public_events)                               as cancel_public_events,
        max(restrictions_on_gatherings)                         as restrictions_on_gatherings,
        max(stay_at_home_requirements)                          as stay_at_home_requirements,
        max(international_travel_controls)                      as international_travel_controls,
        avg(stringency_index)                                   as avg_stringency_index,

        -- outcomes to measure policy effectiveness
        sum(new_confirmed)                                      as total_new_cases,
        sum(new_deceased)                                       as total_new_deaths,
        max(current_hospitalized_patients)                      as current_hospitalized,

        -- derived
        round(safe_divide(
            sum(new_deceased),
            nullif(sum(new_confirmed), 0)
        ) * 100, 2)                                             as case_fatality_rate,

        max(population)                                         as population,
        avg(gdp_per_capita_usd)                                 as avg_gdp_per_capita,
        avg(human_development_index)                            as avg_hdi

    from staging
    group by date, country_code, country_name
)

select * from aggregated