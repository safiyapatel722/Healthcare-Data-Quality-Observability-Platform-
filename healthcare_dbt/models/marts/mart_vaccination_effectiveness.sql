with staging as (
    select * from {{ ref('stg_covid19') }}
),

aggregated as (
    select
        date,
        country_code,
        country_name,

        -- vaccination progress
        sum(new_persons_vaccinated)                             as total_new_vaccinated,
        sum(new_persons_fully_vaccinated)                       as total_new_fully_vaccinated,
        max(cumulative_persons_vaccinated)                      as total_cumulative_vaccinated,
        max(cumulative_persons_fully_vaccinated)                as total_cumulative_fully_vaccinated,
        sum(new_vaccine_doses_administered)                     as total_new_doses,
        max(cumulative_vaccine_doses_administered)              as total_cumulative_doses,

        -- by brand
        sum(new_doses_pfizer)                                   as total_doses_pfizer,
        sum(new_doses_moderna)                                  as total_doses_moderna,
        sum(new_doses_janssen)                                  as total_doses_janssen,

        -- outcomes during vaccination period
        sum(new_confirmed)                                      as total_new_cases,
        sum(new_deceased)                                       as total_new_deaths,

        -- effectiveness metric
        round(safe_divide(
            max(cumulative_persons_fully_vaccinated),
            nullif(max(population), 0)
        ) * 100, 2)                                             as vaccination_coverage_pct,

        max(population)                                         as population

    from staging
    group by date, country_code, country_name
)

select * from aggregated