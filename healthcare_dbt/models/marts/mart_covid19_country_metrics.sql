with staging as (
    select * from {{ ref('stg_covid19') }}
),

aggregated as (
    select
        country_code,
        country_name,
        date,
        sum(new_confirmed)                          as total_new_cases,
        sum(new_deceased)                           as total_new_deaths,
        sum(new_recovered)                          as total_new_recovered,
        max(cumulative_confirmed)                   as total_cumulative_cases,
        max(cumulative_deceased)                    as total_cumulative_deaths,
        max(population)                             as population,
        round(
            safe_divide(
                sum(new_deceased),
                nullif(sum(new_confirmed),0)
            ) * 100, 2
        )                                           as case_fatality_rate
    from staging
    group by country_code, country_name, date
)

select * from aggregated