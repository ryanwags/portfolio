{{ 
    config(
        materialized='table'
    )
}}

{% set sources = ['closing_clear','evening_buzz','facebook','google','instagram','microsoft_ads',
                  'millennial_matters','sales_forward','tiktok','twitter','yahoo','youtube'] %}

with 
    -- stack all spend staging models
    spend_stacked as (
        {% for src in sources %}
            select
                month,
                '{{src}}' as source_shortname,
                round(spend, 2) as spend 
            from {{ source('spend', 'spend_' + src) }}
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    ),

    -- append clean source and channel labels
    join_labels as (
        select 
            s.month,
            -- s.source_shortname,
            m.label as source,
            m.channel as channel,
            cast(s.spend as float64) as spend
        from spend_stacked s
        left join {{ ref('source_channel_mappings') }} m 
            on s.source_shortname = m.short_name
    )

select * from join_labels