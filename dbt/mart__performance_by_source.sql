-- performance by month x first touch source
-- all ctes joined on MD5 surrogate key constructed from month x source

with 
    -- all months in analysis range
    scaffold as (
        {{ 
            dbt_utils.date_spine(
                datepart = "month",
                start_date = "cast('2018-01-01' as date)",
                end_date = "date_trunc(current_date, month)"
            )
        }}
    ),

    -- make (month x source) scaffold via cartesian join 
    source_scaffold as (
        select 
            {{ dbt_utils.generate_surrogate_key(['cast(s.date_month as date)', 'm.label']) }} as key,
            cast(s.date_month as date) as month,
            m.label as source
        from scaffold s
        left join (select distinct label from {{ ref('source_channel_mappings') }}) m on 1=1
    ),

    -- spend by month x source
    spend_agg as (
        select 
            {{ dbt_utils.generate_surrogate_key(['s.month', 's.source']) }} as key,
            s.month,
            s.source,
            sum(s.spend) as spend 
        from {{ ref('spend') }} s
        group by 
            key,
            s.month,
            s.source
    ),

    -- count of registrations by month x source
    registrations_agg as (
        select
            {{ dbt_utils.generate_surrogate_key(['date_trunc(registered_at_utc, month)', 'first_touch_source']) }} as key,
            date_trunc(registered_at_utc, month) as month,
            first_touch_source as source,
            count(user_id) as n_registrations
        from {{ ref('stg_rds__users') }}
        group by 
            key,
            date_trunc(registered_at_utc, month),
            first_touch_source
    ),

    -- flag first time orders; append user's first touch UTM source
    orders as (
        select
            o.user_id,
            o.order_total,
            o.status,
            date_trunc(o.submitted_at_utc, month) as month,
            o.submitted_at_utc = min(case when o.status = 'received' then o.submitted_at_utc end) over (partition by o.user_id) as is_first_order,
            u.first_touch_source as source
        from {{ ref('stg_rds__orders') }} o
        left join {{ ref('stg_rds__users') }} u 
            on o.user_id = u.user_id 
    ),

    -- summarize new/repeat/failed orders by month x source
    orders_agg as (
        select 
            {{ dbt_utils.generate_surrogate_key(['month', 'source']) }} as key,
            month,
            source,
            -- activity from new customers
            sum(case when is_first_order then order_total end) as amt_new_orders,
            count(case when is_first_order then user_id end) as n_new_orders,
            -- activity from repeat customers
            sum(case when not is_first_order and status = 'received' then order_total end) as amt_repeat_orders,
            count(case when not is_first_order and status = 'received' then user_id end) as n_repeat_orders,            
            -- summary of failed orders
            sum(case when status = 'failed' then order_total end) as amt_failed_orders,
            count(case when status = 'failed' then user_id end) as n_failed_orders
        from orders 
        group by 
            key,
            month,
            source 
    ),

    -- join all ctes, compute KPIs
    final as (
        select
            sc.key,
            sc.month,
            sc.source,
            sp.spend,
            coalesce(r.n_registrations, 0) as n_registrations,
            -- counts of orders by order status
            coalesce(o.n_new_orders, 0) as n_new_orders,
            coalesce(o.n_repeat_orders, 0) as n_repeat_orders,
            coalesce(o.n_failed_orders, 0) as n_failed_orders,
            -- order revenue by order status
            coalesce(round(o.amt_new_orders, 2), 0) as amt_new_orders,
            coalesce(round(o.amt_repeat_orders, 2), 0) as amt_repeat_orders,
            coalesce(round(o.amt_failed_orders, 2), 0) as amt_failed_orders,
            -- conversion rates, cost per conversions
            least(safe_divide(o.n_new_orders, r.n_registrations), 1) as cvr_reg_customer, -- 'registration to acquisition' conversion rate, limit to 100%
            safe_divide(sp.spend, r.n_registrations) as cost_per_reg, -- cost per registration
            safe_divide(sp.spend, o.n_new_orders) as cac -- cost per customer
        from source_scaffold sc
        left join spend_agg sp 
            on sc.key = sp.key
        left join registrations_agg r 
            on sc.key = r.key 
        left join orders_agg o
            on sc.key = o.key    
    )

select * from final
order by 
    month, 
    source
