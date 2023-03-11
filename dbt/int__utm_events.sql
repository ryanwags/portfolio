-- extract UTMs from event records; filter out records where user landed from same UTM source within 24 hours of last occurrence.

{{ 
    config(
        materialized='incremental',
        unique_key = ['event_id']
    )
}}

with 
    -- fetch UTM events with 24-hour lookback window
    events as (
        select
            e.event_id,
            -- if user has been identified, swap in stitched ID; else keep anonymous ID
            coalesce(s.user_id, e.visitor_id) as stitched_id, 
            e.event_time_utc,
            e.referrer_url_querystring,
        from {{ ref('stg__events') }} e
        left join {{ ref('visitor_stitching') }} s
            on e.visitor_id = s.visitor_id
        where 
            e.referrer_url_querystring like '%utm_%' 
            {% if is_incremental() %}
                and e.event_time_utc > ( select date_add(max(event_time_utc), interval -1 day) from {{ this }} )
            {% endif %}
    ),

    -- extract UTM params from querystring
    extract_utms as (
        select 
            event_id,
            stitched_id,
            event_time_utc,
            -- referrer_url_querystring, -- for dev
            {{ dbt_utils.get_url_parameter(field='url', url_parameter='utm_source') }} as utm_source,
            {{ dbt_utils.get_url_parameter(field='url', url_parameter='utm_medium') }} as utm_medium,
            {{ dbt_utils.get_url_parameter(field='url', url_parameter='utm_campaign') }} as utm_campaign,
            {{ dbt_utils.get_url_parameter(field='url', url_parameter='utm_content') }} as utm_content,
            {{ dbt_utils.get_url_parameter(field='url', url_parameter='utm_term') }} as utm_term
        from events
    ),

    -- flag dupe events (user hit from same UTM source within 24 hours)
    flag_same_session as (
        select 
            *,
            -- lag source, flag if <24 hours, convert bool to int (1=same session), coalesce nulls to 0.
            coalesce(
                (datediff('hours', 
                          lag(event_time_utc, 1) over (partition by stitched_id, utm_source order by event_time), 
                          event_time)::float < 24)::int,
                 0) as is_same_session
        from extract_utms 
    ),

    -- select de-duped records
    final as (
        select  
            event_id,
            stitched_id,
            event_time_utc,
            -- referrer_url_full_url, -- for dev
            utm_source,
            utm_campaign,
            utm_medium,
            utm_content,
            utm_term
        from flag_same_session 
        where not is_same_session 
    )

select * from final