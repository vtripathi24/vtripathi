with session_views as (
select distinct
    dsti.event_date,
    dsti.variant,
    dsti.session_id
from integrated_events.diner_session_topics_impressions dsti
left join integrated_events.diner_session_topics dst
    on lower(dst.session_id) = lower(dsti.session_id)
where dsti.event_date between date '{{start_date}}' and date '{{end_date}}'
    and dsti.impression_visible = true
    and dsti.page_name in ('homepage logged in','homepage logged out','home restaurant list')
    and dsti.brand in ('grubhub','seamless')
    and dsti.topic_name not in ('cuisine ribbon')
    and dsti.app in ('Android Native')
    and dsti.is_campus = false
    and dsti.flow_name in ('{{flow_name1}}','{{flow_name2}}')
    --and dsti.flow_name in ('Topics-return-android','Topics-new-android')
    and dsti.routing_group_name in ('{{rtng_grp_name1}}','{{rtng_grp_name2}}','{{rtng_grp_name3}}','{{rtng_grp_name4}}','{{rtng_grp_name5}}','{{rtng_grp_name6}}')
    --and dsti.routing_group_name in ('android-variant-1','android-variant-1-randinsert','android-variant-3','android-variant-3-randinsert','global-control','global-control-randinsert')
    and json_format(cast(dst.variant_list as JSON)) not like '%random_rank%'
)

, session_clicks as
(
select distinct
    dsti.event_date,
    dsti.variant,
    dsti.session_id
from integrated_events.diner_session_topics_impressions dsti
left join integrated_events.diner_session_topics dst
    on lower(dst.session_id) = lower(dsti.session_id)
where dsti.event_date between date '{{start_date}}' and date '{{end_date}}'
    and dsti.impression_visible = true
    and dsti.impression_clicked = true
    and dsti.page_name in ('homepage logged in','homepage logged out','home restaurant list')
    and dsti.brand in ('grubhub','seamless')
    and dsti.topic_name not in ('cuisine ribbon')
    and dsti.app in ('Android Native')
    and dsti.is_campus = false
    and dsti.flow_name in ('{{flow_name1}}','{{flow_name2}}')
    --and dsti.flow_name in ('Topics-return-android','Topics-new-android')
    and dsti.routing_group_name in ('{{rtng_grp_name1}}','{{rtng_grp_name2}}','{{rtng_grp_name3}}','{{rtng_grp_name4}}','{{rtng_grp_name5}}','{{rtng_grp_name6}}')
    --and dsti.routing_group_name in ('android-variant-1','android-variant-1-randinsert','android-variant-3','android-variant-3-randinsert','global-control','global-control-randinsert')
    and json_format(cast(dst.variant_list as JSON)) not like '%random_rank%'
),


orders as (
select distinct
    variant,
    topic_name,
    topic_id,
    session_id,
    browser_id,
    session_converted,
    lc_attributed_order_uuid
from integrated_events.diner_session_topics
    cross join unnest (lc_attributed_order_uuids) as t(lc_attributed_order_uuid)
where event_date between date '{{start_date}}' and date '{{end_date}}'
    and brand in ('grubhub','seamless')
    and page_name in ('homepage logged in','homepage logged out','home restaurant list')
    and app in ('Android Native')
    and is_campus = false
    and lc_attributed_order_uuid is not null
    and topic_name not in ('cuisine ribbon')
    and dsti.flow_name in ('{{flow_name1}}','{{flow_name2}}')
    --and dsti.flow_name in ('Topics-return-android','Topics-new-android')
    and dsti.routing_group_name in ('{{rtng_grp_name1}}','{{rtng_grp_name2}}','{{rtng_grp_name3}}','{{rtng_grp_name4}}','{{rtng_grp_name5}}','{{rtng_grp_name6}}')
    --and dsti.routing_group_name in ('android-variant-1','android-variant-1-randinsert','android-variant-3','android-variant-3-randinsert','global-control','global-control-randinsert')
    and json_format(cast(variant_list as JSON)) not like '%random_rank%'
    
)

select
    --event_date,
    case when variant in ('Topics-return-android_global-control','Topics-new-android_global-control',
                          'Topics-return-android_global-control-randinsert','Topics-new-android_global-control-randinsert') then 'Control'
    else 'Test' end as variant,
    count(distinct session_id) as views,
    cast(avg(sessions_clicked) as decimal(10,5)) as avg_ctr,
        cast(stddev(sessions_clicked) as decimal(10,5)) as stddev_ctr,
        sum(sessions_converted) as sessions_converted,
        sum(sessions_clicked) as sessions_clicked,
    cast(avg(sessions_converted) as decimal(10,5)) as avg_cvr,
    cast(stddev(sessions_converted) as decimal(10,5)) as stddev_cvr
from
(
    select distinct
        v.event_date,
        v.variant,
        v.session_id,
        count(distinct case when c.session_id is not null then v.session_id end) as sessions_clicked,
        count(distinct case when o.lc_attributed_order_uuid is not null then v.session_id end) as sessions_converted
    from session_views v
    left join session_clicks c
    on lower(v.session_id) = lower(c.session_id)
    left join orders o
    on lower(v.session_id) = lower(o.session_id)
    group by 1,2,3
)
where variant is not null
group by 1
order by 1
