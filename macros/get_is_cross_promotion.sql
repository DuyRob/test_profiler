-- copying the get_is_cross_promotion.sql code, to refactor later

{% macro get_is_cross_promotion(media_source, campaign, site_id) -%}
    (media_source in ('ironsource_int', 'mintegral_int') and (campaign like '%_cp' or campaign like '%_cp- optimizer'))
    or
    (lower(media_source) like '%cross%promo%')
    or (
    case when media_source = 'ironsource_int' then regexp_extract(site_id, '^\\d+_(.*)') else site_id end

    in (
      select distinct title_name from `amanotes-analytics.raw_ironsource_titles.ironsource_titles` where title_name is not null
      union all
      select distinct network_app_name from `amanotes-analytics.raw_cost_by_batch.ironsource_cost*` where network_app_name is not null
      union all
      select distinct case when media_source = 'ironsource_int' then app_name else site_id end
      from `amanotes-analytics.performance_monitoring.dim_internal_site_id`
      where case when media_source = 'ironsource_int' then app_name else site_id end is not null
    )
  )
{%- endmacro %}