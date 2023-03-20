-- copying the get_is_cross_promotion.sql code, to refactor later

{% macro get_agency_from_campaign_name_and_default_rules(media_source, campaign, network_app_id) -%}
    case
      when lower(campaign) like '%mobvista%' then 'mobvista'
      when lower(campaign) like '%yeahmobi%' then 'yeahmobi'
      when lower(campaign) like '%funloop%' then 'funloop'
      when media_source in ('bytedance_int', 'bytedanceglobal_int') and lower(campaign) not like 'amanotes_%' then 'mobvista'
      when media_source = 'tencent_int' and  network_app_id in ('16944790', '16934058') then 'yeahmobi'
    end
{%- endmacro %}