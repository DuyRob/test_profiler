{{ config(
    materialized = 'incremental', 
    unique_key = 'column_key', 
    on_schema_change = 'sync_all_columns', 
    partition_by = {'field':'profiling_date'},
    tags =[ "profiling" ]
 )}} 


with prep as (
    select *
     from {{ source('mintegral_staging', 'advertiser_advanced_performance_report') }}
where 1=1 


{{ date_incremental('date', 1) }}

),

calculation_cte as (  
     
 
     select
       'date' as column_name,
       'DATE' as data_type,
        current_date() as profiling_date,
        COUNT( date) as count, 
        COUNT(DISTINCT date) as count_distinct,
        COUNTIF(date IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(date IS NULL), COUNT( date )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT date ), COUNT( date)) as distinct_ratio, 
        null as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'campaign_id' as column_name,
       'INT64' as data_type,
        current_date() as profiling_date,
        COUNT( campaign_id) as count, 
        COUNT(DISTINCT campaign_id) as count_distinct,
        COUNTIF(campaign_id IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(campaign_id IS NULL), COUNT( campaign_id )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT campaign_id ), COUNT( campaign_id)) as distinct_ratio, 
        null as count_na,
        
        AVG(campaign_id) as average,
        SUM(campaign_id) as sum,
        STDDEV(campaign_id) as  standard_dev,
        MIN(campaign_id)   as min,
        MAX (campaign_id)  as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'offer_id' as column_name,
       'INT64' as data_type,
        current_date() as profiling_date,
        COUNT( offer_id) as count, 
        COUNT(DISTINCT offer_id) as count_distinct,
        COUNTIF(offer_id IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(offer_id IS NULL), COUNT( offer_id )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT offer_id ), COUNT( offer_id)) as distinct_ratio, 
        null as count_na,
        
        AVG(offer_id) as average,
        SUM(offer_id) as sum,
        STDDEV(offer_id) as  standard_dev,
        MIN(offer_id)   as min,
        MAX (offer_id)  as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'app_id' as column_name,
       'STRING' as data_type,
        current_date() as profiling_date,
        COUNT( app_id) as count, 
        COUNT(DISTINCT app_id) as count_distinct,
        COUNTIF(app_id IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(app_id IS NULL), COUNT( app_id )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT app_id ), COUNT( app_id)) as distinct_ratio, 
        COUNTIF( lower(app_id) in ('nan','n/a','-infinty','na','','-','__','--','_')) as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'site_app_name' as column_name,
       'STRING' as data_type,
        current_date() as profiling_date,
        COUNT( site_app_name) as count, 
        COUNT(DISTINCT site_app_name) as count_distinct,
        COUNTIF(site_app_name IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(site_app_name IS NULL), COUNT( site_app_name )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT site_app_name ), COUNT( site_app_name)) as distinct_ratio, 
        COUNTIF( lower(site_app_name) in ('nan','n/a','-infinty','na','','-','__','--','_')) as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'location' as column_name,
       'STRING' as data_type,
        current_date() as profiling_date,
        COUNT( location) as count, 
        COUNT(DISTINCT location) as count_distinct,
        COUNTIF(location IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(location IS NULL), COUNT( location )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT location ), COUNT( location)) as distinct_ratio, 
        COUNTIF( lower(location) in ('nan','n/a','-infinty','na','','-','__','--','_')) as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'creative_id' as column_name,
       'INT64' as data_type,
        current_date() as profiling_date,
        COUNT( creative_id) as count, 
        COUNT(DISTINCT creative_id) as count_distinct,
        COUNTIF(creative_id IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(creative_id IS NULL), COUNT( creative_id )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT creative_id ), COUNT( creative_id)) as distinct_ratio, 
        null as count_na,
        
        AVG(creative_id) as average,
        SUM(creative_id) as sum,
        STDDEV(creative_id) as  standard_dev,
        MIN(creative_id)   as min,
        MAX (creative_id)  as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'creative_name' as column_name,
       'STRING' as data_type,
        current_date() as profiling_date,
        COUNT( creative_name) as count, 
        COUNT(DISTINCT creative_name) as count_distinct,
        COUNTIF(creative_name IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(creative_name IS NULL), COUNT( creative_name )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT creative_name ), COUNT( creative_name)) as distinct_ratio, 
        COUNTIF( lower(creative_name) in ('nan','n/a','-infinty','na','','-','__','--','_')) as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'currency' as column_name,
       'STRING' as data_type,
        current_date() as profiling_date,
        COUNT( currency) as count, 
        COUNT(DISTINCT currency) as count_distinct,
        COUNTIF(currency IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(currency IS NULL), COUNT( currency )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT currency ), COUNT( currency)) as distinct_ratio, 
        COUNTIF( lower(currency) in ('nan','n/a','-infinty','na','','-','__','--','_')) as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'impression' as column_name,
       'INT64' as data_type,
        current_date() as profiling_date,
        COUNT( impression) as count, 
        COUNT(DISTINCT impression) as count_distinct,
        COUNTIF(impression IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(impression IS NULL), COUNT( impression )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT impression ), COUNT( impression)) as distinct_ratio, 
        null as count_na,
        
        AVG(impression) as average,
        SUM(impression) as sum,
        STDDEV(impression) as  standard_dev,
        MIN(impression)   as min,
        MAX (impression)  as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'click' as column_name,
       'INT64' as data_type,
        current_date() as profiling_date,
        COUNT( click) as count, 
        COUNT(DISTINCT click) as count_distinct,
        COUNTIF(click IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(click IS NULL), COUNT( click )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT click ), COUNT( click)) as distinct_ratio, 
        null as count_na,
        
        AVG(click) as average,
        SUM(click) as sum,
        STDDEV(click) as  standard_dev,
        MIN(click)   as min,
        MAX (click)  as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'conversion' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( conversion) as count, 
        COUNT(DISTINCT conversion) as count_distinct,
        COUNTIF(conversion IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(conversion IS NULL), COUNT( conversion )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT conversion ), COUNT( conversion)) as distinct_ratio, 
        null as count_na,
        
        AVG(conversion) as average,
        SUM(conversion) as sum,
        STDDEV(conversion) as  standard_dev,
        MIN(conversion)   as min,
        MAX (conversion)  as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'ecpm' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( ecpm) as count, 
        COUNT(DISTINCT ecpm) as count_distinct,
        COUNTIF(ecpm IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(ecpm IS NULL), COUNT( ecpm )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT ecpm ), COUNT( ecpm)) as distinct_ratio, 
        null as count_na,
        
        AVG(ecpm) as average,
        SUM(ecpm) as sum,
        STDDEV(ecpm) as  standard_dev,
        MIN(ecpm)   as min,
        MAX (ecpm)  as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'cpc' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( cpc) as count, 
        COUNT(DISTINCT cpc) as count_distinct,
        COUNTIF(cpc IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(cpc IS NULL), COUNT( cpc )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT cpc ), COUNT( cpc)) as distinct_ratio, 
        null as count_na,
        
        AVG(cpc) as average,
        SUM(cpc) as sum,
        STDDEV(cpc) as  standard_dev,
        MIN(cpc)   as min,
        MAX (cpc)  as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'ctr' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( ctr) as count, 
        COUNT(DISTINCT ctr) as count_distinct,
        COUNTIF(ctr IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(ctr IS NULL), COUNT( ctr )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT ctr ), COUNT( ctr)) as distinct_ratio, 
        null as count_na,
        
        AVG(ctr) as average,
        SUM(ctr) as sum,
        STDDEV(ctr) as  standard_dev,
        MIN(ctr)   as min,
        MAX (ctr)  as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'cvr' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( cvr) as count, 
        COUNT(DISTINCT cvr) as count_distinct,
        COUNTIF(cvr IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(cvr IS NULL), COUNT( cvr )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT cvr ), COUNT( cvr)) as distinct_ratio, 
        null as count_na,
        
        AVG(cvr) as average,
        SUM(cvr) as sum,
        STDDEV(cvr) as  standard_dev,
        MIN(cvr)   as min,
        MAX (cvr)  as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'ivr' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( ivr) as count, 
        COUNT(DISTINCT ivr) as count_distinct,
        COUNTIF(ivr IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(ivr IS NULL), COUNT( ivr )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT ivr ), COUNT( ivr)) as distinct_ratio, 
        null as count_na,
        
        AVG(ivr) as average,
        SUM(ivr) as sum,
        STDDEV(ivr) as  standard_dev,
        MIN(ivr)   as min,
        MAX (ivr)  as max
        
     from prep 
     group by 1,2,3
    union all  
 
     select
       'spend' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( spend) as count, 
        COUNT(DISTINCT spend) as count_distinct,
        COUNTIF(spend IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF(spend IS NULL), COUNT( spend )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT spend ), COUNT( spend)) as distinct_ratio, 
        null as count_na,
        
        AVG(spend) as average,
        SUM(spend) as sum,
        STDDEV(spend) as  standard_dev,
        MIN(spend)   as min,
        MAX (spend)  as max
        
     from prep 
     group by 1,2,3
     
 
)
                     
select *,
{{  dbt_utils.surrogate_key (['column_name','data_type','profiling_date'])  }}  as column_key 
from calculation_cte   
