{{ config(
    materialized = 'incremental', 
    unique_key = 'column_key', 
    on_schema_change = 'sync_all_columns', 
    partition_by = {'field':'profiling_date'},
    tags =[ "profiling" ]
 )}} 


with prep as (
    select *,         
    'dummy' as partition_field
     from {{ source('mintegral_staging', 'advertiser_advanced_performance_report') }}
where 1=1 


{{ date_incremental('date', 1) }}
and date >= '2023-03-25'


),

calculation_cte as (  
     
 
     select distinct
       'date' as column_name,
       'DATE' as data_type,
        current_date() as profiling_date,
        COUNT( date) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT date) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(date IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(date IS NULL) OVER(PARTITION BY partition_field ), COUNT( date ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT date ) OVER(PARTITION BY partition_field ), COUNT( date) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as quantile_25,
        null as quantile_50,
        null as quantile_75,
        null as quantile_90,
        null as max
        
     from prep 
    union all 
   
 
     select distinct
       'campaign_id' as column_name,
       'INT64' as data_type,
        current_date() as profiling_date,
        COUNT( campaign_id) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT campaign_id) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(campaign_id IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(campaign_id IS NULL) OVER(PARTITION BY partition_field ), COUNT( campaign_id ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT campaign_id ) OVER(PARTITION BY partition_field ), COUNT( campaign_id) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(campaign_id) OVER(PARTITION BY partition_field ) as average,
        SUM(campaign_id) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (campaign_id, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (campaign_id, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (campaign_id,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (campaign_id,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (campaign_id,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (campaign_id,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (campaign_id) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    union all 
   
 
     select distinct
       'offer_id' as column_name,
       'INT64' as data_type,
        current_date() as profiling_date,
        COUNT( offer_id) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT offer_id) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(offer_id IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(offer_id IS NULL) OVER(PARTITION BY partition_field ), COUNT( offer_id ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT offer_id ) OVER(PARTITION BY partition_field ), COUNT( offer_id) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(offer_id) OVER(PARTITION BY partition_field ) as average,
        SUM(offer_id) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (offer_id, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (offer_id, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (offer_id,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (offer_id,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (offer_id,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (offer_id,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (offer_id) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    union all 
   
 
     select distinct
       'app_id' as column_name,
       'STRING' as data_type,
        current_date() as profiling_date,
        COUNT( app_id) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT app_id) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(app_id IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(app_id IS NULL) OVER(PARTITION BY partition_field ), COUNT( app_id ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT app_id ) OVER(PARTITION BY partition_field ), COUNT( app_id) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        COUNTIF( lower(app_id) in ('nan','n/a','-infinty','na','','-','__','--','_')) OVER(PARTITION BY partition_field) as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as quantile_25,
        null as quantile_50,
        null as quantile_75,
        null as quantile_90,
        null as max
        
     from prep 
    union all 
   
 
     select distinct
       'site_app_name' as column_name,
       'STRING' as data_type,
        current_date() as profiling_date,
        COUNT( site_app_name) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT site_app_name) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(site_app_name IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(site_app_name IS NULL) OVER(PARTITION BY partition_field ), COUNT( site_app_name ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT site_app_name ) OVER(PARTITION BY partition_field ), COUNT( site_app_name) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        COUNTIF( lower(site_app_name) in ('nan','n/a','-infinty','na','','-','__','--','_')) OVER(PARTITION BY partition_field) as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as quantile_25,
        null as quantile_50,
        null as quantile_75,
        null as quantile_90,
        null as max
        
     from prep 
    union all 
   
 
     select distinct
       'location' as column_name,
       'STRING' as data_type,
        current_date() as profiling_date,
        COUNT( location) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT location) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(location IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(location IS NULL) OVER(PARTITION BY partition_field ), COUNT( location ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT location ) OVER(PARTITION BY partition_field ), COUNT( location) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        COUNTIF( lower(location) in ('nan','n/a','-infinty','na','','-','__','--','_')) OVER(PARTITION BY partition_field) as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as quantile_25,
        null as quantile_50,
        null as quantile_75,
        null as quantile_90,
        null as max
        
     from prep 
    union all 
   
 
     select distinct
       'creative_id' as column_name,
       'INT64' as data_type,
        current_date() as profiling_date,
        COUNT( creative_id) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT creative_id) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(creative_id IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(creative_id IS NULL) OVER(PARTITION BY partition_field ), COUNT( creative_id ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT creative_id ) OVER(PARTITION BY partition_field ), COUNT( creative_id) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(creative_id) OVER(PARTITION BY partition_field ) as average,
        SUM(creative_id) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (creative_id, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (creative_id, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (creative_id,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (creative_id,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (creative_id,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (creative_id,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (creative_id) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    union all 
   
 
     select distinct
       'creative_name' as column_name,
       'STRING' as data_type,
        current_date() as profiling_date,
        COUNT( creative_name) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT creative_name) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(creative_name IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(creative_name IS NULL) OVER(PARTITION BY partition_field ), COUNT( creative_name ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT creative_name ) OVER(PARTITION BY partition_field ), COUNT( creative_name) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        COUNTIF( lower(creative_name) in ('nan','n/a','-infinty','na','','-','__','--','_')) OVER(PARTITION BY partition_field) as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as quantile_25,
        null as quantile_50,
        null as quantile_75,
        null as quantile_90,
        null as max
        
     from prep 
    union all 
   
 
     select distinct
       'currency' as column_name,
       'STRING' as data_type,
        current_date() as profiling_date,
        COUNT( currency) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT currency) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(currency IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(currency IS NULL) OVER(PARTITION BY partition_field ), COUNT( currency ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT currency ) OVER(PARTITION BY partition_field ), COUNT( currency) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        COUNTIF( lower(currency) in ('nan','n/a','-infinty','na','','-','__','--','_')) OVER(PARTITION BY partition_field) as count_na,
        
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as quantile_25,
        null as quantile_50,
        null as quantile_75,
        null as quantile_90,
        null as max
        
     from prep 
    union all 
   
 
     select distinct
       'impression' as column_name,
       'INT64' as data_type,
        current_date() as profiling_date,
        COUNT( impression) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT impression) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(impression IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(impression IS NULL) OVER(PARTITION BY partition_field ), COUNT( impression ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT impression ) OVER(PARTITION BY partition_field ), COUNT( impression) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(impression) OVER(PARTITION BY partition_field ) as average,
        SUM(impression) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (impression, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (impression, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (impression,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (impression,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (impression,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (impression,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (impression) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    union all 
   
 
     select distinct
       'click' as column_name,
       'INT64' as data_type,
        current_date() as profiling_date,
        COUNT( click) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT click) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(click IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(click IS NULL) OVER(PARTITION BY partition_field ), COUNT( click ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT click ) OVER(PARTITION BY partition_field ), COUNT( click) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(click) OVER(PARTITION BY partition_field ) as average,
        SUM(click) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (click, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (click, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (click,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (click,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (click,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (click,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (click) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    union all 
   
 
     select distinct
       'conversion' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( conversion) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT conversion) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(conversion IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(conversion IS NULL) OVER(PARTITION BY partition_field ), COUNT( conversion ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT conversion ) OVER(PARTITION BY partition_field ), COUNT( conversion) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(conversion) OVER(PARTITION BY partition_field ) as average,
        SUM(conversion) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (conversion, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (conversion, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (conversion,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (conversion,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (conversion,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (conversion,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (conversion) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    union all 
   
 
     select distinct
       'ecpm' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( ecpm) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT ecpm) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(ecpm IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(ecpm IS NULL) OVER(PARTITION BY partition_field ), COUNT( ecpm ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT ecpm ) OVER(PARTITION BY partition_field ), COUNT( ecpm) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(ecpm) OVER(PARTITION BY partition_field ) as average,
        SUM(ecpm) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (ecpm, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (ecpm, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (ecpm,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (ecpm,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (ecpm,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (ecpm,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (ecpm) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    union all 
   
 
     select distinct
       'cpc' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( cpc) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT cpc) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(cpc IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(cpc IS NULL) OVER(PARTITION BY partition_field ), COUNT( cpc ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT cpc ) OVER(PARTITION BY partition_field ), COUNT( cpc) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(cpc) OVER(PARTITION BY partition_field ) as average,
        SUM(cpc) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (cpc, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (cpc, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (cpc,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (cpc,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (cpc,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (cpc,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (cpc) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    union all 
   
 
     select distinct
       'ctr' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( ctr) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT ctr) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(ctr IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(ctr IS NULL) OVER(PARTITION BY partition_field ), COUNT( ctr ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT ctr ) OVER(PARTITION BY partition_field ), COUNT( ctr) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(ctr) OVER(PARTITION BY partition_field ) as average,
        SUM(ctr) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (ctr, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (ctr, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (ctr,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (ctr,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (ctr,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (ctr,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (ctr) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    union all 
   
 
     select distinct
       'cvr' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( cvr) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT cvr) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(cvr IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(cvr IS NULL) OVER(PARTITION BY partition_field ), COUNT( cvr ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT cvr ) OVER(PARTITION BY partition_field ), COUNT( cvr) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(cvr) OVER(PARTITION BY partition_field ) as average,
        SUM(cvr) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (cvr, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (cvr, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (cvr,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (cvr,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (cvr,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (cvr,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (cvr) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    union all 
   
 
     select distinct
       'ivr' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( ivr) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT ivr) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(ivr IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(ivr IS NULL) OVER(PARTITION BY partition_field ), COUNT( ivr ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT ivr ) OVER(PARTITION BY partition_field ), COUNT( ivr) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(ivr) OVER(PARTITION BY partition_field ) as average,
        SUM(ivr) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (ivr, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (ivr, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (ivr,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (ivr,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (ivr,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (ivr,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (ivr) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    union all 
   
 
     select distinct
       'spend' as column_name,
       'FLOAT64' as data_type,
        current_date() as profiling_date,
        COUNT( spend) OVER(PARTITION BY partition_field ) as count, 
        COUNT(DISTINCT spend) OVER(PARTITION BY partition_field ) as count_distinct,
        COUNTIF(spend IS NULL) OVER(PARTITION BY partition_field) as count_null,
        IEEE_DIVIDE(COUNTIF(spend IS NULL) OVER(PARTITION BY partition_field ), COUNT( spend ) OVER(PARTITION BY partition_field )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT spend ) OVER(PARTITION BY partition_field ), COUNT( spend) OVER(PARTITION BY partition_field )) as distinct_ratio, 
        null as count_na,
        
        AVG(spend) OVER(PARTITION BY partition_field ) as average,
        SUM(spend) OVER(PARTITION BY partition_field ) as sum,
        STDDEV(COALESCE (spend, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
        MIN(COALESCE (spend, 0 )) OVER (PARTITION BY partition_field)  as min,
        PERCENTILE_CONT (spend,0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT (spend,0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT (spend,0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT (spend,0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        MAX (spend) OVER (PARTITION BY partition_field)  as max
        
     from prep 
    
   
 
)
                     
select  * from calculation_cte 