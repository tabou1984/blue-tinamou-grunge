#Query for the first exercise.

query_q1 = """
select fullVisitorId, totals, device
from bigquery-public-data.google_analytics_sample.ga_sessions_{}
"""

#Query for the secord exercice
 
query_q2 ="""
with a as
    (
    select fullVisitorId, transaction.transactionId, transaction.transactionRevenue, hour, minute, hitNumber 
    from
      (
      SELECT fullVisitorId, h.transaction as transaction, h.hour, h.minute, h.hitNumber
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_{}`, UNNEST(hits) as h
      )
    )


select d.fullVisitorId
,TIMESTAMP(concat(CAST(PARSE_DATE("%Y%m%d", "{}") as string)," ",CAST(d.hour as string),":",CAST(d.minute as string),":","00")) as start_timestamp
,TIMESTAMP(concat(CAST(PARSE_DATE("%Y%m%d", "{}") as string)," ",CAST(c.convert_hour as string),":",CAST(c.convert_minute as string),":","00")) as convert_timestamp
from
(
select fullVisitorId, transaction.transactionId, transaction.transactionRevenue, hour, minute, hitNumber 
from
  (
    SELECT fullVisitorId, h.transaction as transaction, h.hour, h.minute, h.hitNumber
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_{}`, UNNEST(hits) as h
  )
  where hitNumber = 1
) d
 left join
  (   
  select a.fullVisitorId, a.hour as convert_hour, a.minute as convert_minute
    from a
    inner join
    (
    select fullVisitorId, min(hitNumber) as min_hit_num
    from a
    where transactionRevenue is not null
    group by fullVisitorId
    ) b 
    on a.fullVisitorId = b.fullVisitorId and a.hitNumber = b.min_hit_num
    where a.transactionRevenue is not null
  ) c 
  on c.fullVisitorId = d.fullVisitorId
"""

#Second (more thorough) query for first exercice. Used for testing purposes with pandas_gbq library
#(not complete with cases etc)
query_q1_2 = """
select sum(a.transactions) as transactions, 
sum(a.visits) as visits,
a.devicecategory,
a.user_type
from
(
  select fullVisitorId, totals.visits as visits, totals.transactions as transactions, device.devicecategory,
  case
  when fullVisitorId in 
  (
      select fullVisitorId
      from bigquery-public-data.google_analytics_sample.ga_sessions_20170731
      group by fullVisitorId 
      having sum(totals.visits) > 1
  ) then 'returning'
  else 'new'
  end as user_type
  from bigquery-public-data.google_analytics_sample.ga_sessions_20170731
) a
group by a.devicecategory, a.user_type
"""