select 
  case 
    when date_format(metric_date, 'y-MM-01') >= '2023-07-01' then cast(date_format(metric_date, 'y-MM-01') as string) 
    else '< 2023-07-01' end as `Data Produção`
  ,case 
    when date_format(effective_at, 'y-MM-01') >= '2023-07-01' then cast(date_format(effective_at, 'y-MM-01') as string) 
    else '< 2023-07-01' end as `Data Safra` 
  ,count(*) as Consumers
from self_service_analytics.pf_subscription_storyline as a
inner join shared.calendar as b
on a.metric_date = b.calendar_date
where (is_month_end = true or metric_date = current_date()) -- = date_add(current_date(), -1)) -- último status do mês
  and status_contract = 'Ativo' 
  and package_title <> 'Original'
group by 1,2
