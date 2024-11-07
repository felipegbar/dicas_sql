/* STATUS ATIVO X CHURN
*VISÃO MENSAL*
- ATIVARAM: Todos os consumers que ativaram ao longo do mês
- CHUNR: Se no último dia do mês o consumer está cancelado

Obs.: Churn é diferente de cancelados, pois muitos cancelados acabam voltando ao picpay mais ainda no mês do cancelamento
*/

with

base_ajustada as ( -- USUÁRIOS QUE ATIVARAM EM ALGUM MOMENTO
-- IMPORTANTE É TER: data_dia (cross join), consumer_id, date_contartacao, data_cancelamento, staus (ativo ou nao no data_dia)
    select 
        a.metric_date
        , a.contract_number
        , a.consumer_id
        , date(case when b.created_at > b.effective_at then b.created_at else b.effective_at end) as effective_date
        , date(date_trunc('MONTH',case when b.created_at > b.effective_at then b.created_at else b.effective_at end)) as effective_month
        , date(b.canceled_at) as canceled_date
        , date(date_trunc('MONTH',b.canceled_at)) as canceled_month
        , a.status_contract
    from validation.pf_subscription_storyline as a
    inner join self_service_analytics.pf_picpaymais_contract_consumer as b
    on a.contract_number = b.contract_number
    where 1=1 
      and a.package_title <> 'Original'
      and a.effective_at is not null -- TODOS QUE CHEGARAM A ATIVAR
      and date(b.created_at) >= '2023-01-01'
      -- and a.consumer_id = 249825480961427485

)

,base_ativaram as ( -- SAFRA
    select
        distinct consumer_id, effective_month
    from base_ajustada
    where date(date_trunc('MONTH',metric_date)) = effective_month
)

,base_churn as ( -- CHURN DO MÊS: olhar o último dia do mês e o mais atual do mês corrente pra ver se o cara estava inativo naquele dia
-- Aqui vai estar o o churn os cancelados do mês (churn do mês atual + churn de safras anteriores nesse mês)
    select 
        distinct consumer_id, canceled_month
    from base_ajustada
    where 1=1
        and status_contract = 'Inativo'
        and (metric_date = last_day(metric_date) or metric_date = current_date())
)

,base_previa as ( -- ESSE É O PASSO MAIS IMPORTANTE PARA O CÁLCULO DO COHORT
-- Esse cancelamento foi de qual safra?
select  
    a.consumer_id
    , a.effective_month -- DATA DE ATIVACAP
    , c.canceled_month -- DATA DE CANCELAMENTO (CHURN)
    , months_between(canceled_month, effective_month) as delta_meses_churn -- MESES ENTRE A AFETIVACAO E O CHURN
    , row_number()over(partition by a.consumer_id, a.effective_month order by c.canceled_month asc) as ordem_cancelamento
from base_ativaram as a
left join base_churn as c
on a.consumer_id = c.consumer_id and a.effective_month <= c.canceled_month
qualify ordem_cancelamento = 1
)

-- select
--   consumer_id
--   , effective_month                                 as mes_safra
--   , canceled_month                                  as mes_churn -- referente a safra
--   , months_between(canceled_month, effective_month) as delta_meses_churn
-- from base_previa
-- -- where consumer_id = 309119605652819572


--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.
-- AQUI É PRA TER ALGUMAS ABERTURAS DO COHORT
--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.--.

,ajuste_churn as (
select
    effective_month
    , case t.delta_meses_churn 
        when 'Churn_m0' then 'a. m0'
        when 'Churn_m1' then 'b. m1'
        when 'Churn_m2' then 'c. m2'
        when 'Churn_m3' then 'd. m3'
        when 'Churn_m4' then 'e. m4'
        when 'Churn_m5' then 'f. m5'
        when 'Churn_m6' then 'g. m6'
        when 'Churn_m7' then 'h. m7'
        when 'Churn_m8' then 'i. m8'
        when 'Churn_m9' then 'j. m9'
        when 'Churn_m10' then 'k. m10'
        when 'Churn_m11' then 'l. m11'
        when 'Permaneceu Ativo' then 'Permaneceu Ativo'
     end as delta_meses_churn
    , consumers as qtt_churn
    -- , sum(consumers)over(partition by t.effective_month order by delta_meses_churn asc rows between UNBOUNDED PRECEDING AND CURRENT ROW) as qtt_churn_acum
from (
  select 
      effective_month
      , case when delta_meses_churn is null then 'Permaneceu Ativo' else concat_ws('','Churn_m',cast(delta_meses_churn as int)) end as delta_meses_churn
      , count(distinct consumer_id) as consumers
  from base_previa
  group by 1,2
) t
where 1=1 
    -- and delta_meses_churn <> 'Permaneceu Ativo' -- Olhar só o churn
    and effective_month >= add_months(date_trunc('MONTH',current_date()),-11) -- Olhar só os últmos 12 meses current_date = 2024-11-06
)

,base_final as (
select
    a.effective_month
    , a.delta_meses_churn
    , a.qtt_churn
    , (100 * a.qtt_churn / qtt_safra) as pct_churn_safra
    , sum(case when delta_meses_churn <> 'Permaneceu Ativo' then a.qtt_churn end)over(partition by a.effective_month order by a.delta_meses_churn asc rows between UNBOUNDED PRECEDING AND CURRENT ROW) as qtt_churn_acum
    , (100 * sum(case when delta_meses_churn <> 'Permaneceu Ativo' then a.qtt_churn end)over(partition by a.effective_month order by a.delta_meses_churn asc rows between UNBOUNDED PRECEDING AND CURRENT ROW) / qtt_safra) pct_qtt_churn_acum
from ajuste_churn as a
left join (
  select effective_month, count(*) as qtt_safra 
  from base_ativaram 
  group by 1
) as b
on a.effective_month = b.effective_month
-- order by 1 desc, 2
)

select 
    effective_month as Safra
    , delta_meses_churn as `Mês Churn`
    , qtt_churn as Clientes
    , pct_churn_safra as `Clientes (%)`
    , case when qtt_churn_acum is null then qtt_churn else qtt_churn_acum end as `Clientes ACUM`
    , case when pct_qtt_churn_acum is null then pct_churn_safra else pct_qtt_churn_acum end as `Clientes ACUM (%)`
from base_final
order by 1 desc, 2
