# Databricks notebook source
# MAGIC %md
# MAGIC ## self_service_analytics.pf_subscription_storyline
# MAGIC #####jira tasks: 
# MAGIC <nav>
# MAGIC NA
# MAGIC </nav>
# MAGIC
# MAGIC #####last authors: <nav>
# MAGIC @FelipeBarreto
# MAGIC
# MAGIC </nav>
# MAGIC
# MAGIC #####last update:
# MAGIC 23/09/2024

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base de cancelados
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Ordenar status de atualização
# MAGIC %sql
# MAGIC
# MAGIC create or replace temp view sql_base_cancelados_ordem as 
# MAGIC
# MAGIC select
# MAGIC   consumer_id 
# MAGIC   ,contract_number
# MAGIC   ,package_title
# MAGIC   ,history_description
# MAGIC   ,history_status
# MAGIC   ,dateadd(HOUR,-3,status_history_at) as status_history_at
# MAGIC   ,row_number()over(
# MAGIC         partition by contract_number 
# MAGIC         order by status_history_at DESC, 
# MAGIC                  (case  
# MAGIC                     when history_status = 'PRE_SUBSCRIPTION' then 0 
# MAGIC                     when history_status = 'SUBSCRIBING'      then 1
# MAGIC                     when history_status = 'SUBSCRIBED' and ((lower(history_description) like '%reabilit%') or (lower(history_description) like '%reativa%')  or (history_description = 'Reaiblitado')) then 5
# MAGIC                     when history_status = 'SUBSCRIBED' and ((lower(history_description) not like '%reabilit%') or (lower(history_description) not like '%reativa%')  or (history_description != 'Reaiblitado')) then 2
# MAGIC                     when history_status = 'DISCONTINUED'     then 3
# MAGIC                     when history_status = 'CANCELED'         then 4
# MAGIC                     end) desc) as ordem_status
# MAGIC from subscriptions.premium_subscriptions_status_history
# MAGIC where 1=1
# MAGIC   and (lower(package_title) in ('picpay mais','picpay +','picpay+','picpay premium') or lower(package_title) = 'original')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classificação dos pacotes
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace temp view sql_base_class_pacotes as 
# MAGIC             
# MAGIC -- CLASSIFICAÇÃO DE PACOTES PP+ | ORIGINAL
# MAGIC SELECT 
# MAGIC   package_id, 
# MAGIC   CASE 
# MAGIC     WHEN lower(package_title) IN ('picpay mais','picpay +','picpay+','picpay premium') THEN 'ppmais'
# MAGIC     WHEN lower(package_title) = 'original' THEN 'original' 
# MAGIC     END AS package_type,
# MAGIC   package_title,
# MAGIC   ROW_NUMBER() OVER(PARTITION BY package_id ORDER BY (CASE lower(package_title)
# MAGIC                                                           WHEN 'picpay mais'    THEN 1 
# MAGIC                                                           WHEN 'picpay +'       THEN 2 
# MAGIC                                                           WHEN 'picpay+'        THEN 2 
# MAGIC                                                           WHEN 'picpay premium' THEN 2 
# MAGIC                                                           ELSE 3 END) ) AS ordem
# MAGIC FROM subscriptions.premium_subscriptions
# MAGIC WHERE (LOWER(package_title) IN ('picpay mais','picpay +','picpay+','picpay premium') OR lower(package_title) = 'original')
# MAGIC QUALIFY ordem = 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pegando data de cancelamento
# MAGIC ---
# MAGIC
# MAGIC Necessário ajustar a data de efetivação (migração do original)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace temp view sql_base_consumers as 
# MAGIC
# MAGIC select 
# MAGIC   a.consumer_id as consumer_id
# MAGIC   -- ,case when b.history_description = 'PORTABILITY : Original -> PicPay Mais' then a.created_at else a.effective_at end as effective_at
# MAGIC   ,case when created_at > effective_at then created_at else effective_at end as effective_at -- OBS!!! HÁ CASOS ASSIM! VALIDAR COM O PRODUTO (VAI ALÉM DE PORTABILIDADE NA DESCRICAO)
# MAGIC   ,case when a.subscription_status in ('CANCELED') then b.status_history_at end as canceled_at -- data de cancelamento
# MAGIC
# MAGIC   ,case 
# MAGIC     when b.package_title = 'Original' and b.history_description = 'PORTABILITY : Original -> PicPay Mais' and b.history_status = 'DISCONTINUED' then a.updated_at
# MAGIC     when a.subscription_status in ('CANCELED') then b.status_history_at 
# MAGIC   end as adjusted_canceled_at -- data de cancelamento EX. consumer_id = 130385090663295605
# MAGIC
# MAGIC   ,a.contract_number
# MAGIC   ,c.package_id
# MAGIC   ,c.package_title
# MAGIC   ,c.package_type
# MAGIC   ,a.subscription_status
# MAGIC   ,a.created_at 
# MAGIC
# MAGIC from subscriptions.premium_subscriptions as a
# MAGIC inner join sql_base_cancelados_ordem as b
# MAGIC   on a.contract_number = b.contract_number and ordem_status = 1
# MAGIC left join sql_base_class_pacotes as c
# MAGIC   on a.package_id = c.package_id
# MAGIC where 1=1
# MAGIC   and (lower(a.package_title) in ('picpay mais','picpay +','picpay+','picpay premium') or lower(a.package_title) = 'original')
# MAGIC   and a.created_at >= '2023-01-01'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross join com calendário -> Status por dia
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view base_class_ativo_dia as
# MAGIC           
# MAGIC -- Verifica se o consumer estava Ativo no dia
# MAGIC select
# MAGIC   consumer_id
# MAGIC   ,calendar_date
# MAGIC   ,effective_at
# MAGIC   ,contract_number
# MAGIC   ,package_id
# MAGIC   ,package_title
# MAGIC   ,package_type
# MAGIC   ,case 
# MAGIC     when date(canceled_at) > current_date() and subscription_status in ('CANCELED') then 'Inativo' -- consumer_id = 131650761944563981
# MAGIC     when date(canceled_at) is null or (canceled_at is not null and calendar_date < date(canceled_at)) then 'Ativo' 
# MAGIC     else 'Inativo' end as status_contract
# MAGIC from sql_base_consumers
# MAGIC cross join (
# MAGIC   select calendar_date
# MAGIC   from shared.calendar
# MAGIC   where calendar_date >= '2023-01-01' 
# MAGIC   and calendar_date <= current_date()
# MAGIC )
# MAGIC where 1=1
# MAGIC   and calendar_date >= date(effective_at)
# MAGIC   and effective_at is not null               -- Somente os que chegaram a ativar em algum momento
# MAGIC -- group by 1,2,3,4,5
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Priorizar Pacote PP+ frente ao original
# MAGIC ---
# MAGIC > Pegar as migrações

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace temp view base_class_priorizada as 
# MAGIC
# MAGIC select 
# MAGIC   consumer_id
# MAGIC   ,a.calendar_date as metric_date
# MAGIC   ,effective_at
# MAGIC   ,status_contract
# MAGIC   ,contract_number
# MAGIC   ,package_id
# MAGIC   ,package_title  
# MAGIC   ,row_number()over(
# MAGIC     partition by consumer_id, a.calendar_date 
# MAGIC     order by (case package_type 
# MAGIC                 when 'ppmais'   then 1 
# MAGIC                 when 'original' then 2 
# MAGIC                 when 'original - saques ilimitados' then 3
# MAGIC                 else 4 end),
# MAGIC               status_contract asc,
# MAGIC               effective_at desc
# MAGIC               ) order_active_day
# MAGIC from base_class_ativo_dia as a
# MAGIC qualify order_active_day = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base final
# MAGIC ---

# COMMAND ----------

base_final = spark.sql("""

select 
  consumer_id
  ,metric_date
  ,effective_at            
  ,status_contract
  ,contract_number
  ,package_id
  ,package_title 
from base_class_priorizada as a
where order_active_day = 1

""")

# COMMAND ----------

_output_df_ = base_final\
    .select(
        (monotonically_increasing_id() + 1).alias('pf_subscription_storyline_id')
        ,'*'
    )

# COMMAND ----------

_output_df_.write.mode('overwrite').saveAsTable('validation.pf_subscription_storyline', format='delta')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(distinct consumer_id), count(*) qtt
# MAGIC from validation.pf_subscription_storyline
# MAGIC where status_contract = 'Ativo' and package_title <> 'Original' and metric_date = '2024-09-18'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(distinct consumer_id), count(*) qtt
# MAGIC from validation.pf_subscription_storyline
# MAGIC where status_contract = 'Ativo' and package_title <> 'Original' and metric_date = '2024-06-12'
