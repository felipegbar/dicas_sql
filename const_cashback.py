# Databricks notebook source
# MAGIC %md 
# MAGIC ####self_service_analytics.pf_cashback_payments
# MAGIC
# MAGIC #####jira tasks:
# MAGIC NA
# MAGIC
# MAGIC #####last authors:
# MAGIC @FelipeBarreto
# MAGIC
# MAGIC #####last update:
# MAGIC 18/11/2024

# COMMAND ----------

# MAGIC %md
# MAGIC # Bibliotecas e parâmetros

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timedelta
from pyspark.sql.window import Window

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled', True)
spark.conf.set('spark.sql.shuffle.partitions', 'auto')
spark.conf.set('spark.sql.join.preferSortMergeJoin', True)
spark.conf.set('spark.sql.adaptive.skewedJoin.enabled', True)  
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', True)
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# COMMAND ----------

OUTPUT_PATH   = 's3://picpay-self-service-tables/self_service_analytics/'
OUTPUT_TABLE  = 'pf_scrapes_account/'
OUTPUT_PATH_FULL = OUTPUT_PATH + OUTPUT_TABLE
delta_dias = 100  # dias que devem sobrescrever na tabela existente
try:
  data_maxima = spark.read.format('delta').load(OUTPUT_PATH_FULL)\
    .agg(max(concat_ws('-',col('year_partition'),col('month_partition'),col('day_partition')))).collect()[0][0]
  inicio_analise = (datetime.strptime(data_maxima, '%Y-%m-%d') - timedelta(days=delta_dias)).strftime('%Y-%m-%d')
  fim_analise = current_date()
  mode_execution = 'overwrite'
  mensagem = 'Tabela existe'
  print(f"""
  -> {mensagem}. Gravar partição do dia em D-{delta_dias} a partir da data {inicio_analise}\n
  """)
except:
  inicio_analise = lit('2024-01-01')
  fim_analise = current_date()
  mode_execution = 'overwrite'
  mensagem = 'Tabela não existe'
  print(f'{mensagem}. Grava full load desde {inicio_analise}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### OBS.: Parâmetro para não mexer no histórico já gravado!!
# MAGIC > Caso queira gravar o histórico de antes de NOV/24, deixa comentado o cdm abaixo

# COMMAND ----------

if mensagem == 'Tabela não existe':
  inicio_analise = inicio_analise
elif (datetime.strptime(inicio_analise, '%Y-%m-%d') < datetime.strptime('2024-11-01', '%Y-%m-%d')):
  inicio_analise = '2024-11-01'
  print(f"O notebook rodará a partir da data {inicio_analise}")

# COMMAND ----------

# DBTITLE 1,Parâmetro - Caso queira rodar um período específico
# inicio_analise = '2024-01-01'
# fim_analise = current_date()

# COMMAND ----------

# MAGIC %md
# MAGIC # Nova base
# MAGIC
# MAGIC A tabela marketing.cleaned_event_cashback_status_changed substitui as tabelas abaixo a partir de nov/24
# MAGIC - event_tracking_historical.reward_recover_movement_created_events_history
# MAGIC - event_tracking_historical.reward_recover_movement_amount_zero_events_history
# MAGIC - event_tracking_historical.reward_recover_movement_expired_events_history
# MAGIC
# MAGIC Filtros:
# MAGIC - CASHBACK PAGO: properties_status = 'CREATED'
# MAGIC - CASHBACK RASPADO: properties_status = 'EXPIRED'

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabelas
# MAGIC ---

# COMMAND ----------

tab_promo_transactions                    = spark.table("marketing.promo_transactions") # tab com dados de cashback antes de nov/24
tab_promo_consumers_instant_cashs         = spark.table('marketing.promo_consumers_instant_cashs')  # tab com dados de instant cash antes de nov/24

tab_promo_campaigns_taxonomy              = spark.table('marketing.promo_campaigns_taxonomy') # tab com taxonomia de campanhas -> bu_solicitante like '%SFPF%'
tab_cleaned_event_cashback_status_changed = spark.table('marketing.cleaned_event_cashback_status_changed') # tab com tudo de cashback a partir de nov/24

tab_pix                                   = spark.table("pix.pix_total_payments")
tab_p2p                                   = spark.table("p2p.p2p_total_payments")
tab_bills_payments                        = spark.table("bills.bills_total_payments")
tab_vehicular_debts                       = spark.table("vehicular_debts.vehicular_debts_total_payments")
# tab_pf_products_transactions            = spark.table('self_service_analytics.pf_products_transactions') # tab com transacoes de SFPF
tab_transactional_core_transactions = spark.table("payments.transactional_core_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC # Ajuste de filtros de tabelas
# MAGIC ---

# COMMAND ----------

## TABELAS DE CASHBACK
df_promo_transactions = tab_promo_transactions\
    .withColumn('created_date', to_date('created_at'))\
    .withColumn('pix_id_caskback',expr("case when created_date >= '2023-07-01' then transaction_id else all_transaction_id end"))\
    .filter(
        (col('created_date') >= inicio_analise) & 
        (col('created_date') <= fim_analise)
    ).select(
            'pix_id_caskback'
            ,'promo_transaction_id'
            ,'campaign_id'
            ,'all_transaction_id'
            ,'transaction_id'
            ,'consumer_id'
            ,col('received_cashback_value').alias('received_cashback_value_promo')
            ,col('paid_credit_card_value').alias('paid_credit_card_value_promo')
          )

df_promo_campaigns_taxonomy = tab_promo_campaigns_taxonomy\
    .filter(col('bu_solicitante').like('%SFPF%'))\
    .dropDuplicates(subset=['promo_campaign_id'])\
    .select(col('promo_campaign_id').alias('campaign_id'),'briefing_id', 'cashback_percentage',lit(True).alias('is_sfpf_campaign_growth'))


df_cashback_sfpf = ( # somente campanhas de SFPF
    df_promo_transactions
        .join(
            df_promo_campaigns_taxonomy, 'campaign_id', 'left'
        )
        .withColumn('is_sfpf_campaign_growth', when(col('is_sfpf_campaign_growth') == True, True).otherwise(False))
)


df_promo_consumers_instant_cashs  = tab_promo_consumers_instant_cashs\
    .filter(
        (to_date('created_at') >= inicio_analise) &
        (to_date('created_at') <= fim_analise)
    )


df_cashback_pago_raspado_apartir_nov_24 = (
tab_cleaned_event_cashback_status_changed
    .dropDuplicates(subset=['properties_movement_id','properties_status'])
    # .withColumn('created_date', to_date('created_at')) # cashback datado do momento do pagamento do benefício
    .withColumn('created_date', to_date('properties_created_at')) # cashback datado do momento do fato gerador (transacao que deu direito)
    .withColumn('is_stant_cash', expr("case when properties_wallet_code_id = 419 then true else false end"))
    .filter(
        (col('created_date') >= inicio_analise) & 
        (col('created_date') <= fim_analise) &
        (col('created_date') >= '2024-11-01') &
        (col('properties_status') == 'CREATED')
    )
    .join(
        df_promo_campaigns_taxonomy
        ,on = col('properties_promo_campaign_id') == col('campaign_id')
        ,how = 'left'
    )
    .join(
        tab_transactional_core_transactions
            .filter(
                (year('created_at') >= 2024) &
                (to_date('created_at') >= inicio_analise) & 
                (to_date('created_at') <= fim_analise)
            )
            .select("correlation_id", col("base_value").alias('paid_credit_card_value_promo'))
        ,on = col('properties_origin_transaction_id') == col('correlation_id')
        ,how = 'inner'
    )
    .select(
            col('id').alias('cleaned_event_cashback_status_changed_id')
            ,(when(col('is_sfpf_campaign_growth') == True, True).otherwise(False)).alias('is_sfpf_campaign_growth')
            ,'properties_status'
            ,col('created_date').alias('created_date_cashback') # data que o cashback foi liberado
            ,'is_stant_cash'
            ,'properties_movement_id'
            ,'briefing_id'
            ,'cashback_percentage'
            ,col('properties_origin_transaction_id').alias('correlation_id')
            ,col('properties_promo_campaign_id').alias('campaign_id')
            ,col('user_id').alias('consumer_id')
            ,expr("case when properties_status = 'CREATED' then properties_original_amount else 0 end").alias('cashback_pago') 
            ,expr("case when properties_status = 'EXPIRED' then properties_returned_amount else 0 end").alias('cashback_raspado')
            ,expr("case when properties_status = 'CREATED' then paid_credit_card_value_promo else 0 end").alias('paid_credit_card_value_promo')
          )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # ANTES DE NOV/2024
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### CASHBACK

# COMMAND ----------

# DBTITLE 1,PIX
df_pix = (
tab_pix
    .filter(
        (col('adjusted_created_date') >= inicio_analise) & 
        (col('adjusted_created_date') <= fim_analise) &
        (col('user_type') == 'CONSUMER') &
        (col('paid_card_value') > 0)
    )
    .withColumn('pix_id_caskback',expr("case when adjusted_created_date >= '2023-07-01' then transactional_core_id else all_transaction_id end"))
)


df_pix_cashback = (
  df_pix.filter(col('adjusted_created_date') < '2024-11-01')
    .join( # join pra pegar e o cashback --> ratiado pelo tpv_card_total
        df_cashback_sfpf.drop('all_transaction_id','consumer_id')
        ,on = 'pix_id_caskback'
        ,how = 'inner'
    )
    .withColumn(
        'received_cashback_ratiado', 
         when(col('paid_wallet_value') > 0, lit(0))
        .when((col('paid_card_value') > 0) & (upper('transaction_type') != 'MULTIPLE_CARDS'), coalesce('received_cashback_value_promo',lit(0)))
        .when((col('paid_card_value') > 0) & (upper('transaction_type') == 'MULTIPLE_CARDS'), 
              col('received_cashback_value_promo') * ((coalesce('paid_card_value',lit(0)) - coalesce('payment_total_fee',lit(0))) / coalesce('paid_credit_card_value_promo',lit(0)))
        )
        .otherwise(lit(0))
    )
).select(
  'all_transaction_id'
  , lit(None).alias('cleaned_event_cashback_status_changed_id')
  , lit('PIX').alias('product_type_name')
  , lit('cashback').alias('type')
  , 'briefing_id'
  , 'cashback_percentage'
  , 'promo_transaction_id'
  , 'campaign_id'
  , 'transactional_core_id'
  , 'transaction_id'
  , 'correlation_id'
  , col('adjusted_created_date').alias('created_date')
  , expr("case when day(adjusted_created_date) <= day(current_date()) then true else false end as is_mtd")
  , 'is_sfpf_campaign_growth'
  , 'credit_card_id'
  , 'consumer_id'
  , 'installments'
  , 'transaction_type'
  , col('received_cashback_ratiado').alias('cashback_value')
  , 'paid_card_value'
  , col('payment_total_fee').alias('total_revenue')
  , expr("""CASE 
                    WHEN received_cashback_ratiado <= 10 THEN '01. até 10'
                    WHEN received_cashback_ratiado > 10  AND received_cashback_ratiado <= 50  THEN '02. até 50'
                    WHEN received_cashback_ratiado > 50  AND received_cashback_ratiado <= 100 THEN '03. até 100'
                    WHEN received_cashback_ratiado > 100 AND received_cashback_ratiado <= 200 THEN '04. até 200'
                    WHEN received_cashback_ratiado > 200 AND received_cashback_ratiado <= 300 THEN '05. até 300'
                    WHEN received_cashback_ratiado > 300 AND received_cashback_ratiado <= 400 THEN '06. até 400'
                    WHEN received_cashback_ratiado > 400 AND received_cashback_ratiado <= 500 THEN '07. até 500'
                    WHEN received_cashback_ratiado > 500 AND received_cashback_ratiado <= 600 THEN '08. até 600'
                    WHEN received_cashback_ratiado > 600 AND received_cashback_ratiado <= 700 THEN '09. até 700'
                    WHEN received_cashback_ratiado > 700 AND received_cashback_ratiado <= 800 THEN '10. até 800'
                    WHEN received_cashback_ratiado > 800 AND received_cashback_ratiado <= 900 THEN '11. até 900'
                    WHEN received_cashback_ratiado > 900 AND received_cashback_ratiado <= 1000 THEN '12. até 1000'
                    WHEN received_cashback_ratiado > 1000 THEN '13. acima de 1000'
                ELSE 'outro' END as cashback_range""")
)


# df_pix_cashback.limit(5).display()

# COMMAND ----------

# DBTITLE 1,P2P
df_p2p = (
tab_p2p
    .withColumn('created_date', to_date('created_at'))\
    .filter(
        (col('created_date') >= inicio_analise) & 
        (col('created_date') <= fim_analise) &
        (col('is_business_transaction') == False) &
        (col('paid_card_value') > 0)
    )
)


df_p2p_cashback = df_p2p\
    .filter(col('created_date') < '2024-11-01')\
    .join( # join pra pegar e o cashback --> ratiado pelo tpv_card_total
            df_cashback_sfpf.drop('transaction_id','consumer_id'), 'all_transaction_id', 'inner'
        )\
    .withColumn(
        'received_cashback_ratiado', 
         when(col('paid_wallet_value') > 0, lit(0))
        .when((col('paid_card_value') > 0) & (upper('transaction_type') != 'MULTIPLE_CARDS'), coalesce('received_cashback_value_promo',lit(0)))
        .when((col('paid_card_value') > 0) & (upper('transaction_type') == 'MULTIPLE_CARDS'), 
              col('received_cashback_value_promo') * ((coalesce('paid_card_value',lit(0)) - coalesce('total_revenue',lit(0))) / coalesce('paid_credit_card_value_promo',lit(0)))
        )
        .otherwise(lit(0))
    )\
    .select(
      'all_transaction_id'
      , lit(None).alias('cleaned_event_cashback_status_changed_id')
      , lit('P2P').alias('product_type_name')
      , lit('cashback').alias('type')
      ,'briefing_id'
      , 'cashback_percentage'
      , 'promo_transaction_id'
      , 'campaign_id'
      , lit(None).alias('transactional_core_id')
      , 'transaction_id'
      , 'correlation_id'
      , 'created_date'
      , expr("case when day(created_date) <= day(current_date()) then true else false end as is_mtd")
      , 'is_sfpf_campaign_growth'
      , 'credit_card_id'
      , col('payer_id').alias('consumer_id')
      , 'installments'
      , 'transaction_type'
      , col('received_cashback_ratiado').alias('cashback_value')
      , 'paid_card_value'
      , 'total_revenue'
      , expr("""CASE 
                      WHEN received_cashback_ratiado <= 10 THEN '01. até 10'
                      WHEN received_cashback_ratiado > 10  AND received_cashback_ratiado <= 50  THEN '02. até 50'
                      WHEN received_cashback_ratiado > 50  AND received_cashback_ratiado <= 100 THEN '03. até 100'
                      WHEN received_cashback_ratiado > 100 AND received_cashback_ratiado <= 200 THEN '04. até 200'
                      WHEN received_cashback_ratiado > 200 AND received_cashback_ratiado <= 300 THEN '05. até 300'
                      WHEN received_cashback_ratiado > 300 AND received_cashback_ratiado <= 400 THEN '06. até 400'
                      WHEN received_cashback_ratiado > 400 AND received_cashback_ratiado <= 500 THEN '07. até 500'
                      WHEN received_cashback_ratiado > 500 AND received_cashback_ratiado <= 600 THEN '08. até 600'
                      WHEN received_cashback_ratiado > 600 AND received_cashback_ratiado <= 700 THEN '09. até 700'
                      WHEN received_cashback_ratiado > 700 AND received_cashback_ratiado <= 800 THEN '10. até 800'
                      WHEN received_cashback_ratiado > 800 AND received_cashback_ratiado <= 900 THEN '11. até 900'
                      WHEN received_cashback_ratiado > 900 AND received_cashback_ratiado <= 1000 THEN '12. até 1000'
                      WHEN received_cashback_ratiado > 1000 THEN '13. acima de 1000'
                  ELSE 'outro' END as cashback_range""")
    )


# df_p2p_cashback.limit(5).display()

# COMMAND ----------

# DBTITLE 1,BILLS
df_bills = (
  tab_bills_payments
    .withColumn('created_date', to_date('created_at'))\
    .filter(
        (col('created_date') >= inicio_analise) & 
        (col('created_date') <= fim_analise) &
        (col('paid_credit_card_value') > 0)
    )
)

df_bills_cashback = df_bills\
    .filter(col('created_date') < '2024-11-01')\
    .join( # join pra pegar e o cashback --> ratiado pelo tpv_card_total
        df_cashback_sfpf.drop('transaction_id'), 'all_transaction_id', 'inner'
    )\
    .withColumn(
        'received_cashback_ratiado', 
         when(col('paid_balance_value') > 0, lit(0))
        .when((col('paid_credit_card_value') > 0) & (upper('payment_type') != 'MULTIPLE_CARDS'), coalesce('received_cashback_value_promo',lit(0)))
        .when((col('paid_credit_card_value') > 0) & (upper('payment_type') == 'MULTIPLE_CARDS'), 
              col('received_cashback_value_promo') * ((coalesce('paid_credit_card_value',lit(0)) - coalesce('total_revenue',lit(0))) / coalesce('paid_credit_card_value_promo',lit(0)))
        )
        .otherwise(lit(0))
    )\
    .select(
      'all_transaction_id'
      , lit(None).alias('cleaned_event_cashback_status_changed_id')
      , lit('BILLS').alias('product_type_name')
      , lit('cashback').alias('type')
      ,'briefing_id'
      , 'cashback_percentage'
      , 'promo_transaction_id'
      , 'campaign_id'
      , lit(None).alias('transactional_core_id')
      , 'transaction_id'
      , 'correlation_id'
      , 'created_date'
      , expr("case when day(created_date) <= day(current_date()) then true else false end as is_mtd")
      , 'is_sfpf_campaign_growth'
      , 'credit_card_id'
      , 'consumer_id'
      , 'installments'
      , col('payment_type').alias('transaction_type')
      , col('received_cashback_ratiado').alias('cashback_value')
      , col('paid_credit_card_value').alias('paid_card_value')
      , 'total_revenue'
      , expr("""CASE 
                      WHEN received_cashback_ratiado <= 10 THEN '01. até 10'
                      WHEN received_cashback_ratiado > 10  AND received_cashback_ratiado <= 50  THEN '02. até 50'
                      WHEN received_cashback_ratiado > 50  AND received_cashback_ratiado <= 100 THEN '03. até 100'
                      WHEN received_cashback_ratiado > 100 AND received_cashback_ratiado <= 200 THEN '04. até 200'
                      WHEN received_cashback_ratiado > 200 AND received_cashback_ratiado <= 300 THEN '05. até 300'
                      WHEN received_cashback_ratiado > 300 AND received_cashback_ratiado <= 400 THEN '06. até 400'
                      WHEN received_cashback_ratiado > 400 AND received_cashback_ratiado <= 500 THEN '07. até 500'
                      WHEN received_cashback_ratiado > 500 AND received_cashback_ratiado <= 600 THEN '08. até 600'
                      WHEN received_cashback_ratiado > 600 AND received_cashback_ratiado <= 700 THEN '09. até 700'
                      WHEN received_cashback_ratiado > 700 AND received_cashback_ratiado <= 800 THEN '10. até 800'
                      WHEN received_cashback_ratiado > 800 AND received_cashback_ratiado <= 900 THEN '11. até 900'
                      WHEN received_cashback_ratiado > 900 AND received_cashback_ratiado <= 1000 THEN '12. até 1000'
                      WHEN received_cashback_ratiado > 1000 THEN '13. acima de 1000'
                  ELSE 'outro' END as cashback_range""")
    )

# df_bills_cashback.limit(5).display()

# COMMAND ----------

# DBTITLE 1,Multas e IPVA
df_vehicular_debts = (
  tab_vehicular_debts
    .withColumn('created_date', to_date('created_at'))\
    .filter(
        (col('created_date') >= inicio_analise) & 
        (col('created_date') <= fim_analise)&
        (col('paid_card_value') > 0)
    )
)

df_vehicular_debts_cashback = df_vehicular_debts\
    .filter(col('created_date') < '2024-11-01')\
    .join( # join pra pegar e o cashback --> ratiado pelo tpv_card_total
        df_cashback_sfpf.drop('transaction_id','consumer_id'), 'all_transaction_id', 'inner'
    )\
    .withColumn(
        'received_cashback_ratiado', 
         when(col('paid_wallet_value') > 0, lit(0))
        .when((col('paid_card_value') > 0) & (upper('payment_type') != 'MULTIPLE_CARDS'), coalesce('received_cashback_value_promo',lit(0)))
        .when((col('paid_card_value') > 0) & (upper('payment_type') == 'MULTIPLE_CARDS'), 
              col('received_cashback_value_promo') * ((coalesce('paid_card_value',lit(0)) - coalesce('total_revenue',lit(0))) / coalesce('paid_credit_card_value_promo',lit(0)))
        )
        .otherwise(lit(0))
    )\
    .select(
      'all_transaction_id'
      , lit(None).alias('cleaned_event_cashback_status_changed_id')
      , lit('Multas e IPVA').alias('product_type_name')
      , lit('cashback').alias('type')
      ,'briefing_id'
      , 'cashback_percentage'
      , 'promo_transaction_id'
      , 'campaign_id'
      , lit(None).alias('transactional_core_id')
      , 'transaction_id'
      , 'correlation_id'
      , 'created_date'
      , expr("case when day(created_date) <= day(current_date()) then true else false end as is_mtd")
      , 'is_sfpf_campaign_growth'
      , 'credit_card_id'
      , 'consumer_id'
      , 'installments'
      , 'transaction_type'
      , col('received_cashback_ratiado').alias('cashback_value')
      , 'paid_card_value'
      , 'total_revenue'
      ,expr("""CASE 
                      WHEN received_cashback_ratiado <= 10 THEN '01. até 10'
                      WHEN received_cashback_ratiado > 10  AND received_cashback_ratiado <= 50  THEN '02. até 50'
                      WHEN received_cashback_ratiado > 50  AND received_cashback_ratiado <= 100 THEN '03. até 100'
                      WHEN received_cashback_ratiado > 100 AND received_cashback_ratiado <= 200 THEN '04. até 200'
                      WHEN received_cashback_ratiado > 200 AND received_cashback_ratiado <= 300 THEN '05. até 300'
                      WHEN received_cashback_ratiado > 300 AND received_cashback_ratiado <= 400 THEN '06. até 400'
                      WHEN received_cashback_ratiado > 400 AND received_cashback_ratiado <= 500 THEN '07. até 500'
                      WHEN received_cashback_ratiado > 500 AND received_cashback_ratiado <= 600 THEN '08. até 600'
                      WHEN received_cashback_ratiado > 600 AND received_cashback_ratiado <= 700 THEN '09. até 700'
                      WHEN received_cashback_ratiado > 700 AND received_cashback_ratiado <= 800 THEN '10. até 800'
                      WHEN received_cashback_ratiado > 800 AND received_cashback_ratiado <= 900 THEN '11. até 900'
                      WHEN received_cashback_ratiado > 900 AND received_cashback_ratiado <= 1000 THEN '12. até 1000'
                      WHEN received_cashback_ratiado > 1000 THEN '13. acima de 1000'
                  ELSE 'outro' END as cashback_range""")
    )

# df_vehicular_debts_cashback.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## INSTANT CASH

# COMMAND ----------

df_instant_cash = (
  df_promo_consumers_instant_cashs
    .join(
        df_promo_campaigns_taxonomy, 'campaign_id', 'inner' # passa por campanahs de SFPF -> is_sfpf_campaign_growth = true
    )
    .select(
      lit(None).alias('all_transaction_id')
      , lit(None).alias('cleaned_event_cashback_status_changed_id')
      , lit('Instant Cash').alias('product_type_name')
      , lit('instant_cash').alias('type')
      , 'briefing_id'
      , 'cashback_percentage'
      , lit(None).alias('promo_transaction_id')
      , 'campaign_id'
      , lit(None).alias('transactional_core_id')
      , lit(None).alias('transaction_id')
      , lit(None).alias('correlation_id')
      , to_date('created_at').alias('created_date')
      , expr("case when day(created_at) <= day(current_date()) then true else false end as is_mtd")
      , expr("case when is_sfpf_campaign_growth = true then true else false end as is_sfpf_campaign_growth")
      , lit(None).alias('credit_card_id')
      , 'consumer_id'
      , lit(None).alias('installments')
      , lit(None).alias('transaction_type')
      , col('instant_cash_given_value').alias('cashback_value')
      , lit(0).alias('paid_card_value')
      , lit(None).alias('total_revenue')
      , expr("""CASE 
                    WHEN instant_cash_given_value <= 10 THEN '01. até 10'
                    WHEN instant_cash_given_value > 10  AND instant_cash_given_value <= 50  THEN '02. até 50'
                    WHEN instant_cash_given_value > 50  AND instant_cash_given_value <= 100 THEN '03. até 100'
                    WHEN instant_cash_given_value > 100 AND instant_cash_given_value <= 200 THEN '04. até 200'
                    WHEN instant_cash_given_value > 200 AND instant_cash_given_value <= 300 THEN '05. até 300'
                    WHEN instant_cash_given_value > 300 AND instant_cash_given_value <= 400 THEN '06. até 400'
                    WHEN instant_cash_given_value > 400 AND instant_cash_given_value <= 500 THEN '07. até 500'
                    WHEN instant_cash_given_value > 500 AND instant_cash_given_value <= 600 THEN '08. até 600'
                    WHEN instant_cash_given_value > 600 AND instant_cash_given_value <= 700 THEN '09. até 700'
                    WHEN instant_cash_given_value > 700 AND instant_cash_given_value <= 800 THEN '10. até 800'
                    WHEN instant_cash_given_value > 800 AND instant_cash_given_value <= 900 THEN '11. até 900'
                    WHEN instant_cash_given_value > 900 AND instant_cash_given_value <= 1000 THEN '12. até 1000'
                    WHEN instant_cash_given_value > 1000 THEN '13. acima de 1000'
                ELSE 'outro' END as cashback_range""")
    )
)

# COMMAND ----------

cashback_antes_nov24 = (
  df_pix_cashback
    .union(df_p2p_cashback)
    .union(df_bills_cashback)
    .union(df_vehicular_debts_cashback)
    .union(df_instant_cash)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # APÓS DE NOV/2024
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,PIX
df_pix_cashback_apos_nov24 = (
  df_pix.filter(col('adjusted_created_date') >= '2024-11-01')
    .join( # join pra pegar e o cashback --> ratiado pelo tpv_card_total
        df_cashback_pago_raspado_apartir_nov_24.filter(col('is_stant_cash') == False).drop('consumer_id')
        ,on = 'correlation_id'
        ,how = 'inner'
    )
    .withColumn(
        'received_cashback_ratiado', 
         when(col('paid_wallet_value') > 0, lit(0))
        .when((col('paid_card_value') > 0) & (upper('transaction_type') != 'MULTIPLE_CARDS'), coalesce('cashback_pago',lit(0)))
        .when((col('paid_card_value') > 0) & (upper('transaction_type') == 'MULTIPLE_CARDS'), 
              col('cashback_pago') * ((coalesce('paid_card_value',lit(0)) - coalesce('payment_total_fee',lit(0))) / coalesce('paid_credit_card_value_promo',lit(0)))
        )
        .otherwise(lit(0))
    )
).select(
  'all_transaction_id'
  , col('cleaned_event_cashback_status_changed_id')
  , lit('PIX').alias('product_type_name')
  , lit('cashback').alias('type')
  , 'briefing_id'
  , 'cashback_percentage'
  , lit(None).alias('promo_transaction_id')
  , 'campaign_id'
  , 'transactional_core_id'
  , lit(None).alias('transaction_id')
  , 'correlation_id'
  , col('adjusted_created_date').alias('created_date')
  , expr("case when day(adjusted_created_date) <= day(current_date()) then true else false end as is_mtd")
  , 'is_sfpf_campaign_growth'
  , 'credit_card_id'
  , 'consumer_id'
  , 'installments'
  , 'transaction_type'
  , col('received_cashback_ratiado').alias('cashback_value')
  , 'paid_card_value'
  , col('payment_total_fee').alias('total_revenue')
  , expr("""CASE 
                    WHEN received_cashback_ratiado <= 10 THEN '01. até 10'
                    WHEN received_cashback_ratiado > 10  AND received_cashback_ratiado <= 50  THEN '02. até 50'
                    WHEN received_cashback_ratiado > 50  AND received_cashback_ratiado <= 100 THEN '03. até 100'
                    WHEN received_cashback_ratiado > 100 AND received_cashback_ratiado <= 200 THEN '04. até 200'
                    WHEN received_cashback_ratiado > 200 AND received_cashback_ratiado <= 300 THEN '05. até 300'
                    WHEN received_cashback_ratiado > 300 AND received_cashback_ratiado <= 400 THEN '06. até 400'
                    WHEN received_cashback_ratiado > 400 AND received_cashback_ratiado <= 500 THEN '07. até 500'
                    WHEN received_cashback_ratiado > 500 AND received_cashback_ratiado <= 600 THEN '08. até 600'
                    WHEN received_cashback_ratiado > 600 AND received_cashback_ratiado <= 700 THEN '09. até 700'
                    WHEN received_cashback_ratiado > 700 AND received_cashback_ratiado <= 800 THEN '10. até 800'
                    WHEN received_cashback_ratiado > 800 AND received_cashback_ratiado <= 900 THEN '11. até 900'
                    WHEN received_cashback_ratiado > 900 AND received_cashback_ratiado <= 1000 THEN '12. até 1000'
                    WHEN received_cashback_ratiado > 1000 THEN '13. acima de 1000'
                ELSE 'outro' END as cashback_range""")
)


# df_pix_cashback_apos_nov24.limit(5).display()

# COMMAND ----------

# DBTITLE 1,P2P
df_p2p_cashback_apos_nov24 = df_p2p\
    .filter(col('created_date') >= '2024-11-01')\
    .join( # join pra pegar e o cashback --> ratiado pelo tpv_card_total
            df_cashback_pago_raspado_apartir_nov_24.filter(col('is_stant_cash') == False).drop('consumer_id'), 'correlation_id', 'inner'
        )\
    .withColumn(
        'received_cashback_ratiado', 
         when(col('paid_wallet_value') > 0, lit(0))
        .when((col('paid_card_value') > 0) & (upper('transaction_type') != 'MULTIPLE_CARDS'), coalesce('cashback_pago',lit(0)))
        .when((col('paid_card_value') > 0) & (upper('transaction_type') == 'MULTIPLE_CARDS'), 
              col('cashback_pago') * ((coalesce('paid_card_value',lit(0)) - coalesce('total_revenue',lit(0))) / coalesce('paid_credit_card_value_promo',lit(0)))
        )
        .otherwise(lit(0))
    )\
    .select(
      'all_transaction_id'
      , col('cleaned_event_cashback_status_changed_id')
      , lit('P2P').alias('product_type_name')
      , lit('cashback').alias('type')
      ,'briefing_id'
      , 'cashback_percentage'
      , lit(None).alias('promo_transaction_id')
      , 'campaign_id'
      , lit(None).alias('transactional_core_id')
      , 'transaction_id'
      , 'correlation_id'
      , 'created_date'
      , expr("case when day(created_date) <= day(current_date()) then true else false end as is_mtd")
      , 'is_sfpf_campaign_growth'
      , 'credit_card_id'
      , col('payer_id').alias('consumer_id')
      , 'installments'
      , 'transaction_type'
      , col('received_cashback_ratiado').alias('cashback_value')
      , 'paid_card_value'
      , 'total_revenue'
      , expr("""CASE 
                      WHEN received_cashback_ratiado <= 10 THEN '01. até 10'
                      WHEN received_cashback_ratiado > 10  AND received_cashback_ratiado <= 50  THEN '02. até 50'
                      WHEN received_cashback_ratiado > 50  AND received_cashback_ratiado <= 100 THEN '03. até 100'
                      WHEN received_cashback_ratiado > 100 AND received_cashback_ratiado <= 200 THEN '04. até 200'
                      WHEN received_cashback_ratiado > 200 AND received_cashback_ratiado <= 300 THEN '05. até 300'
                      WHEN received_cashback_ratiado > 300 AND received_cashback_ratiado <= 400 THEN '06. até 400'
                      WHEN received_cashback_ratiado > 400 AND received_cashback_ratiado <= 500 THEN '07. até 500'
                      WHEN received_cashback_ratiado > 500 AND received_cashback_ratiado <= 600 THEN '08. até 600'
                      WHEN received_cashback_ratiado > 600 AND received_cashback_ratiado <= 700 THEN '09. até 700'
                      WHEN received_cashback_ratiado > 700 AND received_cashback_ratiado <= 800 THEN '10. até 800'
                      WHEN received_cashback_ratiado > 800 AND received_cashback_ratiado <= 900 THEN '11. até 900'
                      WHEN received_cashback_ratiado > 900 AND received_cashback_ratiado <= 1000 THEN '12. até 1000'
                      WHEN received_cashback_ratiado > 1000 THEN '13. acima de 1000'
                  ELSE 'outro' END as cashback_range""")
    )


# df_p2p_cashback_apos_nov24.limit(5).display()

# COMMAND ----------

# DBTITLE 1,BILLS
df_bills_cashback_apos_nov24 = df_bills\
    .filter(col('created_date') >= '2024-11-01')\
    .join( # join pra pegar e o cashback --> ratiado pelo tpv_card_total
        df_cashback_pago_raspado_apartir_nov_24.filter(col('is_stant_cash') == False), 'correlation_id', 'inner'
    )\
    .withColumn(
        'received_cashback_ratiado', 
         when(col('paid_balance_value') > 0, lit(0))
        .when((col('paid_credit_card_value') > 0) & (upper('payment_type') != 'MULTIPLE_CARDS'), coalesce('cashback_pago',lit(0)))
        .when((col('paid_credit_card_value') > 0) & (upper('payment_type') == 'MULTIPLE_CARDS'), 
              col('cashback_pago') * ((coalesce('paid_credit_card_value',lit(0)) - coalesce('total_revenue',lit(0))) / coalesce('paid_credit_card_value_promo',lit(0)))
        )
        .otherwise(lit(0))
    )\
    .select(
      'all_transaction_id'
      , col('cleaned_event_cashback_status_changed_id')
      , lit('BILLS').alias('product_type_name')
      , lit('cashback').alias('type')
      ,'briefing_id'
      , 'cashback_percentage'
      , lit(None).alias('promo_transaction_id')
      , 'campaign_id'
      , lit(None).alias('transactional_core_id')
      , 'transaction_id'
      , 'correlation_id'
      , 'created_date'
      , expr("case when day(created_date) <= day(current_date()) then true else false end as is_mtd")
      , 'is_sfpf_campaign_growth'
      , 'credit_card_id'
      , 'consumer_id'
      , 'installments'
      , col('payment_type').alias('transaction_type')
      , col('received_cashback_ratiado').alias('cashback_value')
      , col('paid_credit_card_value').alias('paid_card_value')
      , 'total_revenue'
      , expr("""CASE 
                      WHEN received_cashback_ratiado <= 10 THEN '01. até 10'
                      WHEN received_cashback_ratiado > 10  AND received_cashback_ratiado <= 50  THEN '02. até 50'
                      WHEN received_cashback_ratiado > 50  AND received_cashback_ratiado <= 100 THEN '03. até 100'
                      WHEN received_cashback_ratiado > 100 AND received_cashback_ratiado <= 200 THEN '04. até 200'
                      WHEN received_cashback_ratiado > 200 AND received_cashback_ratiado <= 300 THEN '05. até 300'
                      WHEN received_cashback_ratiado > 300 AND received_cashback_ratiado <= 400 THEN '06. até 400'
                      WHEN received_cashback_ratiado > 400 AND received_cashback_ratiado <= 500 THEN '07. até 500'
                      WHEN received_cashback_ratiado > 500 AND received_cashback_ratiado <= 600 THEN '08. até 600'
                      WHEN received_cashback_ratiado > 600 AND received_cashback_ratiado <= 700 THEN '09. até 700'
                      WHEN received_cashback_ratiado > 700 AND received_cashback_ratiado <= 800 THEN '10. até 800'
                      WHEN received_cashback_ratiado > 800 AND received_cashback_ratiado <= 900 THEN '11. até 900'
                      WHEN received_cashback_ratiado > 900 AND received_cashback_ratiado <= 1000 THEN '12. até 1000'
                      WHEN received_cashback_ratiado > 1000 THEN '13. acima de 1000'
                  ELSE 'outro' END as cashback_range""")
    )

# df_bills_cashback_apos_nov24.limit(5).display()

# COMMAND ----------

# DBTITLE 1,Multas e IPVA
df_vehicular_debts_cashback_apos_nov24 = df_vehicular_debts\
    .filter(col('created_date') >= '2024-11-01')\
    .join( # join pra pegar e o cashback --> ratiado pelo tpv_card_total
        df_cashback_pago_raspado_apartir_nov_24
          .filter(col('is_stant_cash') == False)
          .drop('consumer_id'), 'correlation_id', 'inner'
    )\
    .withColumn(
        'received_cashback_ratiado', 
         when(col('paid_wallet_value') > 0, lit(0))
        .when((col('paid_card_value') > 0) & (upper('payment_type') != 'MULTIPLE_CARDS'), coalesce('cashback_pago',lit(0)))
        .when((col('paid_card_value') > 0) & (upper('payment_type') == 'MULTIPLE_CARDS'), 
              col('cashback_pago') * ((coalesce('paid_card_value',lit(0)) - coalesce('total_revenue',lit(0))) / coalesce('paid_credit_card_value_promo',lit(0)))
        )
        .otherwise(lit(0))
    )\
    .select(
      'all_transaction_id'
      , col('cleaned_event_cashback_status_changed_id')
      , lit('Multas e IPVA').alias('product_type_name')
      , lit('cashback').alias('type')
      ,'briefing_id'
      , 'cashback_percentage'
      , lit(None).alias('promo_transaction_id')
      , 'campaign_id'
      , lit(None).alias('transactional_core_id')
      , 'transaction_id'
      , 'correlation_id'
      , 'created_date'
      , expr("case when day(created_date) <= day(current_date()) then true else false end as is_mtd")
      , 'is_sfpf_campaign_growth'
      , 'credit_card_id'
      , 'consumer_id'
      , 'installments'
      , 'transaction_type'
      , col('received_cashback_ratiado').alias('cashback_value')
      , 'paid_card_value'
      , 'total_revenue'
      ,expr("""CASE 
                      WHEN received_cashback_ratiado <= 10 THEN '01. até 10'
                      WHEN received_cashback_ratiado > 10  AND received_cashback_ratiado <= 50  THEN '02. até 50'
                      WHEN received_cashback_ratiado > 50  AND received_cashback_ratiado <= 100 THEN '03. até 100'
                      WHEN received_cashback_ratiado > 100 AND received_cashback_ratiado <= 200 THEN '04. até 200'
                      WHEN received_cashback_ratiado > 200 AND received_cashback_ratiado <= 300 THEN '05. até 300'
                      WHEN received_cashback_ratiado > 300 AND received_cashback_ratiado <= 400 THEN '06. até 400'
                      WHEN received_cashback_ratiado > 400 AND received_cashback_ratiado <= 500 THEN '07. até 500'
                      WHEN received_cashback_ratiado > 500 AND received_cashback_ratiado <= 600 THEN '08. até 600'
                      WHEN received_cashback_ratiado > 600 AND received_cashback_ratiado <= 700 THEN '09. até 700'
                      WHEN received_cashback_ratiado > 700 AND received_cashback_ratiado <= 800 THEN '10. até 800'
                      WHEN received_cashback_ratiado > 800 AND received_cashback_ratiado <= 900 THEN '11. até 900'
                      WHEN received_cashback_ratiado > 900 AND received_cashback_ratiado <= 1000 THEN '12. até 1000'
                      WHEN received_cashback_ratiado > 1000 THEN '13. acima de 1000'
                  ELSE 'outro' END as cashback_range""")
    )

# df_vehicular_debts_cashback_apos_nov24.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## INSTANT CASH

# COMMAND ----------

instant_cash_apos_nov24 = (
  df_cashback_pago_raspado_apartir_nov_24
    .filter(
      (col('is_stant_cash') == True) &
      (col('is_sfpf_campaign_growth') == True) # passa por campanhas de SFPF
    )
    .select(
      lit(None).alias('all_transaction_id')
      , col('cleaned_event_cashback_status_changed_id')
      , lit('Instant Cash').alias('product_type_name')
      , lit('instant_cash').alias('type')
      , 'briefing_id'
      , 'cashback_percentage'
      , lit(None).alias('promo_transaction_id')
      , 'campaign_id'
      , lit(None).alias('transactional_core_id')
      , lit(None).alias('transaction_id')
      , 'correlation_id'
      , col('created_date_cashback').alias('created_date')
      , expr("case when day(created_date_cashback) <= day(current_date()) then true else false end as is_mtd")
      , 'is_sfpf_campaign_growth'
      , lit(None).alias('credit_card_id')
      , 'consumer_id'
      , lit(None).alias('installments')
      , lit(None).alias('transaction_type')
      , col('cashback_raspado').alias('cashback_value')
      , lit(None).alias('paid_card_value')
      , lit(None).alias('total_revenue')
      ,expr("""CASE 
                      WHEN cashback_raspado <= 10 THEN '01. até 10'
                      WHEN cashback_raspado > 10  AND cashback_raspado <= 50  THEN '02. até 50'
                      WHEN cashback_raspado > 50  AND cashback_raspado <= 100 THEN '03. até 100'
                      WHEN cashback_raspado > 100 AND cashback_raspado <= 200 THEN '04. até 200'
                      WHEN cashback_raspado > 200 AND cashback_raspado <= 300 THEN '05. até 300'
                      WHEN cashback_raspado > 300 AND cashback_raspado <= 400 THEN '06. até 400'
                      WHEN cashback_raspado > 400 AND cashback_raspado <= 500 THEN '07. até 500'
                      WHEN cashback_raspado > 500 AND cashback_raspado <= 600 THEN '08. até 600'
                      WHEN cashback_raspado > 600 AND cashback_raspado <= 700 THEN '09. até 700'
                      WHEN cashback_raspado > 700 AND cashback_raspado <= 800 THEN '10. até 800'
                      WHEN cashback_raspado > 800 AND cashback_raspado <= 900 THEN '11. até 900'
                      WHEN cashback_raspado > 900 AND cashback_raspado <= 1000 THEN '12. até 1000'
                      WHEN cashback_raspado > 1000 THEN '13. acima de 1000'
                  ELSE 'outro' END as cashback_range""")
    )
)

# COMMAND ----------

cashback_apos_nov24 = (
  df_pix_cashback_apos_nov24
    .union(df_p2p_cashback_apos_nov24)
    .union(df_bills_cashback_apos_nov24)
    .union(df_vehicular_debts_cashback_apos_nov24)
    .union(instant_cash_apos_nov24)
)

# COMMAND ----------

# MAGIC %md # Join Final
# MAGIC ---

# COMMAND ----------

_output_df_  = cashback_antes_nov24.union(cashback_apos_nov24)\
    .select(
        (monotonically_increasing_id() + 1).alias('pf_cashback_payment_id')
        ,'*'
    ).select(
        'pf_cashback_payment_id'
        ,'cleaned_event_cashback_status_changed_id' ## nova
        ,'promo_transaction_id' ## nova
        ,'campaign_id' ## nova
        ,'briefing_id'
        ,'transactional_core_id' ## nova
        ,'transaction_id' ## nova
        ,'correlation_id' ## nova
        ,'credit_card_id' ## nova
        ,'all_transaction_id'
        ,'consumer_id'
        ,'created_date'
        ,date_format('created_date','y-MM').alias('year_month')
        ,'type'
        ,'product_type_name'
        ,'transaction_type' ## nova
        ,'is_mtd'
        ,'is_sfpf_campaign_growth'
        ,'installments'
        ,'cashback_percentage'
        ,'cashback_range'
        ,col('cashback_value').alias('received_cashback_value')
        ,col('paid_card_value').alias('paid_credit_card_value')
        ,'total_revenue' ## nova
        ,date_format(col('created_date'), 'yyyy').alias('year_partition')
        ,date_format(col('created_date'), 'MM').alias('month_partition')
        ,date_format(col('created_date'), 'dd').alias('day_partition')
    )

