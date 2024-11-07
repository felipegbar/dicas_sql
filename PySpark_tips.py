# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark Tips
# MAGIC `@felipebarreto`
# MAGIC
# MAGIC Documentação:
# MAGIC   - [SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
# MAGIC   - [GitHub PySpatk](https://github.com/apache/spark/tree/master/python/pyspark)
# MAGIC   - [Spark by Examples](https://sparkbyexamples.com/pyspark-tutorial/)
# MAGIC ***
# MAGIC
# MAGIC **Aplicação**: 
# MAGIC * &check; [CR - Notebook](https://picpay-principal.cloud.databricks.com/?o=3048457690269382#notebook/3911230370723820) | [CR - Funções](https://picpay-principal.cloud.databricks.com/?o=3048457690269382#notebook/3911230370766429/command/3911230370766432)
# MAGIC * &check; [Recorrência de BILLS](https://picpay-principal.cloud.databricks.com/?o=3048457690269382#notebook/1505237358979984/command/1505237358979993)
# MAGIC * &check; [MAT/WAT/DAT - MAU/WAU/DAU]()
# MAGIC * &check; [Incremental de Tabelas]()
# MAGIC * &check; [Parâmetros SQL](https://picpay-principal.cloud.databricks.com/?o=3048457690269382#notebook/4175223022088063/command/4175223022088069)
# MAGIC * &check; [Teste AB - Separação de público]()

# COMMAND ----------

# MAGIC %md # 00. Vantagens do PySpark
# MAGIC ---
# MAGIC
# MAGIC 1. Velocidade de escrita
# MAGIC 2. Segunda opção de linguagem
# MAGIC 3. Manipulação do dado
# MAGIC 4. Menor risco de errar/esquecer algum parâmetro, pois é setado no início

# COMMAND ----------

# MAGIC %md # 01. Primeiros Passos
# MAGIC ***
# MAGIC
# MAGIC - <b>Organização:</b> Faça tudo de maneira organizada;
# MAGIC - <b>Fluência:</b> Explique minimamente seu código;
# MAGIC - <b>Identação:</b> Idente sempre
# MAGIC
# MAGIC
# MAGIC > Exemplo: Guardar um código
# MAGIC
# MAGIC
# MAGIC ``` python
# MAGIC df_bills = spark.table("bills.bills_transactions_overview")\
# MAGIC     .filter(
# MAGIC         (to_date('transaction_created_at') >= to_date(add_months(date_trunc('month',current_date()),-(analysis_months+frequency_months-1)))) &
# MAGIC         (to_date('transaction_created_at') < current_date()) &
# MAGIC         (col('is_approved') == True)
# MAGIC     )\
# MAGIC     .withColumn(
# MAGIC        'calendar_date', to_date('transaction_created_at')
# MAGIC     )\
# MAGIC     .join(
# MAGIC         spark.table("bills.bills_transactions_registers")
# MAGIC             .select('all_transaction_id','cnpj_cpf_origin','cnpj_cpf_origin_type','species_title')
# MAGIC         ,on = 'all_transaction_id'
# MAGIC         ,how = 'left'
# MAGIC     )\
# MAGIC     .join(
# MAGIC         broadcast(spark.table("shared.calendar"))
# MAGIC         ,on = 'calendar_date'
# MAGIC         ,how = 'left'
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Libs
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Base PIX de exemplo
## Acessando uma tabela específica 
pix_transactions = spark.table('pix.pix_transactions').limit(1000)

# COMMAND ----------

# DBTITLE 1,Visualização da base
pix_transactions.limit(3).display()

# COMMAND ----------

# DBTITLE 1,Visualização da base
pix_transactions.select('all_transaction_id','user_id').limit(3).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Visualização da base
pix_transactions.select('all_transaction_id','user_id').show(3, truncate=False)

# COMMAND ----------

# DBTITLE 1,Schema
pix_transactions.printSchema()

# COMMAND ----------

# DBTITLE 1,Tipo do objeto
type(pix_transactions)

# COMMAND ----------

# DBTITLE 1,Número de linhas
pix_transactions.count()

# COMMAND ----------

# DBTITLE 1,Número de colunas
len(pix_transactions.columns)

# COMMAND ----------

# DBTITLE 1,Campos 
pix_transactions.columns

# COMMAND ----------

# DBTITLE 1,Colocando as colunas em ordem alfabética
pix_transactions\
    .select(sorted(pix_transactions.columns))\
    .display()

# COMMAND ----------

data_esp = 7

# COMMAND ----------

# DBTITLE 1,Selec Específico de Pix - 7 primeiros dias do mês
pix_df = spark.sql(f"""

select 
    adjusted_created_at as data,
    user_id,
    date_add(adjusted_created_at, 7) as data_7,
    case 
        when cast(installments as int) > 1 and 
             payment_method in ('CREDIT_CARD', 'BALANCE_AND_CREDIT_CARD') then 'Cartao parcelado'
        when cast(installments as int) = 1 and 
             payment_method in ('CREDIT_CARD', 'BALANCE_AND_CREDIT_CARD') then 'Cartao a vista'
        else 'Saldo carteira' end as metodo_pagamento,
    paid_total_value_with_revenue as tpv
from pix.pix_transactions as pt
where 1=1
    and pix_transaction_type = 'PF'
    and day(adjusted_created_at) <= {data_esp}

limit 5

""")

pix_df.show()

# COMMAND ----------

lista_cols = ['metodo_pagamento']

# COMMAND ----------

# DBTITLE 1,SELECIONANDO E AGRUPANDO DIMENSÕES
agg_pix = pix_df\
    .groupBy(lista_cols)\
    .agg(
        sum('tpv').alias('TPV'),
        count('*').alias('Transações'),
        countDistinct('user_id').alias('Usuários')
    )

agg_pix.show()

# COMMAND ----------

# DBTITLE 1,Renomeando, adicionando e excluindo um campo
agg_pix\
    .withColumnRenamed('metodo_pagamento', 'Método de Pagamento')\
    .withColumn('Ticket Médio', 
                round(col('TPV') / col('Usuários'), 2)
    )\
    .drop('Usuários')\
    .show()

# COMMAND ----------

# MAGIC %md # 02. Algumas Estatísticas Descritivas
# MAGIC ***

# COMMAND ----------

# DBTITLE 1,summary
cols_summary = [
    'summary'
    ,'status'
    ,'payment_method'
    ,'paid_total_value_with_revenue'
]

pix_transactions.summary()\
    .select(cols_summary).display()

# COMMAND ----------

# MAGIC %md # 03. View Temporária
# MAGIC ***

# COMMAND ----------

## Query derivada do dataframe criardo anteriormente

pix_df.createOrReplaceTempView("agg3")

# COMMAND ----------

# MAGIC %sql select * from agg3

# COMMAND ----------

# MAGIC %md # 04. Ler arquivo csv

# COMMAND ----------

# File location and type
file_location = "s3://picpay-datalake-sandbox/felipebarreto/self_service_analytics/base_mat_p2p_teste.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files
df_s3 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

cols = [
 'Produto'
 ,'Data'
 ,'MAT35'
]

df_s3 = df_s3.toDF(*cols) # renomeado todas as colunas ao mesmo tempo | sempre na ordem

df_s3.show(5)

# COMMAND ----------

# MAGIC %md # 05. Criando uma tabela no Validation

# COMMAND ----------

## AS VARIÁVEIS NÃO PODEM TER ESPAÇOS

df_s3.write.saveAsTable('validation.pf_p2p_mat_teste', format='parquet', mode='overwrite')

# COMMAND ----------

# MAGIC %md # Função para baixar mais de 1MM de linhas 

# COMMAND ----------

from pyspark.sql.window import Window
def _download_bigdata_ (df, n1, n2, particao='consumer_id'):
    #df.createOrReplaceTempView("tabela")
    base = df.withColumn("dense_rank", 
                         dense_rank().over(
                             Window.orderBy(particao)
                         )
                        )\
        .filter((col('dense_rank') >= n1) & (col('dense_rank') <= n2))
    return base

# COMMAND ----------

# MAGIC %md # 06. Ciando dataframe

# COMMAND ----------

df_2 = spark.createDataFrame(
    [
        ("linha 1", 1, 10, 23),
        ("linha 2", 2, 11, 25),
        ("linha 3", 3, 12, 12),
        ("linha 4", 4, 13, 4),
        ("linha 5", 5, 14, 5)
    ],
    ["Linha", "jan/22", "fev/22", "mar/22"] 
)

df_2.display()

# COMMAND ----------

# MAGIC %md # 07. Escrevendo tabela em parquet no S3

# COMMAND ----------

df_s3.write.format('parquet').mode('overwrite').save('s3://picpay-datalake-sandbox/felipebarreto/self_service_analytics/df_mat_p2p_parquet')

# COMMAND ----------

# MAGIC %md # 08. Lendo tabela no S3

# COMMAND ----------

df = spark.read.parquet('s3://picpay-datalake-sandbox/felipebarreto/self_service_analytics/df_name')

df.show()

# COMMAND ----------

# MAGIC %md # 09. Unpivot

# COMMAND ----------

  , stack(12,
         'm0', churn_avg_1  ,
         'm1', churn_avg_2  ,
         'm2', churn_avg_3  ,
         'm3', churn_avg_4  ,
         'm4', churn_avg_5  ,
         'm5', churn_avg_6  ,
         'm6', churn_avg_7  ,
         'm7', churn_avg_8  ,
         'm8', churn_avg_9  ,
         'm9', churn_avg_10 ,
         'm10', churn_avg_11 ,
         'm11', churn_avg_12 
    ) as (`Mês`, Total)

# COMMAND ----------

from pyspark.sql.functions import expr

df_2_unpiovot = df_2\
    .select(
        'Linha'
        ,expr("""
            stack(3, 
            '1.Janeiro', `jan/22`,
            '2.Fevereiro', `Fev/22`,
            '3.Março', `Mar/22`
            )
             as (`Mês`,Total)  
        """)
    )
    
df_2_unpiovot.display()

# COMMAND ----------

# MAGIC %md # 10. Pivot

# COMMAND ----------

from pyspark.sql.functions import sum

df_2_unpiovot\
    .groupBy('Linha')\
    .pivot('Mês')\
    .agg(sum('Total')).display()

# COMMAND ----------

# MAGIC %md # 11. Cálculo do MAT,WAT,DAT / MAU,WAU,DAU

# COMMAND ----------

# DBTITLE 1,Função

def _crossjoin_mat (df, 
                    col_group = ['product_type_name','calendar_date'],  
                    kpi_time = 35,
                    kpi = 'mau',
                    data_min_metrica = '2019-11-01'):
    """
    Função para calcular o MAT, WAT, DAT / MAU, WAU, DAU 
    
    Parâmetros:
    -----------
    df:  Base de dados --> formato campos: created_date, consumer_id, product_type_name --> product_type_name não precisa, aí é pra qdo tiver mais de um produto
    col_group = ['product_type_name','calendar_date']
    kpi_time: Tempo em dias escolhido
    
    Exemplo:
    --------
    ## mat_1  == DAT
    ## mat_7  == WAT 7
    ## mat_35 == MAT 35
    """
    dt_corte_min = df.select(min('created_date')).collect()[0][0]
    df_kpi = (
            df.crossJoin(
                    broadcast(
                        spark.table("shared.calendar")
                            .filter(
                                    (col('calendar_date') < current_date()) &
                                    (col('calendar_date') >= lit(data_min_metrica))
                                ) # tabela de calendário com campo calendar_date no formato yyyy-mm-dd
                            .select('calendar_date')
                            )
                        )
                .withColumn(
                        'window_reference', datediff(col('calendar_date'), 'created_date'))
                .withColumn(
                        'is_kpi', 
                        when((col('window_reference') >= 0) & (col('window_reference') <= (kpi_time-1)), lit(1))
                        .otherwise(lit(0)))
                .filter(col('is_kpi') == 1)
    # >> Resultado: o consumer do produto product_type_name, na data calendar_date, é mat_35
                .groupBy(['consumer_id'] + col_group)
                .agg(max('is_kpi').alias('is_kpi'))
    ## Vendo o MAT 35 por produto
                .groupBy(col_group)
                .agg(countDistinct('consumer_id').alias(str(kpi) + '_' + str(kpi_time)))
    ## retirar a gordura do cálculo defasado de dias
                .filter(
                    (col('calendar_date') > date_add(lit(dt_corte_min), kpi_time))
                )
        )
    return df_kpi

# COMMAND ----------

# DBTITLE 1,Exemplo para P2P
from pyspark.sql.functions import *

df_p2p = (
        spark.table('p2p.p2p_transactions_overview')
        .filter((col('is_approved')==True) & (col('created_at')>='2023-06-01'))
        .select(
            'consumer_id'
            ,to_date('created_at').alias('created_date')
            ,'product_type_name'
            ,'payment_distribution' # coloquei mais um parâmetro - SoF
        )
    )
df_p2p.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_type_name, count(distinct consumer_id) qtt 
# MAGIC from p2p.p2p_transactions_overview
# MAGIC where 
# MAGIC is_approved
# MAGIC and created_at >= date_add(current_date(),-35) and date(created_at) < current_date()
# MAGIC group by 1

# COMMAND ----------

# DBTITLE 1,MAT 35 para P2P
_crossjoin_mat(
    df = df_p2p
    ,col_group = ['product_type_name','calendar_date']
    ,kpi_time = 35
    ,kpi = 'mat'
).display()

# COMMAND ----------

# DBTITLE 1,WAT 7 para P2P
_crossjoin_mat(
    df = df_p2p
    ,col_group = ['product_type_name','calendar_date']
    ,kpi_time = 7
    ,kpi = 'wat'
).display()

# COMMAND ----------

# DBTITLE 1,MAT 35 para P2P - SoF
_crossjoin_mat(
    df = df_p2p
    ,col_group = ['calendar_date','product_type_name','payment_distribution']
    ,kpi_time = 35
    ,kpi = 'mat'

).orderBy(col('calendar_date').desc(),'product_type_name','payment_distribution').display()

# COMMAND ----------

# MAGIC %md # 12. Separação de Público

# COMMAND ----------

display(
df_p2p
    .groupBy('payment_distribution')
    .agg(countDistinct('consumer_id').alias('consumers'))
)

# COMMAND ----------

# DBTITLE 1,Ajustando a base
df_amostra = df_p2p\
    .filter(col('payment_distribution').isin('mixed', 'credit', 'balance'))\
    .select('consumer_id','payment_distribution').dropDuplicates()

df_amostra.show(3)

# COMMAND ----------

# DBTITLE 1,3 grupos de 10mil consumers, independente de payment_distribution
(df1, df2, df3) = df_amostra.randomSplit([.1,.1,.1])

df_amorstra_final = (
    df1.select('consumer_id',lit('Grupo 1').alias('grupos')).limit(10000)
    .union(df2.select('consumer_id',lit('Grupo 2').alias('grupos')).limit(10000))
    .union(df3.select('consumer_id',lit('Grupo 3').alias('grupos')).limit(10000))
)

df_amorstra_final.count()

# COMMAND ----------

df_amorstra_final.show(3)

# COMMAND ----------

# DBTITLE 1,Amostra de 10mil (mixed, credit, balance)
df_amostra_10k_mixed = df_amostra.filter(col('payment_distribution') == 'mixed')\
  .sample(
    withReplacement = False, # sem reposição
    fraction = .5,
    seed = 123
  ).limit(10000)

df_amostra_10k_mixed.count()

# COMMAND ----------

# DBTITLE 1,Função
def _aleatorio_payment_ (tipo):
    base = (
        df_amostra
        .filter(col('payment_distribution') == tipo)
        .sample(withReplacement = False,fraction = .5,seed = 123).limit(10000)
    )
    return base

# COMMAND ----------

df_amostra_10k_payment = (
    _aleatorio_payment_(tipo='mixed')
    .union(_aleatorio_payment_(tipo='credit'))
    .union(_aleatorio_payment_(tipo='balance'))
)

df_amostra_10k_payment.count()

# COMMAND ----------

df_amostra_10k_payment.display()

# COMMAND ----------

display(
    df_amostra_10k_payment.groupBy('payment_distribution').agg(count('*'))
)

# COMMAND ----------

# MAGIC %md # 13. Looping

# COMMAND ----------

df_p2p.limit(3).display()

# COMMAND ----------

# DBTITLE 1,Data de referência
meses_ref = (
df_p2p
  .select(
    max('created_date').alias('mes_referencia')
  )
)

meses_ref.display()

# COMMAND ----------

for i in range(1,7):
    print(i)

# COMMAND ----------

[i for i in range(6)]

# COMMAND ----------

# DBTITLE 1,Looping pra criar e nomear vários campos
for i in range(8):
  meses_ref = meses_ref\
    .withColumn('m_' + str(i), regexp_replace(substring(add_months('mes_referencia',-i),1,7),'-','/'))
  globals()['m_' + str(i)] = meses_ref.first()['m_' + str(i)]

meses_ref.display()

# COMMAND ----------

m_3

# COMMAND ----------

# MAGIC %md # 14. Incrermental de tabelas

# COMMAND ----------

# DBTITLE 1,Usando a tabela de MAT35 de p2p gerada como exemplo
## caminho: 's3://picpay-datalake-sandbox/felipebarreto/self_service_analytics/df_mat_p2p_parquet'

OUTPUT_PATH      = 's3://picpay-datalake-sandbox/felipebarreto/self_service_analytics/'
OUTPUT_TABLE     = 'df_mat_p2p_parquet/'
OUTPUT_PATH_FULL = 's3://picpay-datalake-sandbox/felipebarreto/self_service_analytics/df_mat_p2p_parquet'

df_s3.display()

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled', True)
spark.conf.set('spark.sql.shuffle.partitions', 'auto')
spark.conf.set('spark.sql.join.preferSortMergeJoin', True)
spark.conf.set('spark.sql.adaptive.skewedJoin.enabled', True)  
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', True)
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

#############################
delta_dias = 5     # dias que devem sobrescrever na tabela existente
gordura_dias = 35   # dias que devem ser retirados do começo do output (quando é uma métrica móvel de dias, como MAT 35)

try:
  data_maxima = spark.read.parquet(OUTPUT_PATH_FULL)\
      .agg(max('Data')).collect()[0][0]
  data_inicio = date_sub(lit(data_maxima), delta_dias+gordura_dias)
  mode_execution = 'overwrite'
  print(f"""
  -> Tabela existe. Será gravada a partição do dia em D-{delta_dias}\n
  -> Rodou para {delta_dias + gordura_dias} dias. Porém, será retirada essa gordura de {gordura_dias} dias devido ao cálculo do MAT 35\n
  """)

except:
  data_inicio = lit('2022-01-01')
  mode_execution = 'overwrite'
  print('Tabela não existe. Grava full load desde 2022')

## cortará o df final nos dias de defasagem para o cálculo do MAT
data_corte = date_add(data_inicio, gordura_dias)

## Deve existir no df final as colunas
# date_format(col('created_date'), 'yyyy').alias('year_partition')
# date_format(col('created_date'), 'MM').alias('month_partition')
# date_format(col('created_date'), 'dd').alias('day_partition')

## gravação final automática (não precisa colocar!)
# _output_df_.write.saveAsTable(
#     "self_service_analytics.nome_tabela", 
#     path="s3://picpay-self-service-tables/self_service_analytics/nome_tabela", 
#     format="parquet", 
#     mode="overwrite", 
#     partitionBy=["year_partition","month_partition","day_partition"]
# )

# COMMAND ----------

_crossjoin_mat(
    df = df_p2p, kpi='mat'
    ).select(
        '*'
        ,date_format(col('calendar_date'), 'yyyy').alias('year_partition')
        ,date_format(col('calendar_date'), 'MM').alias('month_partition')
        ,date_format(col('calendar_date'), 'dd').alias('day_partition')
    ).write.saveAsTable(
    "self_service_analytics.nome_tabela", 
    path="s3://picpay-self-service-tables/self_service_analytics/nome_tabela", 
    format="parquet", 
    mode="overwrite", 
    partitionBy=["year_partition","month_partition","day_partition"]
)

# COMMAND ----------

# DBTITLE 1,Pra ficar mais claro
df_s3.select(
        lit(data_maxima).alias('data_maxima'),
        lit(data_inicio).alias('data_inicio'),
        lit(data_corte).alias('data_corte')
    ).dropDuplicates().display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md # VALIDAÇÕES

# COMMAND ----------


