''' INICIALIZAÇÃO DO SPARK COM O CONECTOR PARA MYSQL '''

pip install mysql-connector-python

pyspark --jars /opt/spark/mysql_connector/mysql-connector-java-8.0.27.jar


''' KILL SPARK APPLICATION SE DEIXAR LIGADO SEM QUERER '''

ps -ux | grep java
kill -9 728585

''' COMANDOS BÁSICOS DO SPARK ACESSANDO O MySQL '''

import mysql.connector

db=mysql.connector.connect(host="your host", user="your username", password="your_password",database="database_name")
db_cursor=db.cursor()
db_cursor.execute("COMANDO")
for item in cursor:
   print(item)

''' COMANDOS BÁSICOS DO SPARK ACESSANDO O Hive '''

spark.sql("COMANDO SQL").show()

''' PASSAR DO csv PARA O SQL (Spark) '''

# ler tabelas

from pyspark.sql.types import StructType, IntegerType, StringType, LongType, TimestampType
from time import time

schema_Voos = StructType() \
    .add("id_Voos",LongType(),True) \
    .add("cod_empresa",StringType(),True) \
    .add("num_voo",IntegerType(),True) \
    .add("cod_autorizacao",IntegerType(),True) \
    .add("cod_tipo_linha",StringType(),True) \
    .add("cod_origem",StringType(),True) \
    .add("cod_destino",StringType(),True) \
    .add("hor_partida_prevista",StringType(),True) \
    .add("hor_partida_real",StringType(),True) \
    .add("hor_chegada_prevista",StringType(),True) \
    .add("hor_chegada_real",StringType(),True) \
    .add("situacao",StringType(),True) \
    .add("cod_justificativa",StringType(),True)
df_Voos = spark.read.format("csv") \
      .option("header", False) \
      .schema(schema_Voos) \
      .load("/user/baccarin/Voos.csv")

schema_Registros = StructType() \
    .add("id_Registros",LongType(),True) \
    .add("cod_empresa",StringType(),True) \
    .add("empresa",StringType(),True) \
    .add("n_voo",IntegerType(),True) \
    .add("equip",StringType(),True) \
    .add("seg",IntegerType(),True) \
    .add("ter",IntegerType(),True) \
    .add("qua",IntegerType(),True) \
    .add("qui",IntegerType(),True) \
    .add("sex",IntegerType(),True) \
    .add("sab",IntegerType(),True) \
    .add("dom",IntegerType(),True) \
    .add("quant_assentos",IntegerType(),True) \
    .add("n_siros",StringType(),True) \
    .add("situacao_siros",StringType(),True) \
    .add("data_registro",TimestampType(),True) \
    .add("inicio_operacao",TimestampType(),True) \
    .add("fim_operacao",TimestampType(),True) \
    .add("natureza_operacao",StringType(),True) \
    .add("n_etapa",IntegerType(),True) \
    .add("cod_origem",StringType(),True) \
    .add("arpt_origem",StringType(),True) \
    .add("cod_destino",StringType(),True) \
    .add("arpt_destino",StringType(),True) \
    .add("horario_partida",TimestampType(),True) \
    .add("horario_chegada",TimestampType(),True) \
    .add("tipo_servico",StringType(),True) \
    .add("objeto_transporte",StringType(),True) \
    .add("codeshare",StringType(),True)
df_Registros = spark.read.format("csv") \
      .option("header", False) \
      .schema(schema_Registros) \
      .load("/user/baccarin/Registros.csv")

schema_missao_aerea = StructType() \
    .add("ordem_de_missao",LongType(),True) \
    .add("data",StringType(),True) \
    .add("cumprimento",StringType(),True) \
    .add("aviacao",StringType(),True) \
    .add("aeronave",StringType(),True) \
    .add("esquadrao",StringType(),True) \
    .add("id_tripulante_1p",LongType(),True) \
    .add("nome_tripulante_1p",StringType(),True) \
    .add("id_tripulante_2p",LongType(),True) \
    .add("nome_tripulante_2p",StringType(),True)
df_missao_aerea = spark.read.format("csv") \
      .option("header", False) \
      .schema(schema_missao_aerea) \
      .load("/user/baccarin/tabelas/missao_aerea_n.csv")

schema_rota = StructType() \
    .add("id_rota",LongType(),True) \
    .add("dep",StringType(),True) \
    .add("arr",StringType(),True) \
    .add("distancia",IntegerType(),True) \
    .add("tempo_de_voo",IntegerType(),True) \
    .add("ordem_de_missao",LongType(),True) \
    .add("observacoes",StringType(),True)
df_rota = spark.read.format("csv") \
      .option("header", False) \
      .schema(schema_rota) \
      .load("/user/baccarin/tabelas/rota_n.csv")   
df_rota = df_rota.drop("id_rota")

df_Voos.show()
df_Registros.show()
df_missao_aerea.show()
df_rota.show()

# Voos

inicio_voos_csvsql = time()
df_Voos.write.format('jdbc').options( \
      url='jdbc:mysql://thunderbird01:3306/testes', \
      driver='com.mysql.jdbc.Driver', \
      dbtable='Voos_tempo', \
      user='hadoop', \
      password='hadoop').mode('overwrite').save()
fim_voos_csvsql = time()
tempo_voo_csvsql = fim_voos_csvsql - inicio_voos_csvsql
tempo_voo_csvsql

# Registros

inicio_registros_csvsql = time()
df_Registros.write.format('jdbc').options( \
      url='jdbc:mysql://thunderbird01:3306/Dados_ANAC', \
      driver='com.mysql.jdbc.Driver', \
      dbtable='Registros_tempo_tres', \
      user='hadoop', \
      password='hadoop').mode('overwrite').save()
fim_registros_csvsql = time()
tempo_registros_csvsql = fim_registros_csvsql - inicio_registros_csvsql
tempo_registros_csvsql

# Missao_Aerea

inicio_missao_csvsql = time()
df_missao_aerea.write.format('jdbc').options( \
      url='jdbc:mysql://thunderbird01:3306/dados_FAB_novo', \
      driver='com.mysql.jdbc.Driver', \
      dbtable='Missao_Aerea_tempo', \
      user='hadoop', \
      password='hadoop').mode('overwrite').save()
fim_missao_csvsql = time()
tempo_missao_csvsql = fim_missao_csvsql - inicio_missao_csvsql
tempo_missao_csvsql

# Rota

inicio_rota_csvsql = time()
df_rota.write.format('jdbc').options( \
      url='jdbc:mysql://thunderbird01:3306/dados_FAB_novo', \
      driver='com.mysql.jdbc.Driver', \
      dbtable='Rota_tempo', \
      user='hadoop', \
      password='hadoop').mode('overwrite').save()
fim_rota_csvsql = time()
tempo_rota_csvsql = fim_rota_csvsql - inicio_rota_csvsql
tempo_rota_csvsql

''' PASSAR DO csv PARA O HIVE (Spark)'''

# ler tabelas

from pyspark.sql.types import StructType, IntegerType, StringType, LongType, TimestampType
from time import time

schema_Voos = StructType() \
    .add("id_Voos",LongType(),True) \
    .add("cod_empresa",StringType(),True) \
    .add("num_voo",IntegerType(),True) \
    .add("cod_autorizacao",IntegerType(),True) \
    .add("cod_tipo_linha",StringType(),True) \
    .add("cod_origem",StringType(),True) \
    .add("cod_destino",StringType(),True) \
    .add("hor_partida_prevista",StringType(),True) \
    .add("hor_partida_real",StringType(),True) \
    .add("hor_chegada_prevista",StringType(),True) \
    .add("hor_chegada_real",StringType(),True) \
    .add("situacao",StringType(),True) \
    .add("cod_justificativa",StringType(),True)
df_Voos = spark.read.format("csv") \
      .option("header", False) \
      .schema(schema_Voos) \
      .load("/user/baccarin/Voos.csv")

schema_Registros = StructType() \
    .add("id_Registros",LongType(),True) \
    .add("cod_empresa",StringType(),True) \
    .add("empresa",StringType(),True) \
    .add("n_voo",IntegerType(),True) \
    .add("equip",StringType(),True) \
    .add("seg",IntegerType(),True) \
    .add("ter",IntegerType(),True) \
    .add("qua",IntegerType(),True) \
    .add("qui",IntegerType(),True) \
    .add("sex",IntegerType(),True) \
    .add("sab",IntegerType(),True) \
    .add("dom",IntegerType(),True) \
    .add("quant_assentos",IntegerType(),True) \
    .add("n_siros",StringType(),True) \
    .add("situacao_siros",StringType(),True) \
    .add("data_registro",TimestampType(),True) \
    .add("inicio_operacao",TimestampType(),True) \
    .add("fim_operacao",TimestampType(),True) \
    .add("natureza_operacao",StringType(),True) \
    .add("n_etapa",IntegerType(),True) \
    .add("cod_origem",StringType(),True) \
    .add("arpt_origem",StringType(),True) \
    .add("cod_destino",StringType(),True) \
    .add("arpt_destino",StringType(),True) \
    .add("horario_partida",TimestampType(),True) \
    .add("horario_chegada",TimestampType(),True) \
    .add("tipo_servico",StringType(),True) \
    .add("objeto_transporte",StringType(),True) \
    .add("codeshare",StringType(),True)
df_Registros = spark.read.format("csv") \
      .option("header", False) \
      .schema(schema_Registros) \
      .load("/user/baccarin/Registros.csv")

schema_missao_aerea = StructType() \
    .add("ordem_de_missao",LongType(),True) \
    .add("data",StringType(),True) \
    .add("cumprimento",StringType(),True) \
    .add("aviacao",StringType(),True) \
    .add("aeronave",StringType(),True) \
    .add("esquadrao",StringType(),True) \
    .add("id_tripulante_1p",LongType(),True) \
    .add("nome_tripulante_1p",StringType(),True) \
    .add("id_tripulante_2p",LongType(),True) \
    .add("nome_tripulante_2p",StringType(),True)
df_missao_aerea = spark.read.format("csv") \
      .option("header", False) \
      .schema(schema_missao_aerea) \
      .load("/user/baccarin/tabelas/missao_aerea_n.csv")

schema_rota = StructType() \
    .add("id_rota",LongType(),True) \
    .add("dep",StringType(),True) \
    .add("arr",StringType(),True) \
    .add("distancia",IntegerType(),True) \
    .add("tempo_de_voo",IntegerType(),True) \
    .add("ordem_de_missao",LongType(),True) \
    .add("observacoes",StringType(),True)
df_rota = spark.read.format("csv") \
      .option("header", False) \
      .schema(schema_rota) \
      .load("/user/baccarin/tabelas/rota_n.csv")   
df_rota = df_rota.drop("id_rota")

df_Voos.show()
df_Registros.show()
df_missao_aerea.show()
df_rota.show()

# Voos
inicio_voos_csvhive = time()
df_Voos.write.mode("overwrite").saveAsTable("teste.Voos_tempo")
fim_voos_csvhive = time()
tempo_voos_csvhive = fim_voos_csvhive - inicio_voos_csvhive
tempo_voos_csvhive

# Registros
inicio_registros_csvhive = time()
df_Registros.write.mode("overwrite").saveAsTable("teste.Registros_tempo")
fim_registros_csvhive = time()
tempo_registros_csvhive = fim_registros_csvhive - inicio_registros_csvhive
tempo_registros_csvhive

# Missao_Aerea

inicio_missao_csvhive = time()
df_missao_aerea.write.mode("overwrite").saveAsTable("dados_FAB_novo.Missao_Aerea_tempo")
fim_missao_csvhive = time()
tempo_missao_csvhive = fim_missao_csvhive - inicio_missao_csvhive
tempo_missao_csvhive

# Rota

inicio_rota_csvhive = time()
df_rota.write.mode("overwrite").saveAsTable("dados_FAB_novo.Rota_tempo")
fim_rota_csvhive = time()
tempo_rota_csvhive = fim_rota_csvhive - inicio_rota_csvhive
tempo_rota_csvhive

''' PASSAR DO MYSQL PARA O HIVE (Spark)'''

# Voos

df_Voos = spark.read.format("jdbc"). \
    option("url", "jdbc:mysql://thunderbird02:3306/Dados_ANAC"). \
    option("driver", "com.mysql.jdbc.Driver"). \
    option("dbtable", "Voos"). \
    option("user", "root").option("password", "t33a").load()
df_Voos.printSchema()
df_Voos.show()
from time import time
inicio = time()
df_Voos.write.mode("overwrite").saveAsTable("teste.Voos_tempo_tres")
fim = time()

# Registros

df_Registros = spark.read.format("jdbc"). \
    option("url", "jdbc:mysql://thunderbird01:3306/Dados_ANAC"). \
    option("driver", "com.mysql.jdbc.Driver"). \
    option("dbtable", "Registros"). \
    option("user", "hadoop").option("password", "hadoop").load()
df_Registros.printSchema()
df_Registros.show()
df_Registros.write.mode("overwrite").saveAsTable("Dados_ANAC.Registros")

# Missao_Aerea

inicio_missao_sqlhive = time()
df_Missao_Aerea = spark.read.format("jdbc"). \
    option("url", "jdbc:mysql://thunderbird01:3306/Dados_FAB"). \
    option("driver", "com.mysql.jdbc.Driver"). \
    option("dbtable", "Missao_Aerea"). \
    option("user", "hadoop").option("password", "hadoop").load()
df_Missao_Aerea.write.mode("overwrite").saveAsTable("teste.Missao_Aerea_tempo_catorze")
fim_missao_sqlhive = time()
tempo_missao_sqlhive = fim_missao_sqlhive - inicio_missao_sqlhive
tempo_missao_sqlhive

# Rota

inicio_rota_sqlhive = time()
df_Rota = spark.read.format("jdbc"). \
    option("url", "jdbc:mysql://thunderbird01:3306/Dados_FAB"). \
    option("driver", "com.mysql.jdbc.Driver"). \
    option("dbtable", "Rota"). \
    option("user", "hadoop").option("password", "hadoop").load()
df_Rota.write.mode("overwrite").saveAsTable("teste.rota_tempo_catorze")
fim_rota_sqlhive = time()
tempo_rota_sqlhive = fim_rota_sqlhive - inicio_rota_sqlhive
tempo_rota_sqlhive
