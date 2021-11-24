''' MONTAR O DATAFRAME '''

df = spark.sql("COMANDO SQL HIVE")

''' SELEÇÃO '''

df_Registros = spark.sql("SELECT * FROM Dados_ANAC.Registros")

df_Voos = spark.sql("SELECT * FROM Dados_ANAC.Voos WHERE cod_empresa='TAM'")

df_Missao_Aerea = spark.sql("SELECT ordem_de_missao, cumprimento, aviacao, aeronave, esquadrao FROM Dados_FAB.Missao_Aerea")

df_Rota = df_Missao_Aerea = spark.sql("SELECT * FROM Dados_FAB.Rota")


''' df_Voos e Missao '''

# importar as bibliotecas a serem utilizadas
from pyspark.sql.functions import when
import pandas as pd
import six
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# selecionar os dados

df = spark.sql("SELECT * \
    FROM Dados_FAB.Missao_Aerea \
    INNER JOIN Dados_FAB.Rota \
    ON Dados_FAB.Missao_Aerea.ordem_de_missao = Dados_FAB.Rota.ordem_de_missao")

# explorar e pré-processar os dados

df.count()
df.show()
df.printSchema()

df_2 = df.select('id_tripulante_1p', 'id_tripulante_2p', 'distancia', 'cumprimento')
df_2.show()
df_2.printSchema()

# transformar os dados

df_3 = df_2.withColumn('cumprimento', \
        when(df_2.cumprimento == 'total','2') \
        .when(df_2.cumprimento == 'parcial','1') \
        .when(df_2.cumprimento == 'não cumprida','0'))
df_4 = df_3.withColumn('cumprimento', df_3.cumprimento.cast('Integer')) \
        .withColumn('id_tripulante_1p', df_3.id_tripulante_1p.cast('Integer')) \
        .withColumn('id_tripulante_2p', df_3.id_tripulante_2p.cast('Integer'))
df_4 = df_4.withColumn('cumprimento', df_4.cumprimento * 5)

df_4.show()
df_4.printSchema()

# minerar

# descobrir correlações

numeric_features = [t[0] for t in df_4.dtypes if t[1] == 'int' or t[1] == 'double']
sampled_data = df_4.select(numeric_features).sample(False, 0.8).toPandas()

for i in df_4.columns:
    if not( isinstance(df_4.select(i).take(1)[0][0], six.string_types)):
        print( "Correlation to cumprimento for ", i, df_4.stat.corr('cumprimento',i))


assembler = VectorAssembler(inputCols=['id_tripulante_1p', 'id_tripulante_2p', 'distancia'], \
            outputCol='features')
df_out = assembler.transform(df_4)
clean_df = df_out.select(['features', 'cumprimento'])
clean_df.show()

splits = clean_df.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

lr = LinearRegression(featuresCol = 'features', labelCol='cumprimento', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)

print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# descrever os resultados
train_df.describe().show()

# predições
predictions = lr_model.transform(test_df)
predictions.select("prediction","cumprimento","features").show()

# ver acurácia
test_results = fit_model.evaluate(test)
test_results.r2




''' df_Registros '''

# importar as bibliotecas a serem utilizadas
from pyspark.sql.functions import when
import pandas as pd
import six
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# selecionar os dados

df = spark.sql("SELECT * FROM Dados_ANAC.Registros")

# explorar e pré-processar os dados

df.count()
df.printSchema()
df.show()

df_2 = df.select('seg', 'ter', 'qua', 'qui', 'sex', 'sab', 'dom', 'quant_assentos')

df_2.select('seg').distinct().show()  # tem valores nulos

# transformar os dados

df_3 = df_2.na.fill(value=0) # retira todos os valores nulos e substitui por zero (0)

df_3.select('seg').distinct().show()  # não tem mais valores nulos


# minerar

assembler = VectorAssembler(inputCols=['seg', 'ter', 'qua', 'qui', 'sex', 'sab', 'dom'], \
            outputCol='features')
df_out = assembler.transform(df_3)
clean_df = df_out.select(['features', 'quant_assentos'])
clean_df.show()

splits = clean_df.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

lr = LinearRegression(featuresCol = 'features', labelCol='quant_assentos', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)

print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# descrever os resultados
train_df.describe().show()

# predições
predictions = lr_model.transform(test_df)
predictions.select("prediction","quant_assentos","features").show()

# ver acurácia
test_results = fit_model.evaluate(test)
test_results.r2
