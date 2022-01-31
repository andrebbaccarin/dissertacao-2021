# carregar os dados

df_Registros = spark.sql("SELECT * FROM Dados_ANAC.Registros")

df_Voos = spark.sql("SELECT * FROM Dados_ANAC.Voos WHERE cod_empresa='TAM'")

df_Missao_Aerea = spark.sql("SELECT ordem_de_missao, cumprimento, aviacao, aeronave, esquadrao FROM Dados_FAB.Missao_Aerea")

df_Rota = df_Missao_Aerea = spark.sql("SELECT * FROM Dados_FAB.Rota")


# contagem do tripulante que voou mais vezes da tabela Missão Aérea

spark.sql("SELECT nome_tripulante_1p, COUNT(nome_tripulante_1p) \
    FROM Dados_FAB.Missao_Aerea \
    GROUP BY nome_tripulante_1p \
    ORDER BY COUNT(nome_tripulante_1p) DESC").show()
	

# rota mais voada da tabela Rota

spark.sql("SELECT dep, arr, COUNT(*) \
    FROM Dados_FAB.Rota \
    GROUP BY dep, arr \
    ORDER BY COUNT(*) DESC").show()
    
# descrição da coluna assentos do Registro

df_Registros.select('quant_assentos').describe().show()
    
# precisão do modelo do Registro
# após realizar as operações de Machine Learning descritas no Apêndice \ref{ap:operacoes_pyspark}

print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# gráfico da tabela voos

import pandas as pd
import matplotlib.pyplot as plt
plt.show(block=True)

df2 = df_Voos.groupBy('cod_origem').count()

dfp = df2.toPandas()
dfp.set_index('cod_origem', inplace=True)

dfp.to_csv('arquivo_de_destino_no_computador_do_spark')

''' no computador local '''

import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("arquivo_de_origem_no_computador_local")

df.set_index('cod_origem', inplace=True)

df2 = df.loc[df['count'] > 10000]

plot = df2.plot(kind='bar')
