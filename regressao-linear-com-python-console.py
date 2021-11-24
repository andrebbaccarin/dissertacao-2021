''' Dados de Voo '''

import pandas as pd
import numpy as np
from   sklearn.linear_model import LinearRegression
from   sklearn.metrics import r2_score
import statsmodels.api as sm
import os

header = ['ordem_de_missao', 'data', 'cumprimento', 'aviacao', 'aeronave', 'esquadrao', 'id_1p', 'nome_1p', 'id_2p', 'nome_2p', 'observacoes']
data = pd.read_csv("/home/hadoop/Desktop/Datasets/tabelas/missao_aerea_n.csv", names = header)

data = data.filter(['id_1p', 'id_2p', 'cumprimento'])

data['cumprimento'].replace({'total': '2', 'parcial': '1', 'não cumprida': '0'}, inplace = True)

data = data.astype({'id_1p': int, 'id_2p': int, 'cumprimento': int})

data['cumprimento'] = 5 * data['cumprimento']

colunas = ['id_1p', 'id_2p']
X = data[colunas]
y = data.cumprimento

mlr_model = LinearRegression(featuresCol = 'features', labelCol='cumprimento', maxIter=10, regParam=0.3, elasticNetParam=0.8)
mlr_model.fit(X, y)

theta0 = mlr_model.intercept_
theta1, theta2 = mlr_model.coef_
theta0, theta1, theta2


''' Registros '''

import pandas as pd
import numpy as np
from   sklearn.linear_model import LinearRegression
from   sklearn.metrics import r2_score
import statsmodels.api as sm
import os

header = ['id', 'cod_e', 'emp', 'nvoo', 'equip', 'seg', 'ter', 'qua', 'qui', 'sex', 'sab', 'dom', 'qtas', 'nsi', 'sitsi' \
          'datareg', 'iniope', 'fimioe', 'natuope', 'netap', 'codori', 'aptorig', 'coddes', 'aptdes', 'horpar', 'horche' \
          'tiposer', 'objt', 'code']
data = pd.read_csv("/home/hadoop/Desktop/registros_s.csv", names = header, error_bad_lines=False)

data = data.filter(['seg', 'ter', 'qua', 'qui', 'sex', 'sab', 'dom', 'qtas'])

data = data.astype({'seg': int, 'ter': int, 'qua': int, 'qui': int, 'sex': int, 'sab': int, 'dom': int})

colunas = ['seg', 'ter', 'qua', 'qui', 'sex', 'sab', 'dom']
X = data[colunas]
y = data.qtas

mlr_model = LinearRegression(featuresCol = 'features', labelCol='qtas', maxIter=10, regParam=0.3, elasticNetParam=0.8)
mlr_model.fit(X, y)

theta0 = mlr_model.intercept_
theta1, theta2 = mlr_model.coef_
theta0, theta1, theta2
