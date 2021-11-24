pyspark --jars /opt/spark/mysql_connector/mysql-connector-java-8.0.27.jar

''' COMANDOS DO SPARK PARA O MYSQL '''

import mysql.connector
db=mysql.connector.connect(host="your host", user="your username", password="your_password",database="database_name")
db_cursor=db.cursor()
db_cursor.execute("COMANDO")
for item in cursor:
   print(item)

# count()

db=mysql.connector.connect(host="localhost", user="hadoop", password="hadoop",database="Dados_ANAC")
db_cursor=db.cursor()

db_cursor.execute("SELECT COUNT(*) FROM Registros;")

db_cursor.execute("SELECT COUNT(*) FROM Dados_ANAC.Voos;")

db=mysql.connector.connect(host="localhost", user="hadoop", password="hadoop",database="Dados_FAB")
db_cursor=db.cursor()

db_cursor.execute("SELECT COUNT(*) FROM Dados_FAB.Missao_Aerea;")

db_cursor.execute("SELECT COUNT(*) FROM Dados_FAB.Rota;")

# somar ocorrências

db=mysql.connector.connect(host="localhost", user="hadoop", password="hadoop",database="Dados_ANAC")
db_cursor=db.cursor()

db_cursor.execute("SELECT cod_empresa, COUNT(cod_empresa) \
    FROM Registros \
    GROUP BY cod_empresa \
    ORDER BY COUNT(cod_empresa) DESC;")

db_cursor.execute("SELECT situacao, COUNT(situacao) \
    FROM Voos \
    GROUP BY situacao;")

db = mysql.connector.connect(host="localhost", user="hadoop", password="hadoop",database="Dados_FAB")
db_cursor=db.cursor()

db_cursor.execute("SELECT nome_tripulante_1p, COUNT(nome_tripulante_1p) \
    FROM Missao_Aerea \
    GROUP BY nome_tripulante_1p \
    ORDER BY COUNT(nome_tripulante_1p) DESC;")

db_cursor.execute("SELECT dep, arr, COUNT(*) \
    FROM Rota \
    GROUP BY dep, arr \
    ORDER BY COUNT(*) DESC;")

# join

db = mysql.connector.connect(host="localhost", user="hadoop", password="hadoop",database="dados_FAB")
db_cursor = db.cursor()

db_cursor.execute("SELECT * \
    FROM Missao_Aerea \
    INNER JOIN Rota \
    ON Missao_Aerea.ordem_de_missao = Rota.ordem_de_missao;")

''' COMANDOS DO SPARK PARA O HIVE '''

# count()

spark.sql("use Dados_ANAC")

spark.sql("SELECT COUNT(*) FROM Registros").show()

spark.sql("SELECT COUNT(*) FROM Voos").show()

spark.sql("use Dados_FAB")

spark.sql("SELECT COUNT(*) FROM Missao_aerea").show()

spark.sql("SELECT COUNT(*) FROM Rota").show()

# somar ocorrências

spark.sql("USE Dados_ANAC")

spark.sql("SELECT cod_empresa, COUNT(cod_empresa) \
            FROM Registros \
            GROUP BY cod_empresa \
            ORDER BY COUNT(cod_empresa) DESC").show()

spark.sql("SELECT situacao, COUNT(situacao) \
    FROM Voos \
    GROUP BY situacao").show()

spark.sql("USE Dados_FAB");

spark.sql("SELECT nome_tripulante_1p, COUNT(nome_tripulante_1p) \
    FROM Missao_Aerea \
    GROUP BY nome_tripulante_1p \
    ORDER BY COUNT(nome_tripulante_1p) DESC").show()

spark.sql("SELECT dep, arr, COUNT(*) \
    FROM Rota \
    GROUP BY dep, arr \
    ORDER BY COUNT(*) DESC").show()

# join

spark.sql("SELECT * \
    FROM Missao_Aerea \
    INNER JOIN Rota \
    ON Missao_Aerea.ordem_de_missao = Rota.ordem_de_missao").show()

''' COMANDOS SQL ''' 

# count()

USE Dados_ANAC

SELECT COUNT(*) FROM Registros;

SELECT COUNT(*) FROM Voos;

USE Dados_FAB

SELECT COUNT(*) FROM Missao_Aerea;

SELECT COUNT(*) FROM Rota;

# somar ocorrências

USE Dados_ANAC;

SELECT cod_empresa, COUNT(cod_empresa)
    FROM Registros
    GROUP BY cod_empresa
    ORDER BY COUNT(cod_empresa) DESC;
    
SELECT situacao, COUNT(situacao)
    FROM Voos
    GROUP BY situacao;

USE Dados_FAB;

SELECT nome_tripulante_1p, COUNT(nome_tripulante_1p)
    FROM Missao_Aerea
    GROUP BY nome_tripulante_1p
    ORDER BY COUNT(nome_tripulante_1p) DESC;

SELECT dep, arr, COUNT(*)
    FROM Rota
    GROUP BY dep, arr
    ORDER BY COUNT(*) DESC;

# join

USE Dados_FAB;

SELECT *
    FROM Missao_Aerea
    INNER JOIN Rota
    ON Missao_Aerea.ordem_de_missao = Rota.ordem_de_missao;
