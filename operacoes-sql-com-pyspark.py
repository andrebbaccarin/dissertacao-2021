pyspark --jars /opt/spark/mysql_connector/mysql-connector-java-8.0.27.jar

''' COMANDOS DO SPARK PARA O MYSQL '''

from time import time
import mysql.connector
db=mysql.connector.connect(host="your host", user="your username", password="your_password",database="database_name")
db_cursor=db.cursor()
db_cursor.execute("COMANDO")
for item in cursor:
   print(item)

# count()

db=mysql.connector.connect(host="localhost", user="hadoop", password="hadoop",database="Dados_ANAC")
db_cursor=db.cursor()

ini_c_reg_sm = time()
db_cursor.execute("SELECT COUNT(*) FROM Registros;")
fin_c_reg_sm = time()
tot_c_reg_sm = fin_c_reg_sm - ini_c_reg_sm
tot_c_reg_sm

ini_c_voo_sm = time()
db_cursor.execute("SELECT COUNT(*) FROM Dados_ANAC.Voos;")
fin_c_voo_sm = time()
tot_c_voo_sm = fin_c_voo_sm - ini_c_voo_sm
tot_c_voo_sm

db=mysql.connector.connect(host="localhost", user="hadoop", password="hadoop",database="Dados_FAB")
db_cursor=db.cursor()

ini_c_mis_sm = time()
db_cursor.execute("SELECT COUNT(*) FROM Dados_FAB.Missao_Aerea;")
fin_c_mis_sm = time()
tot_c_mis_sm = fin_c_mis_sm - ini_c_mis_sm
tot_c_mis_sm

ini_c_rot_sm = time()
db_cursor.execute("SELECT COUNT(*) FROM Dados_FAB.Rota;")
fin_c_rot_sm = time()
tot_c_rot_sm = fin_c_rot_sm - ini_c_rot_sm
tot_c_rot_sm

# somar ocorrências

db=mysql.connector.connect(host="localhost", user="hadoop", password="hadoop",database="Dados_ANAC")
db_cursor=db.cursor()

ini_s_reg_sm = time()
db_cursor.execute("SELECT cod_empresa, COUNT(cod_empresa) \
    FROM Registros \
    GROUP BY cod_empresa \
    ORDER BY COUNT(cod_empresa) DESC;")
fin_s_reg_sm = time()
tot_s_reg_sm = fin_s_reg_sm - ini_s_reg_sm
tot_s_reg_sm

ini_s_voo_sm = time()
db_cursor.execute("SELECT situacao, COUNT(situacao) \
    FROM Voos \
    GROUP BY situacao;")
fin_s_voo_sm = time()
tot_s_voo_sm = fin_s_voo_sm - ini_s_voo_sm
tot_s_voo_sm

db = mysql.connector.connect(host="localhost", user="hadoop", password="hadoop",database="Dados_FAB")
db_cursor=db.cursor()

ini_s_mis_sm = time()
db_cursor.execute("SELECT nome_tripulante_1p, COUNT(nome_tripulante_1p) \
    FROM Missao_Aerea \
    GROUP BY nome_tripulante_1p \
    ORDER BY COUNT(nome_tripulante_1p) DESC;")
fin_s_mis_sm = time()
tot_s_mis_sm = fin_s_mis_sm - ini_s_mis_sm
tot_s_mis_sm

ini_s_rot_sm = time()
db_cursor.execute("SELECT dep, arr, COUNT(*) \
    FROM Rota \
    GROUP BY dep, arr \
    ORDER BY COUNT(*) DESC;")
fin_s_rot_sm = time()
tot_s_rot_sm = fin_s_rot_sm - ini_s_rot_sm
tot_s_rot_sm

# join

db = mysql.connector.connect(host="localhost", user="hadoop", password="hadoop",database="dados_FAB_oito")
db_cursor = db.cursor()

ini_join_sm = time()
db_cursor.execute("SELECT * \
    FROM Missao_Aerea \
    INNER JOIN Rota \
    ON Missao_Aerea.ordem_de_missao = Rota.ordem_de_missao;")
fin_join_sm = time()
tot_join_sm = fin_join_sm - ini_join_sm
tot_join_sm

''' COMANDOS DO SPARK PARA O HIVE '''

# count()

from time import time

spark.sql("use Dados_ANAC")

ini_c_reg = time()
spark.sql("SELECT COUNT(*) FROM registros").show()
fin_c_reg = time()
tot_c_reg = fin_c_reg - ini_c_reg
tot_c_reg

ini_c_voo = time()
spark.sql("SELECT COUNT(*) FROM voos").show()
fin_c_voo = time()
tot_c_voo = fin_c_voo - ini_c_voo
tot_c_voo

spark.sql("use Dados_FAB")

ini_c_mis = time()
spark.sql("SELECT COUNT(*) FROM missao_aerea").show()
fin_c_mis = time()
tot_c_mis = fin_c_mis - ini_c_mis
tot_c_mis

ini_c_rota = time()
spark.sql("SELECT COUNT(*) FROM rota").show()
fin_c_rota = time()
tot_c_mis = fin_c_rota - ini_c_rota
tot_c_mis

# somar ocorrências

from time import time

spark.sql("USE Dados_ANAC")

ini_s_mis = time()
spark.sql("SELECT cod_empresa, COUNT(cod_empresa) \
            FROM Registros \
            GROUP BY cod_empresa \
            ORDER BY COUNT(cod_empresa) DESC").show()
fin_s_mis = time()
tot_s_mis = fin_s_mis - ini_s_mis
tot_s_mis

ini_s_voo = time()
spark.sql("SELECT situacao, COUNT(situacao) \
    FROM Voos \
    GROUP BY situacao").show()
fin_s_voo = time()
tot_s_voo = fin_s_voo - ini_s_voo
tot_s_voo

spark.sql("USE Dados_FAB");

ini_s_mis = time()
spark.sql("SELECT nome_tripulante_1p, COUNT(nome_tripulante_1p) \
    FROM Missao_Aerea \
    GROUP BY nome_tripulante_1p \
    ORDER BY COUNT(nome_tripulante_1p) DESC").show()
fin_s_mis = time()
tot_s_mis = fin_s_mis - ini_s_mis
tot_s_mis

ini_s_rot = time()
spark.sql("SELECT dep, arr, COUNT(*) \
    FROM Rota \
    GROUP BY dep, arr \
    ORDER BY COUNT(*) DESC").show()
fin_s_rot = time()
tot_s_rot = fin_s_rot - ini_s_rot
tot_s_rot

# join

ini_join = time()
spark.sql("SELECT * \
    FROM Missao_Aerea \
    INNER JOIN Rota \
    ON Missao_Aerea.ordem_de_missao = Rota.ordem_de_missao").show()
fin_join = time()
tot_join = fin_join - ini_join
tot_join

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
