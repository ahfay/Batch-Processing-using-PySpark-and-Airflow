import os
import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from sqlalchemy import create_engine

# spark Session
def spark_get_session():
    return SparkSession.builder \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .master("local") \
            .appName("PySpark_Postgres").getOrCreate()

# func config
def config(connection_db):
    path = os.getcwd()
    with open(os.path.join('/opt/airflow/dags/modules/', 'credential.json')) as file:
        conf = json.load(file)[connection_db]
        return conf
    
# func spark_get_conn
def spark_get_conn(table_name):
    conf = config("source")
    return spark_get_session().read.format("jdbc") \
            .option("url", conf["url"]) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", table_name) \
            .option("user", conf["user"]) \
            .option("password", conf["password"]).load()   
            
def tidb_get_conn():
    conf = config("target")
    return create_engine(conf["url"], echo=False)
