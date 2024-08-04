# Databricks notebook source
# Crear base de datos
db = 'S09_DB6_JOCAS'

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

# Seleccionar la base de datos
spark.sql(f"USE {db}")


# Preparar base de datos y configurar para habilitar Delta Lake
spark.sql("SET spark.databricks.delta.formatCheck.enabled = false")
spark.sql("SET spark.databricks.delta.properties.autoOptimize.optimizeWrite = true")

# COMMAND ----------

# Declarar variables para la conexión con la base de datos postgre

driver = "org.postgresql.Driver"
database_host = "databricksdmc.postgres.database.azure.com"
database_port = "5432"
database_name = "northwind"
database_user = "grupo6_antony"
password = "Especializacion6"
table = "orders"

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

# COMMAND ----------

sql_order = (spark.read.
             format("jdbc").
             option("driver",driver).
             option("url",url).
             option("dbtable",table).
             option("user",database_user).
             option("password",password).
             load())

# COMMAND ----------

sql_order.display()

# COMMAND ----------

tables = ["orders","customer_customer_demo","customers","employees", "order_details", "region","territories","suppliers"]

sql_order = (spark.read.
             format("jdbc").
             option("driver",driver).
             option("url",url).
             option("dbtable",table).
             option("user",database_user).
             option("password",password).
             load())


# COMMAND ----------

tables = ["orders", "customer_customer_demo", "customers", "employees", "order_details", "region", "territories", "suppliers"]

# Diccionario para almacenar los DataFrames
dataframes = {}

# Leer todas las tablas y almacenarlas en el diccionario
for table in tables:
    dataframes[table] = (spark.read
                         .format("jdbc")
                         .option("driver", driver)
                         .option("url", url)
                         .option("dbtable", table)
                         .option("user", database_user)
                         .option("password", password)
                         .load())




# COMMAND ----------

orders_df = dataframes["orders"]
orders_df.display()

# COMMAND ----------

customers_df = dataframes["customers"]
customers_df.display()

# COMMAND ----------

sql_order.write.format("delta").mode("overwrite").saveAsTable("S09_DB6_JOCAS.orders")

# COMMAND ----------


for table in tables:
    options = {'driver':driver, "url":url,"dbtable":table,"user":database_user,"password":password }
    dt_table = spark.read.format("jdbc").options(**options).load()
    table = "S09_DB6_JOCAS."+table
    dt_table.write.format("delta").mode("overwrite").saveAsTable(table)
