#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco, Maximilian Engelhardt
#***************************************************************************/

# NB: THIS SCRIPT REQUIRES A SPARK 3 CLUSTER

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------

from pyspark.sql import SparkSession
from datetime import datetime
import sys
import configparser

config = configparser.ConfigParser()
config.read("/app/mount/parameters.conf")
s3BucketName=config.get("general","s3BucketName")
username=config.get("general","username")

print("Running as Username: ", username)

spark = SparkSession \
    .builder \
    .appName("ICEBERG LOAD") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .getOrCreate()

print("TOP 20 ROWS IN CAR SALES TABLE")
spark.sql("SELECT * FROM {}_CAR_DATA.car_sales".format(username)).show()

print("\n")
print("CAR SALES TABLE PRE-ICEBERG MIGRATION PARTITIONS: ")
print("SHOW PARTITIONS {}_CAR_DATA.CAR_SALES".format(username))
spark.sql("SHOW PARTITIONS {}_CAR_DATA.CAR_SALES".format(username)).show()

#----------------------------------------------------
#               MIGRATE SPARK TABLES TO ICEBERG TABLE
#----------------------------------------------------
try:
    print("ALTER TABLE {}_CAR_DATA.CAR_SALES UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
    spark.sql("ALTER TABLE {}_CAR_DATA.CAR_SALES UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
    print("CALL spark_catalog.system.migrate('{}_CAR_DATA.CAR_SALES')".format(username))
    spark.sql("CALL spark_catalog.system.migrate('{}_CAR_DATA.CAR_SALES')".format(username))
    print("Migrated the Car Sales Table to Iceberg Format.")
except Exception as e:
    print(e)
    print("The Car Sales table has already been migrated to Iceberg Format.")

try:
    print("ALTER TABLE {}_CAR_DATA.CUSTOMER_DATA UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
    spark.sql("ALTER TABLE {}_CAR_DATA.CUSTOMER_DATA UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
    print("CALL spark_catalog.system.migrate('{}_CAR_DATA.CUSTOMER_DATA')".format(username))
    spark.sql("CALL spark_catalog.system.migrate('{}_CAR_DATA.CUSTOMER_DATA')".format(username))
    print("Migrated the Customer Data table to Iceberg Format.")
except Exception as e:
    print(e)
    print("The Customer Data table has already been migrated to Iceberg.")

print("\n")
print("DESCRIBE TABLE spark_catalog.{}_CAR_DATA.CAR_SALES".format(username))
spark.sql("DESCRIBE TABLE spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).show(20, False)

print("CAR SALES TABLE POST-ICEBERG MIGRATION PARTITIONS: ")
spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES.PARTITIONS".format(username)).show()

#---------------------------------------------------
#               SHOW ICEBERG TABLE SNAPSHOTS
#---------------------------------------------------

spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES.history".format(username)).show(20, False)
spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES.snapshots".format(username)).show(20, False)

# STORE TIMESTAMP BEFORE INSERTS
now = datetime.now()
timestamp = datetime.timestamp(now)
print("PRE-INSERT TIMESTAMP: ", timestamp)

#---------------------------------------------------
#               INSERT DATA
#---------------------------------------------------

# PRE-INSERT COUNT
print("PRE-INSERT COUNT")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).show()

# INSERT DATA VIA DATAFRAME API
temp_df = spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).sample(fraction=0.3, seed=3)
temp_df.writeTo("spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).append()

# INSERT DATA VIA SQL
print("INSERT DATA VIA SPARK SQL")
temp_df.createOrReplaceTempView("CAR_SALES_SAMPLE".format(username))
insert_qry = "INSERT INTO spark_catalog.{0}_CAR_DATA.CAR_SALES SELECT * FROM CAR_SALES_SAMPLE".format(username)
print(insert_qry)
spark.sql(insert_qry)

#---------------------------------------------------
#               TIME TRAVEL
#---------------------------------------------------

# NOTICE SNAPSHOTS HAVE BEEN ADDED
spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES.history".format(username)).show(20, False)
spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES.snapshots".format(username)).show(20, False)

# POST-INSERT COUNT
print("POST-INSERT COUNT")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).show()

# TIME TRAVEL AS OF PREVIOUS TIMESTAMP
df = spark.read.option("as-of-timestamp", int(timestamp*1000)).format("iceberg").load("spark_catalog.{}_CAR_DATA.CAR_SALES".format(username))

# POST TIME TRAVEL COUNT
print("POST-TIME TRAVEL COUNT")
print(df.count())

#---------------------------------------------------
#               INCREMENTAL READ
#---------------------------------------------------

print("\n")
print("INCREMENTAL READ")

print("ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)")
print("SELECT * FROM {}_CAR_DATA.CAR_SALES.history;".format(username))
spark.sql("SELECT * FROM {}_CAR_DATA.CAR_SALES.history;".format(username)).show()

print("ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)")
print("SELECT * FROM {}_CAR_DATA.CAR_SALES.snapshots;".format(username))
spark.sql("SELECT * FROM {}_CAR_DATA.CAR_SALES.snapshots;".format(username)).show()

print("STORE FIRST AND LAST SNAPSHOT ID'S FROM SNAPSHOTS TABLE")
print("SELECT * FROM {}_CAR_DATA.CAR_SALES.snapshots;".format(username))
snapshots_df = spark.sql("SELECT * FROM {}_CAR_DATA.CAR_SALES.snapshots;".format(username))
snapshots_df.show()

last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
first_snapshot = snapshots_df.select("snapshot_id").head(1)[0][0]

print("READ BETWEEN FIRST SNAPSHOP: {0} AND LAST SNAPSHOP: {1}".format(first_snapshot, last_snapshot))
spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", first_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("spark_catalog.{}_CAR_DATA.CAR_SALES".format(username)).show()
