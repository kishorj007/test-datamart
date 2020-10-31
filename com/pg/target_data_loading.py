from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
import com.pg.utils.utility as ut

if __name__ == '__main__':

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"])\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')


    # Read CP and addr data for current date
    # create temp view on top of that
    # execute a spark sql query to get the dim table data


    # Read CP data for current date

    print("\nStart reading data from CP parque file")

    Customer_file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + "CP"
    Customer_df = spark.sql("select * from parquet.`{}`".format(Customer_file_path))
    Customer_df.printSchema()
    Customer_df.show(5, False)
    Customer_df.createOrReplaceTempView("CustomerPortal")

    spark.sql("""SELECT 
                   DISTINCT REGIS_CNSM_ID, CAST(REGIS_CTY_CODE AS SMALLINT), CAST(REGIS_ID AS INTEGER),
                   REGIS_LTY_ID, REGIS_DATE, REGIS_CHANNEL, REGIS_GENDER, REGIS_CITY, INS_DT
                FROM
                  CustomerPortal
                WHERE
                  INS_DT = '2020-10-29'""")\
        .show(5, False)

    # Read addr data for current date

    print("\nStart reading data from Address parque file")

    Address_file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + "ADDR"
    Address_df = spark.sql("select * from parquet.`{}`".format(Customer_file_path))
    Address_df.printSchema()
    Address_df.show(5, False)
    Address_df.createOrReplaceTempView("Address")



    print("Completed   <<<<<<<<<")

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" com/pg/target_data_loading.py
