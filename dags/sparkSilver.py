import configparser, argparse

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import udf, col, trim, count
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import from_unixtime, to_timestamp, expr, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,BooleanType,DoubleType,LongType, ArrayType


#---------------------Get Input/output/config path -----------------------------------

""" This function get input/output/config path as param from spark-submit
    All arguments are optional
    inputpath  => standard path for read data
    outputpath => standard path to write data
    configpath => path for get credentials. This is ".cfg" type
    localRun    = Y/N => param to spark

    If input/output/config/local is not informed, will working using
        Input_path = 's3a://bees-breweries/"
        Output_path = 's3a://bees-breweries" (this is my bucket)
        Configpath  = has no defaul value
        localRun    = "y"

    Examples:
        spark-submit spark_docker.py --inputpath s3a://bees-breweries --outputpath s3a://bees-breweries --configpath dl.cfg
                                     --localrun y

        To get help: spark-submit spark_docker.py --help

    Return input_path, output_path, config_path, localrun
"""

def get_path():
    # Initiate the parser
    parser = argparse.ArgumentParser(description='Path for input/output/config files')

    # Add long and short argument
    parser.add_argument("--inputpath", "-inputpath", help="The URI for Bronze bucket location like s3a://bees-breweries")
    parser.add_argument("--outputpath", "-outputpath", help="The URI for your S3 bucket location like s3a://bees-breweries")
    parser.add_argument("--configpath", "-configtpath", help="The URI for your AWS config file like /home/data/dl.cfg")
    parser.add_argument("--localrun", "-localrun", help="N=default (NO), Y=run in local mode (yes)")

    # Read arguments from the command line
    args = parser.parse_args()

    # Check for --inputpath
    if args.inputpath:
        inputpath=args.inputpath
    else:
        inputpath = 's3a://bees-breweries/bronze/'

    # Check for --outputpath
    if args.outputpath:
        outputpath = args.outputpath
    else:
        outputpath ='s3a://bees-breweries/silver/'

    if not args.localrun or (args.localrun.lower() != 'y' and 'n'):
        localrun = 'y'
    else:
        localrun = args.localrun.lower()


    return inputpath,\
            outputpath,\
            localrun,\
           '' if args.configpath is None else args.configpath



#-----------------GET AWS Credentials  ------------------------------------------------

""" This function get AWS credential from configpath
    If the configpath is unacessable, return empty values

    Input:  config_path
    Output: Return AWS Credentials (ACCESS, SECRET, TOKEN) or empty values

"""

def get_credentials(config_path):
    
    config = configparser.ConfigParser()

    try:
        with open(config_path) as credentials:
            config.read_file(credentials)
            return config['AWS']['AWS_ACCESS_KEY_ID'],\
                   config['AWS']['AWS_SECRET_ACCESS_KEY'],\
                   config['AWS']['AWS_SESSION_TOKEN']

    except IOError:
        return "", "", ""
   

    return aws_access_key_id, aws_secret_access_key



#-----------------Working with Spark Session-------------------------------------------------------

""" The function "create_spark_session" creates a spark session using AWS credentials if config file was informed
    In sparkConf, set enviroment to access AWS S3 using TemporaryCredentials if TOKEN Credential is provided
    To read s3a, neeed to enable S3V4 to JavaOptions
    In this case was needed to use: SparkConf, sparkContex and SparkSession to flag all AWS enviroment

    Input: aws credencials (access_key, secret_key, token)
           localrun => identify the type of run

    Output: Return spark session as spark
"""

def create_spark_session(AWS_ACCESS_KEY, AWS_SECRET_KEY, localrun):
    conf = SparkConf()
    conf.setAppName('pyspark_aws')
    conf.set('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
    conf.set('spark.driver.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')

    if AWS_ACCESS_KEY:
        conf.set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)

    if localrun == 'y':
        conf.setMaster('local[*]')

    if AWS_SECRET_KEY:
        conf.set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)

    #print(conf.toDebugString())

    sc = SparkContext(conf=conf)
    sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

    spark = SparkSession \
            .builder \
            .appName('pyspark_aws') \
            .getOrCreate()

    print('session created')

    return spark


#---------------WORKING WITH Breweries DATA --------------------------------------
"""
The function "process_bronze" works with Bronze Layer
    Read the file from S3 bucket
    Use recurssiveFileLookup => read all the files in the path
    Transform in DataFrame and Temporay View
    Save then S3 bucket using parquet file type
    Partitioned by brewery location.

About Malformad data
    Have three ways to handle this type of data
    - To include this data in a separate column (mode = PERMISSIVE)
    - To ignore all bad records (mode = DROPMALFORMED)
    - Throws an exception when it meets corrupted records (mode = FAILFAST)
     Assuming that completely broken records will not part of out dataset

Input: spark context
       input_path  => partial path for song_data
       output_path => partial path to write tables
       number => 0 = all path for input data |  1 = partial path for input data

Output: brewery.parquet on bucket silver
"""

def process_bronze(spark, input_data, output_data):

# get filepath to breweries data file
    input_data_path = input_data + '*.json'

    #brewery.9.2023-06-18T14:11:46.196558+00:00.json
    print('input_path', input_data_path)

## read brewerie_data using pre-defined schema
    df = spark.read\
        .option("recursiveFileLookup", "true")\
        .option("mode", "DROPMALFORMED")\
        .json(input_data_path)


    print ('====>', df)

    df.createOrReplaceTempView("brewerie_data")

    spark.sql\
    ("""
                select * from brewerie_data limit 10
    """).show()

    spark.catalog.dropGlobalTempView("brewerie_data")
    print ('output_path', output_data)

    #brewerie_data.write.partitionBy("country","state").mode("append").parquet(output_data)
    df.write.partitionBy("country","state").mode("append").parquet(output_data)

    print ('Finish brewierie_file')


def main():
    print ("Start of process")

    #path of input/output/config data
    input_data, output_data, localmode, config_data = get_path()
    print (input_data, output_data, localmode, config_data)

    #get AWS credentials
    AWS_ACCESS_KEY, AWS_SECRET_KEY = get_credentials(config_data)

    #start spark
    spark = create_spark_session(AWS_ACCESS_KEY, AWS_SECRET_KEY, localmode)


    ##process from bronze to silver
    process_bronze(spark, input_data, output_data)

    spark.stop()

    print ("End of process")


if __name__ == "__main__":
    main()
