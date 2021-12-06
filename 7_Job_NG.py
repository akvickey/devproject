import sys #onetime
import logging
from io import StringIO # python3; python2: BytesIO 
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import col,lit
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import pandas as pd
import s3fs
import ast
import time,datetime

def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df

def trim_string(untrimmed_df):
    string_cols = [f.name for f in untrimmed_df.schema.fields if (isinstance(f.dataType, StringType))]
    other_cols = [f.name for f in untrimmed_df.schema.fields if not(isinstance(f.dataType, StringType))]
    trimmed_df = untrimmed_df.select(other_cols+[col(c).substr(1, 6000).alias(c) for c in string_cols])
    trimmed_df.show(5)
    return trimmed_df 
    
   
def null_to_string(null_df):
    null_cols = [f.name for f in null_df.schema.fields if (isinstance(f.dataType, NullType))]
    other_cols = [f.name for f in null_df.schema.fields if not(isinstance(f.dataType, NullType))]
    string_df = null_df.select(other_cols+[col(c).cast(StringType()).alias(c) for c in null_cols])
    string_df.show(5)
    return string_df 
    
## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dfnew1 = pd.read_csv('s3://convosight-prod-redshift/config/7_Job_NG.csv')
print("-----------------printing config data----------------------------")
print(dfnew1)
print("-----------------printing no of rows in config file----------------------------")
no_of_table = len(dfnew1.index)
print(no_of_table)
max_update_time = []
max_update_timestamp = []
job_run_completion = []
total_no_of_rows = []
filtered_no_of_rows = []
a =[]
b =[]
redshift_table = ''
redshift_table_main = ''
now = datetime.datetime.now()

for i in range(no_of_table):
    try:
        dynamodb_tablename = dfnew1['tablename'][i]
        partitionkey =  dfnew1['partitionkey'][i]
        sortkey =  dfnew1['sortkey'][i]
        createtime =  dfnew1['createtime'][i]
        updatetime =  dfnew1['updatetime'][i]
        cast_decision =  dfnew1['cast_columns'][i]
        print("-----------------------table details--------------------")
        print(dynamodb_tablename)
        print(partitionkey)
        print(sortkey)
        print(createtime)
        print(updatetime)
        print(cast_decision)
        
        datasource0 = glueContext.create_dynamic_frame.from_options(
            connection_type="dynamodb",
            connection_options={
                "dynamodb.input.tableName": dynamodb_tablename,
                "dynamodb.throughput.read.percent": "1.0",
                "dynamodb.splits": "776"
            }
        )
        
        
        print("-----------------------table schema-----------------------------")
        datasource0.printSchema()
        if cast_decision == 'none':
            datasource1 = ResolveChoice.apply(datasource0, choice = "make_cols")
        else :
            decision = list(ast.literal_eval(cast_decision))
            print(decision)
            datasource1 = ResolveChoice.apply(datasource0,specs = decision)
        datasource1 = ResolveChoice.apply(datasource1, choice = "make_cols")
        print("-----------------------after resolving table schema-----------------------------")
        datasource1.printSchema()
        dfirst = datasource1.toDF()
        dfirst = dfirst.withColumn("last_job_run",lit(now))
        if sortkey == 'not-present':
            dfirst = dfirst.withColumn("key_id",sha2(partitionkey, 256))
        else :
            dfirst = dfirst.withColumn("key_id",sha2(concat_ws("-",partitionkey,sortkey), 256))
        
        print("-----------------------table with hash key-----------------------------")
        dfirst.show(5)
        
       
        
        scalar_cols = [f.name for f in dfirst.schema.fields if not(isinstance(f.dataType, ArrayType))]
        list_cols = [f.name for f in dfirst.schema.fields if (isinstance(f.dataType, ArrayType))]
        scalar_df = dfirst.select(*scalar_cols)
        after_flattening = flatten_df(scalar_df)
        after_trimming = trim_string(after_flattening)
        final_df = null_to_string(after_trimming)
        final_df.printSchema()
        print("------------final df------------")
        final_df.show(5)
        
        new_df = DynamicFrame.fromDF(final_df, glueContext, "dynamic_df")
        redshift_table = dynamodb_tablename.replace("-","_")
        redshift_table_main = redshift_table + "_main"
        
        job_runtime = datetime.datetime.now()
        job_run_completion.append(job_runtime)
        row_count = dfirst.count()
        total_no_of_rows.append(row_count)
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = new_df, catalog_connection = "prod_dynamo_db_redshift", connection_options = {"dbtable":redshift_table_main, "database": "convosight_prod_dynamo_data"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink1")
        
        for item in list_cols :
            if (sortkey == 'not-present') :
                sub_tab = dfirst.select(partitionkey,"key_id",item,"last_job_run")
                sub_tab = sub_tab.select(partitionkey,"key_id","last_job_run", F.posexplode(item).alias("position", "list_value"))
                sub_tab = sub_tab.withColumn("key_id_1",sha2(concat_ws("-","key_id","position"), 256))
            else:
                sub_tab = dfirst.select(partitionkey,sortkey,"key_id",item,"last_job_run")
                sub_tab = sub_tab.select(partitionkey,sortkey,"key_id","last_job_run", F.posexplode(item).alias("position", "list_value"))
                sub_tab = sub_tab.withColumn("key_id_1",sha2(concat_ws("-","key_id","position"), 256))
            
                
            sub_tab_after_flattening = flatten_df(sub_tab)
            sub_tab_after_trimming = trim_string(sub_tab_after_flattening)
            sub_tab_final_df = null_to_string(sub_tab_after_trimming)
            sub_tab_final_df.printSchema()
            print("------------final df------------")
            sub_tab_final_df.show(5)
            new_df_1 = DynamicFrame.fromDF(sub_tab_final_df, glueContext, "dynamic_df")
            sub_table_name = "{}_{}".format(redshift_table,item)
            datasink_subtable = glueContext.write_dynamic_frame.from_jdbc_conf(frame = new_df_1, catalog_connection = "prod_dynamo_db_redshift", connection_options = {"dbtable": sub_table_name, "database": "convosight_prod_dynamo_data"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink2")

        print("table:- "+ dynamodb_tablename +" -> done")
        a.append(str(dynamodb_tablename))
        b.append("success")
    except Exception as e:
        logging.exception('table {} not done'.format(dynamodb_tablename))
        a.append(str(dynamodb_tablename))
        b.append("failed")
        job_run_completion.append(job_runtime)
        continue
    
year = now.year
month = now.month
day = now.day
columns = ["table_name","status","row_count"]
status_df = spark.createDataFrame(zip(a, b,total_no_of_rows), columns).coalesce(1)
status_df.show()
status_df_glue = DynamicFrame.fromDF(status_df, glueContext, "dynamic_df")
dfnew1["JOB_RUN_COMPLETION"] = job_run_completion
dfnew1.to_csv ('s3://convosight-prod-redshift/config/7_Job_NG.csv', index = False, header=True)
s3_target = "s3://convosight-prod-redshift/status_report/7_Job_NG/"
s3path = s3_target + "/year=" + "{:0>4}".format(str(year)) + "/month=" + "{:0>2}".format(str(month)) + "/day=" + "{:0>2}".format(str(day)) + "/"
s3sink = glueContext.write_dynamic_frame.from_options(frame = status_df_glue, connection_type = "s3", connection_options = {"path": s3path}, format = "csv", transformation_ctx = "s3sink")
job.commit()
        