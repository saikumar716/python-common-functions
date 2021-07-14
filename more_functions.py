"""
Common Pyspark Functions

Generic methods:
    - s3_path_split
    - s3_read
    - get_table_details
"""

import sys
import logging
from pprint import pformat
from time import strftime,gmtime
from collections import namedtuple

import boto3
from pyspark.sql.types import StringType, StructType, StructField

#return type in s3_path_split
S3Path = namedtuple("S3Path", ["bucket_name", "key"])

def s3_path_split(s3_path: str) -> S3Path:
    """
    Split an S3 path into bucket and key.
    Parameters
    ----------
    s3_path : str
    Returns
    -------
    splitted : S3Path
    Examples
    --------
    >>> s3_path_split('s3://my-bucket/foo/bar.jpg')
    S3Path(bucket_name='my-bucket', key='foo/bar.jpg')
    """
    try:
        params = locals()
        if not s3_path.startswith("s3://"):
            raise ValueError(
                f"s3_path is expected to start with 's3://', but was {s3_path}"
            )
        bucket_key = s3_path[len("s3://") :]
        bucket_name, key = bucket_key.split("/", 1)
        return S3Path(bucket_name, key)
    except Exception as e:
        f_name = sys._getframe().f_code.co_name
        failure_alert(e,f_name,params)

def s3_read(source):
    """
    Read a file from an S3 source.

    Parameters
    ----------
    source : str
        Path starting with s3://, e.g. 's3://bucket-name/key/foo.bar'

    Returns
    -------
    content : utf-8
    """
    
    try:
        params = locals()
        s3 = boto3.client('s3')
        bucket, Prefix = s3_path_split(source)
        result = s3.list_objects(Bucket = bucket, Prefix=Prefix)
        for obj in result.get('Contents'):
            data = s3.get_object(Bucket=bucket, Key=obj.get('Key'))
            contents = data['Body'].read().decode("utf-8")
            return contents
    except Exception as e:
        f_name = sys._getframe().f_code.co_name
        failure_alert(e,f_name,params)

def get_table_details(ddl):
    """
    Get the table details from DDL string.

    Parameters
    ----------
    source : str

    Returns
    -------
    Dictionary : {'table_type': table_type, 'columnns_dict': columnns_dict, 'partition_dict': partition_dict,
                     'inputformat': inputformat, 'outputformat': outputformat, 'location': location}
                     
    Example: {"table_type": "EXTERNAL", "columnns_dict": {"serial_number": "string", "country": "string"}, "partition_dict": {"month_run": "string,"}, 
                "inputformat": "org.apache.hadoop.hive.ql.io.parquet.mapredparquet", 
                "outputformat": "org.apache.hadoop.hive.ql.io.parquet.mapredparquet", 
                "location": "s3://saikumar716.prd.us-east-1.blu/database/tables/table_name.sql"}
    """
    try:
        params = locals()
        columns =[]
        columns_datatypes = []
        table_type = ''
        partitions =[]
        partitions_datatypes = []
        
        ddl = ddl.lower()
        #table_type
        table_type_lines =ddl.split(" ")

        if table_type_lines[1] == 'external':
            table_type= 'external'
        else:
            table_type = 'managed'
        logging.info(f"Table type is: {table_type}")

        #columns and datatypes
        column_data = ddl.split("(")[1].split(")")[0].split(",")
        i = 0
        while (i < len(column_data)) :
            name = column_data[i].strip().split(" ")[0].replace('`','')
            columns.append(name)
            datatype = column_data[i].strip().split(" ")[1].replace('`','')
            columns_datatypes.append(datatype)
            i = i + 1
        columnns_dict = dict(zip(columns,columns_datatypes))
        logging.info(f"Table columns and datatypes dictionory is: {columnns_dict}")

        #partitioned data
        part = "partitioned by ("
        if part in ddl:
            partition_lines = ddl.split(part)[1].split(")")[0].replace('`','').split(" ")
            partition_lines = ' '.join(partition_lines).split()
            j = 0
            while (j <= len(partition_lines) -1):
                name = partition_lines[j].replace('`','')
                partitions.append(name)
                j = j + 2

            k = 1
            while (k < len(partition_lines)):
                name = partition_lines[k].replace('`','')
                partitions_datatypes.append(name)
                k = k + 2
            partition_dict = dict(zip(partitions,partitions_datatypes))
            logging.info(f"Table partitions  and partitions_datatypes dictionory is: {partition_dict}")
        else:
            logging.info(f"Table doesn't contain partitions: {partition_dict}")
            partition_dict = {}

        #inputformat
        inputformat = ddl.split("inputformat")[1].split("\'")[1]
        logging.info(f"Table inputformat is: {inputformat}")

        #outputformat
        outputformat = ddl.split("outputformat")[1].split("\'")[1]
        logging.info(f"Table outputformat is: {outputformat}")

        #location
        location = ddl.split("location")[1].split("\'")[1]
        logging.info(f"Table location is: {location}")

        return {'table_type': table_type, 'columnns_dict': columnns_dict, 'partition_dict': partition_dict, 'inputformat': inputformat, 'outputformat': outputformat, 'location': location}
    except Exception as e:
        f_name = sys._getframe().f_code.co_name
        failure_alert(e,f_name,params)

def compare_ddl(hiveContext,schema_name,table_name):
    """
    compare production DDL stored in s3 location, current Hive table details and returns dictionary result.

    Parameters
    ----------
    hiveContext ([object]): Spark Hive Context Object
    schema_name ([string]): Schema name
    table_name ([string]): tablename

    Returns
    -------
    Testcase_template
    Example:
    {'test_case_number': '', 'check_name': 'compare_ddl for schema.tablename', 'status': 'passed', 'check_runtimestamp': '2021-06-17 08:11:07'}
    """
    try:
        timestamp = str(strftime("%Y-%m-%d %H:%M:%S", gmtime()))
        f_name = sys._getframe().f_code.co_name
        params = locals()

        #reading  production tables DDL from s3 location
        env = 'prd'
        bucket = f'saikumar716.{env}.us-east-1.blu'
        source = f"s3://{bucket}/database/ddl_uat/hive/{schema_name}/{table_name}.sql"
        prod_ddl = s3_read(source)

        #extracting production table details with existing ddl
        prod_table_details_dict = get_table_details(prod_ddl)
        
        # # current hive table 
        hive_ddl = extract_ddl(hiveContext,schema_name,table_name)

        #extracting current hive table details with extracted ddl
        current_table_details_dict = get_table_details(hive_ddl)

        #Comparing two ddls 
        #columns
        if current_table_details_dict["columnns_dict"] == prod_table_details_dict['columnns_dict']:
            logging.info(f"columns matched which is : {current_table_details_dict['columnns_dict']}")
        else:
            # creates a set of tuples (k/v pairs)
            prod_dict_set = set(prod_table_details_dict['columnns_dict'].items()) 
            current_dict_set = set(current_table_details_dict['columnns_dict'].items())
            # set method for comparisons
            setdiff = current_dict_set.difference(prod_dict_set) 
            # unpack the tuples for processing
            for k, v in setdiff:
                logging.info(f"column/datatype differences between prod ddl and current ddl = {k}: {v}")
            raise ValueError(f"columns/datatype changed!! Prod table columns and current table columns/datatypes differences : {setdiff}")
        
        #partitions
        if current_table_details_dict["partition_dict"] == prod_table_details_dict['partition_dict']:
            logging.info(f"partitions matched which is : {current_table_details_dict['partition_dict']}")
        else:
            
            # creates a set of tuples (k/v pairs)
            prod_dict_set = set(prod_table_details_dict['partition_dict'].items()) 
            current_dict_set = set(current_table_details_dict['partition_dict'].items())
            # set method for comparisons
            setdiff = current_dict_set.difference(prod_dict_set) 
            # unpack the tuples for processing
            for k, v in setdiff:
                logging.info(f"partitions column/datatype differences between prod ddl and current ddl = {k}: {v}")
            raise ValueError(f"partitions columns/datatype changed!! Prod table columns and current table columns/datatypes differences : {setdiff}")
        
        #location
        if current_table_details_dict["location"] == prod_table_details_dict['location']:
            logging.info(f"location matched which is : {current_table_details_dict['location']}")
        else:
            logging.info(f"location changed prod table location: {prod_table_details_dict['location']} and current table location: {current_table_details_dict['location']}")
            raise ValueError(f"location changed prod table location: {prod_table_details_dict['location']} and current table location: {current_table_details_dict['location']}")

        #inputformat
        if current_table_details_dict["inputformat"] == prod_table_details_dict['inputformat']:
            logging.info(f"inputformat matched which is : {current_table_details_dict['inputformat']}")
        else:
            logging.info(f"inputformat changed prod table inputformat: {prod_table_details_dict['inputformat']} and current table inputformat: {current_table_details_dict['inputformat']}")
            raise ValueError(f"inputformat changed prod table inputformat: {prod_table_details_dict['inputformat']} and current table inputformat: {current_table_details_dict['inputformat']}")

        #outputformat
        if current_table_details_dict["outputformat"] == prod_table_details_dict['outputformat']:
            logging.info(f"outputformat matched which is : {current_table_details_dict['outputformat']}")
        else:
            logging.info(f"outputformat changed prod table outputformat: {prod_table_details_dict['outputformat']} and current table outputformat: {current_table_details_dict['outputformat']}")
            raise ValueError(f"outputformat changed prod table outputformat: {prod_table_details_dict['outputformat']} and current table outputformat: {current_table_details_dict['outputformat']}")
        
        #table_type
        if current_table_details_dict["table_type"] == prod_table_details_dict['table_type']:
            logging.info(f"table_type matched which is : {current_table_details_dict['table_type']}")
        else:
            logging.info(f"table_type changed prod table table_type: {prod_table_details_dict['table_type']} and current table table_type: {current_table_details_dict['table_type']}")
            raise ValueError(f"table_type changed prod table table_type: {prod_table_details_dict['table_type']} and current table table_type: {current_table_details_dict['table_type']}")
            
        status = 'passed'
        testcase_template = {
            'test_case_number':''
            ,'check_name':f'{f_name} for {schema_name}.{table_name} in prod and current Hive table'
            ,'status':"passed"
            ,'check_runtimestamp':str(timestamp)
        }
        logging.info(f"{f_name} Test Case for {params} - {status}")
        return testcase_template
    except Exception as e:
        status = 'failed'
        testcase_template = {'test_case_number':''
            ,'check_name':f'Error running {f_name} for {schema_name}.{table_name} - Error : {e}'
            ,'status': 'failed'
            ,'check_runtimestamp':str(timestamp)
        }
        logging.info(f"{f_name} Test Case for {params} - Error - {status} ")
        return testcase_template
