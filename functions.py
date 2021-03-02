pip install xlrd==1.2.0
pip install openpyxl

%python
import csv
import re
import pandas as pd
from openpyxl import Workbook
import openpyxl
import numpy as np
import os
import xlrd 
import glob
from zipfile import ZipFile
# Import hashlib library (sha256 method is part of it)
import hashlib
import shutil
import gzip
import tarfile
from pathlib import Path
import databricks.koalas as ks


def file_exists(file_path):
  """
  
  :parm file_path: blob path to check if file exists
  :return: true if file exists and size is more than 0 kb else false
  """
  total =0
  temp_path = file_path.replace("/dbfs", "dbfs:")
  try:
    if dbutils.fs.ls(temp_path):
        dir_files = dbutils.fs.ls(temp_path)
        for file in dir_files:
          if file.size > 0:
            return True
    else:
      raise RuntimeError("no files to process")
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      raise RuntimeError("no files to process")
    else:
      raise e

def Combine_files(source_file_path, target_file_path, extension):
  """
  
  :parm source_file_path: source path to check if file exists and convert to csv
  :parm target_file_path: target path to write csv file
  :parm extension: type of file format
  """
  try:
    
    if file_exists(source_file_path) :
      print("file exists!!")
      #set working directory
      os.chdir(source_file_path)

      #find all csv files in the folder
      #use glob pattern matching -> extension = 'csv'
      #save result in list -> all_filenames      
      all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
      print("number of part files inside given folder are: {}".format(len(all_filenames)))
      #combine all files in the list
      combined_file = pd.concat([pd.read_csv(f) for f in all_filenames ])
      #export to csv
      combined_file.to_csv( target_file_path,line_terminator='\r\n', index=False, encoding='utf-8-sig')
      print("merging all part files has been complted!!")
  except Exception as e:
    raise(e)
    print(f"merging all part files failed for {source_file_path}:", e) 

def convert_to_csv(source_file_path, target_file_path, sheetname = [0]):
  """
  
  :parm source_file_path: source path to check if file exists and convert to csv
  :parm target_file_path: target path to write csv file
  """
  try:
    # set all .xls files in your folder to list
    allfiles = glob.glob(source_file_path + "*.xlsx")
    # for loop to aquire all excel files in folder
    if len(allfiles) > 0:
      print("number of files: " + str(len(allfiles)))
      for excelfiles in allfiles:
        read_file_dict = pd.read_excel(excelfiles, sheet_name = [sheetname])
        read_file = pd.DataFrame.from_dict(read_file_dict.values())
        read_file.to_csv (target_file_path,line_terminator='\r\n',index = None,header=True)
        print("File_converted_to_csv")
    else:
      raise RuntimeError("no files to process")
  except Exception as e:
    raise(e)
    
def unzip_files(source_file_path, target_file_path):
  """
  
  :parm source_file_path: source path to check if file exists and unzip
  :parm target_file_path: target path to write files
  """
  
  if file_exists(source_file_path) :
    print("file exists!!")
    #set working directory
    os.chdir(source_file_path.rsplit('/', 1)[0] + '/')
    with ZipFile(source_file_path, 'r') as zipObj:
     # Extract all the contents of zip file in different directory
      zipObj.extractall(target_file_path)
      print("File is unzipped in path:  {}".format(target_file_path))
    
def checksum(old_file_path, new_file_path):
  """
  
  :parm old_file_path: old file path to check if file exists and find sha256
  :parm new_file_path: new file path to check if file exists and find sha256
  """
  sha256_hash = hashlib.sha256
  if file_exists(old_file_path) :
    print("old file exists!!")
    #set working directory
    os.chdir(old_file_path.rsplit('/', 1)[0] + '/')
    # Correct original sha256 goes here
    with open(old_file_path) as old_file_to_check:
        # read contents of the file
        data = old_file_to_check.read()    
        # pipe contents of the file through
        original_sha256 =  sha256_hash(data.encode('utf-8')).hexdigest()
#     new_file_path = new_file_path + new_date + "/"
    if file_exists(new_file_path) :
      print("new file exists!!")
      #set working directory
      os.chdir(new_file_path.rsplit('/', 1)[0] + '/')
      # Open,close, read file and calculate sha256 on its contents 
      with open(new_file_path) as new_file_to_check:
          # read contents of the file
          data = new_file_to_check.read()    
          # pipe contents of the file through
          new_sha256 =  sha256_hash(data.encode('utf-8')).hexdigest()
      # Finally compare original sha256 with freshly calculated
      if original_sha256 == new_sha256:
          print("checksum verified.")
          return True
      else:
          print("checksum verification failed!.")
          return False
    else:
      print("No file found in given path:{}".format(new_file_path + "/" + filename))
  else:
      print("No file found in given path:{}".format(old_file_path))
      
def uncompress_gz_files(source_file, target_directory):
    """
      This function uncompress gz files and also tar files inside gz
      
      Parameters:
        source_file: the absolute path including filename
        target_directory: the target directory where to uncompress the files
        
      Return: Nothing
    """
    target_directory = re.sub('/{2,}','__', target_directory)
    os.makedirs(os.path.dirname(target_directory), exist_ok=True)
    if tarfile.is_tarfile(source_file):
      tar = tarfile.open(source_file, "r:gz")
      tar.extractall(target_file)
      tar.close()
    else:
      filename = Path(source_file).name.replace(".gz", "")
      if ('csv' in filename) or ('txt' in filename) or ('xlsx' in filename):
        target = re.sub('/{2,}','__', f"{target_directory}/{filename}.csv") 
        with gzip.open(source_file, 'rb') as f:
            with open(target, 'wb') as o:
                  shutil.copyfileobj(f, o)
                  
def table_to_csv(table_name,target_path):
  """
  
  :parm table_name: databricks table name ex: raw.ca_trax_kpi_detail.
  :parm target_path : target location of csv file to be placed.
  """
  try:
    df = spark.sql(f"select * from {table_name}")
    kdf = ks.DataFrame(df)
    kdf.to_csv(path=target_path, num_files=1)
  except Exception as e:
    raise RuntimeError("convertion to csv failed", e)
