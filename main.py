import pyspark	
from src.pre_process import call_me
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

def getCounts(df):
    df.write.parquet("hello")
    return(df.count())  

def main(sqlContext):

  df=sqlContext.read.text("sample.txt")
  df.show()
  total_rec = getCounts(df)
  print("Number of records :",total_rec)

if __name__=='__main__':
   conf = (SparkConf().setAppName('Application name'))
   sc = SparkContext(conf = conf)
   sc.setLogLevel("ERROR")
   sqlContext = SQLContext(sc)
   main(sqlContext)
   sc.stop()


