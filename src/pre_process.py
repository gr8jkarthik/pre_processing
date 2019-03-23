def call_me():
   print("Hello Spark")
   print("TeamCity Testing: Looks Working1")

def getCounts(df):
   df.write.parquet("hello")
   call_me()
   return (df.count())

