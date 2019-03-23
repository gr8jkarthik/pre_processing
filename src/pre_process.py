def call_me():
   print("Hello Spark")

def getCounts(df):
   df.write.parquet("hello")
   call_me()
   return (df.count())

