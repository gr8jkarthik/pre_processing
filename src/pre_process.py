def call_me():
   print("Hello Spark")
   print("TeamCity Testing: Looks Working3")
   print("Demo")

def getCounts(df):
   df.write.parquet("hello")
   call_me()
   return (df.count())

