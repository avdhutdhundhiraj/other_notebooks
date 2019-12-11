
from pyspark.sql import *
from pyspark.sql.types import *
#from pyspark.sql import SQLContext as sq
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

from faker import Faker
conf = SparkConf().setAppName('kmeans').set("spark.default.parallelism","32")
sc = SparkContext(conf=conf)
spark = SQLContext(sc)
fake = Faker('nl_NL')
fake_line = lambda x: Row(fake.sha256(), fake.name(), fake.street_name(), fake.province(), fake.country(), fake.phone_number(), fake.email(), fake.iban())
df_header = ['primary_id', 'name', 'streetname', 'province', 'country', 'phonenumber', 'email', 'iban']

num_records = int(1e8)# this can be taken from any value this current value is used for test only 

df = sc.parallelize(range(0, num_records)).map(fake_line).toDF(schema = df_header)

# since pyspark is lazy execute use count to get the values 
df.count()

