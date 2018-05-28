
# coding: utf-8

# In[ ]:




# In[2]:

import pyspark


# In[3]:

from pyspark.sql import SparkSession


# In[4]:

spark = SparkSession.builder.appName('vinn').getOrCreate()


# In[5]:

df = spark.read.csv('hdfs://localhost:9000/Python/people.csv', inferSchema=True, header=True)


# In[6]:

df.show()


# In[7]:

df.printSchema()


# In[8]:

df.columns


# In[9]:

df.describe().show()


# In[10]:

from pyspark.sql.types import (StructField,StringType,IntegerType,StructType)


# In[11]:

type(df['char_37'])


# In[12]:

df.select('char_37').count()


# In[13]:

df.select('char_37').show()


# In[14]:

df.select('char_15').describe().show()


# In[15]:

type(df.head(2)[0])


# In[16]:

df.head(2)[0]


# In[17]:

df.withColumnRenamed('char_15', 'char15').show()


# In[18]:

df.createOrReplaceTempView('people')


# In[19]:

result = spark.sql("SELECT * FROM people WHERE char_38=50")


# In[20]:

result.show()


# In[21]:

df.printSchema()


# In[22]:

df.head()


# In[23]:

for xx in df.head():
    print(xx)


# In[24]:

df.head(3)[0]


# In[25]:

df.filter("char_38<50").show()

df.filter("char_38<50").show()
# In[26]:

df.filter("char_38<50").select(['char_15', 'char_17', 'char_3']).show()


# In[27]:

df.filter((df['char_38'] > 50) & (df['char_38'] < 50)).show()# its wrong bcoz data has not integer rather than char_38


# In[28]:

df.filter(df['char_38'] == 50).collect()


# In[29]:

#group by and aggregate functions


# In[30]:

df.printSchema()


# In[31]:

df.groupBy('char_15').count().show()


# In[32]:

df.agg({'char_38':'sum'}).show()


# In[33]:

group_data = df.groupBy("char_15")
group_data.agg({'char_38':'sum'}).show()


# In[34]:

from pyspark.sql.functions import countDistinct, avg, stddev


# In[35]:

df.select(countDistinct('char_38')).show()


# In[37]:

df.select(avg('char_38')).show()


# In[38]:

df.select(stddev('char_38')).show()


# In[39]:

df.select(avg('char_38').alias('Avrage_of_char38')).show()


# In[40]:

from pyspark.sql.functions import format_number


# In[41]:

stdd = df.select(stddev('char_38').alias('std'))


# In[42]:

stdd.select(format_number('std', 2).alias('std')).show()


# In[99]:

#missing data


# In[46]:

from pyspark.sql.functions import mean
mean_val = df.select(mean(df['char_38'])).collect()


# In[47]:

df.na.fill(df.select(mean(df['char_38'])).collect()[0][0],['char_38']).show()


# In[ ]:



