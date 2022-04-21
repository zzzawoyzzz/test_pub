

# สร้าง Spark Session เพิ้อใช้งาน Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

#audible_data = spark.sql("select * from result")
dt_clean = spark.read.csv('gs://dataproc-staging-asia-southeast2-537169427832-9rlydjvh/google-cloud-dataproc-metainfo/f0bb62b5-0e85-4a7c-9fa0-c407c0582d85/cluster-awoy-m/ws2_data.csv', header = True, inferSchema = True, )

from pyspark.sql import functions as f

dt_clean = dt_clean.withColumn("timestamp",
                        f.to_timestamp(dt_clean.timestamp, 'yyyy-MM-dd HH:mm:ss')
                        )
# สำคัญ: เปลี่ยน aaa เป็นชื่อประเทศที่คุณคิดว่าผิด และ bbb เป็นชื่อประเทศที่ถูกต้อง
from pyspark.sql.functions import when

dt_clean_country = dt_clean.withColumn("CountryUpdate", when(dt_clean['Country'] == 'Japane', 'Japan').otherwise(dt_clean['Country']))
dt_clean = dt_clean_country.drop("Country").withColumnRenamed('CountryUpdate', 'Country')

dt_clean.where(dt_clean['user_id'].rlike("^[a-z0-9]{8}$")).count()

dt_correct_userid = dt_clean.filter(dt_clean["user_id"].rlike("^[a-z0-9]{8}$"))
dt_incorrect_userid = dt_clean.subtract(dt_correct_userid)

dt_clean_userid = dt_clean.withColumn("user_id_update", when(dt_clean['user_id'] == 'ca86d17200', 'ca86d172').otherwise(dt_clean['user_id']))

dt_correct_userid = dt_clean_userid.filter(dt_clean_userid["user_id"].rlike("^[a-z0-9]{8}$"))
dt_incorrect_userid = dt_clean_userid.subtract(dt_correct_userid)
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed('user_id_update', 'user_id')

dt_clean_userid = dt_clean.withColumn("user_id_update", when(dt_clean['user_id'].isNull(), '00000000').otherwise(dt_clean['user_id']))
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed('user_id_update', 'user_id')

dt_clean.write.mode("overwrite").csv('gs://processed_awoy/Cleaned_data.csv', header = True)

print("processed finnish awoy12334")