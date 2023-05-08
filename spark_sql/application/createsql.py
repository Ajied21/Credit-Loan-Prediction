from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, date_format, to_date
from pyspark.sql.functions import col

class Data:

    def data_sql(self):

        spark       = SparkSession.builder.master("local")\
                                            .appName("G2Academy")\
                                                .getOrCreate()

        df_1   = spark.read.option("header", True).csv("ListUser_Ajied.csv")
        df_2   = spark.read.option("header", True).csv("Status_Ajied.csv")
        df_3   = spark.read.option("header", True).csv("TitleStatus_Ajied.csv")
        df_4   = spark.read.option("header", True).csv("BlockTitle_Ajied.csv")

        df_1.createOrReplaceTempView("df_1")
        df_2.createOrReplaceTempView("df_2")
        df_3.createOrReplaceTempView("df_3")
        df_4.createOrReplaceTempView("df_4")

        #print(df_1.show())
        #print(df_2.show())
        #print(df_3.show())
        #print(df_4.show())
        """
        df_1 = df_1.\
                    withColumn("No", df_1.No.cast(IntegerType())).\
                    withColumn("CreatedAt", to_date(df_1.CreatedAt, "dd-MM-yyyy"))
        
        df_2 = df_2.\
                    withColumn("No", df_2.No.cast(IntegerType())).\
                    withColumn("CreatedAt", to_date(df_2.CreatedAt, "dd-MM-yyyy"))

        df_4 = df_4.\
                    withColumn("No", df_4.No.cast(IntegerType())).\
                    withColumn("CreatedAt", to_date(df_4.CreatedAt, "dd-MM-yyyy"))
        """
        #print(df_1.show())

        data_fact = spark.sql("SELECT C.UserId, B.StatusId, D.ClientId\
                              FROM df_2 as A\
                              INNER JOIN df_3 as B ON A.StatusId=B.StatusId\
                              INNER JOIN df_1 as C ON A.UserId=C.UserId INNER JOIN df_4 as D ON A.No=D.No")
        print("data_fact")
        print(data_fact.show())
        
        #print(df_2.show())
        #print(df_2.filter(df_2.UserId.isNull()).show()) 

        #print(df_4.show())
        #print(df_4.filter(df_4.Title.isNull()).show())

        #print(df_4.filter(df_4.ClientId.isNull()).show())

        #print(spark.sql("SELECT DISTINCT Title, CreatedAt FROM df_4 WHERE Title IS NOT NULL AND ClientId IS NOT NULL ORDER BY 1").show())

        #data_2=spark.sql("select date_format(CreatedAt, 'MM') as Tanggal from df_1")
        #print(data_2.show())
        
        """
        print(spark.\
            sql("SELECT DISTINCT A.StatusId, B.Title, COUNT(A.StatusId) as Status FROM df_2 as A\
                 INNER JOIN df_3 as B ON A.StatusId=B.StatusId WHERE B.Title = 'Block' GROUP BY 1,2\
                    UNION SELECT DISTINCT A.StatusId, B.Title, COUNT(A.StatusId) as Status FROM df_2 as A\
                         INNER JOIN df_3 as B ON A.StatusId=B.StatusId WHERE B.Title = 'Not Block' GROUP BY 1,2 ORDER BY Status").show())
        """

        #print(df_1.select(date_format("CreatedAt", "MM-yyyy").alias("Tanggal")).show())

        #print(df_1.printSchema())
        #print(df_2.printSchema())
        #print(df_4.printSchema())

        #print(df_2.show())

        #print(df_2.select((df_2.No),date_format(df_2.CreatedAt, 'MM-yyyy').alias('CreateAt'), (df_2.StatusId)).show())

        print(spark.\
            sql("SELECT DISTINCT EXTRACT(MONTH FROM A.CreatedAt) as Data_Waktu, A.StatusId, B.title\
                FROM df_2 as A\
                    INNER JOIN df_4 as B\
                        ON A.UserId=B.ClientId").show())