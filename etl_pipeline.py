from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import pandas as pd
spark = SparkSession.builder.appName("ETL_Pipeline").getOrCreate()

orders_url = "https://raw.githubusercontent.com/ankitbansal6/namastesql_etl_datasets/refs/heads/main/ta/orders.csv"
products_url = "https://raw.githubusercontent.com/ankitbansal6/namastesql_etl_datasets/refs/heads/main/ta/products.csv"
#extract
orders_pdf=pd.read_csv(orders_url)
products_pdf=pd.read_csv(products_url)
#print(orders_pdf)
orders_df=spark.createDataFrame(orders_pdf)
products_df=spark.createDataFrame(products_pdf)
#orders_df.show(100)
#data Cleaning
orders_df=orders_df.fillna({"quantity":1})
#orders_df.show()
#orders_df.printSchema()
orders_df=orders_df.withColumn("quantity",F.col("quantity").cast(IntegerType()))
#orders_df.show()
#orders_df.printSchema()
#products_df.show()
#products_df.printSchema()
products_df=products_df.fillna({"category":"unknown"})
#products_df.show()
products_df=products_df.withColumn("category",F.regexp_replace("category","NaN","unknown"))
#products_df.show()
#print(products_df)
merge_df=orders_df.join(products_df,on="product_id",how="inner")
merge_df=merge_df.withColumn("revenue",F.col("quantity")*F.col("price"))
#merge_df.show()
agg_df=(
	merge_df.groupBy("category")
	.agg(F.sum("revenue").alias("total_revenue"))
	.orderBy(F.col("total_revenue").desc())
)
agg_df.show()
