# 📊 PySpark ETL Pipeline – Category Revenue Analysis

This project demonstrates a complete **ETL (Extract–Transform–Load) pipeline using PySpark**, where raw order and product datasets are processed to compute **category-wise total revenue**.
It showcases essential data engineering concepts such as data cleaning, transformation, joining datasets, and performing aggregations at scale.

---

## 🚀 Project Overview

### **Objective**

To build an ETL pipeline that:

* Extracts data from external CSV sources
* Cleans and transforms raw data
* Joins the datasets on `product_id`
* Calculates revenue per order
* Aggregates revenue by product category
* Sorts results to identify high-performing categories

---

## 📂 Dataset Source

The datasets are publicly accessible from GitHub:

* **Orders Dataset**
  `https://raw.githubusercontent.com/ankitbansal6/namastesql_etl_datasets/refs/heads/main/ta/orders.csv`

* **Products Dataset**
  `https://raw.githubusercontent.com/ankitbansal6/namastesql_etl_datasets/refs/heads/main/ta/products.csv`

These files contain basic information about orders and product metadata used for category-based revenue analysis.

---

## 🛠️ Technologies Used

* **PySpark (Spark SQL)**
* **Pandas**
* **Python 3**
* **GitHub Raw Data Source**

---

## 🧩 ETL Pipeline Steps

### **1️⃣ Extract**

* Read CSV files using Pandas
* Convert them into Spark DataFrames

### **2️⃣ Transform**

* Handle missing values in `quantity` and `category`
* Cast incorrect data types
* Replace dirty data like `"NaN"` strings
* Compute `revenue = quantity × price`

### **3️⃣ Load**

* Perform aggregation:

  * Total revenue per category
* Order results in descending order

---

## 🧪 Code Snippet

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import pandas as pd

spark = SparkSession.builder.appName("ETL_Pipeline").getOrCreate()

orders_url = "https://raw.githubusercontent.com/ankitbansal6/namastesql_etl_datasets/refs/heads/main/ta/orders.csv"
products_url = "https://raw.githubusercontent.com/ankitbansal6/namastesql_etl_datasets/refs/heads/main/ta/products.csv"

# Extract
orders_pdf = pd.read_csv(orders_url)
products_pdf = pd.read_csv(products_url)

orders_df = spark.createDataFrame(orders_pdf)
products_df = spark.createDataFrame(products_pdf)

# Transform
orders_df = orders_df.fillna({"quantity": 1})
orders_df = orders_df.withColumn("quantity", F.col("quantity").cast(IntegerType()))

products_df = products_df.fillna({"category": "unknown"})
products_df = products_df.withColumn("category", F.regexp_replace("category", "NaN", "unknown"))

merge_df = orders_df.join(products_df, on="product_id", how="inner")
merge_df = merge_df.withColumn("revenue", F.col("quantity") * F.col("price"))

# Load
agg_df = (
    merge_df.groupBy("category")
    .agg(F.sum("revenue").alias("total_revenue"))
    .orderBy(F.col("total_revenue").desc())
)

agg_df.show()
```

---

## 📈 Sample Output

```
+---------------+--------------+
|    category   | total_revenue|
+---------------+--------------+
| Electronics   |     25400.0  |
| Grocery       |     18750.0  |
| Clothing      |      9340.0  |
| unknown       |      2500.0  |
+---------------+--------------+
```

*(Note: Output will vary based on dataset values.)*

---

## 📘 What I Learned

* How to design and build a **scalable ETL pipeline**.
* Data cleaning with PySpark functions.
* Performing joins, aggregations, and transformations.
* Handling real-world data quality issues.
* Working with Spark DataFrames end-to-end.

---

## 🗂️ Project Structure

```
📦 pyspark-etl-pipeline
 ┣ 📄 etl_pipeline.py
 ┣ 📄 README.md
 ┗ 📁 output/   (optional for saving results)
```

---

## 🤝 Contributions

Feel free to raise issues or open pull requests for improvements!

---

## ⭐ Show Support

If you found this project useful, consider giving a **⭐ star** to the repository.

---
