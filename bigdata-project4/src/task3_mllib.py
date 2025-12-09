from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import col, datediff, to_date, udf, when, dayofweek, dayofmonth
from pyspark.sql.types import FloatType, IntegerType
import os

def main():
    spark = SparkSession.builder \
        .appName("Task3_MLlib") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Base path determination
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)

    train_path = "file://" + os.path.join(project_root, "data", "raw", "ccf_offline_stage1_train.csv")
    test_path = "file://" + os.path.join(project_root, "data", "raw", "ccf_offline_stage1_test_revised.csv")
    predictions_output = os.path.join(project_root, "output", "task3", "predictions")
    
    print("Loading data...")
    df_train_raw = spark.read.csv(train_path, header=True)
    df_test_raw = spark.read.csv(test_path, header=True)
    
    # UDFs for Feature Engineering
    
    # 1. Calculate Discount Rate (Existing)
    def calc_discount(rate):
        if rate is None or rate == 'null':
            return 1.0
        if ':' in rate:
            try:
                a, b = rate.split(':')
                return 1.0 - float(b)/float(a)
            except:
                return 1.0
        else:
            try:
                return float(rate)
            except:
                return 1.0

    # 2. Extract "Man" threshold (New) - e.g., 150 from "150:20"
    def get_discount_man(rate):
        if rate is None or rate == 'null':
            return 0.0
        if ':' in rate:
            try:
                return float(rate.split(':')[0])
            except:
                return 0.0
        return 0.0 # Direct discount has 0 threshold

    # 3. Extract Discount Type (New) - 1 for "Man Jian", 0 for Direct
    def get_discount_type(rate):
        if rate is None or rate == 'null':
            return 0
        if ':' in rate:
            return 1
        return 0

    calc_discount_udf = udf(calc_discount, FloatType())
    get_discount_man_udf = udf(get_discount_man, FloatType())
    get_discount_type_udf = udf(get_discount_type, IntegerType())

    def preprocess(df, is_train=True):
        # Filter null Coupon_id
        df = df.filter(col("Coupon_id") != "null")
        
        # --- Feature Engineering ---
        
        # 1. Discount Features
        df = df.withColumn("discount_rate_val", calc_discount_udf(col("Discount_rate")))
        df = df.withColumn("discount_man", get_discount_man_udf(col("Discount_rate")))
        df = df.withColumn("discount_type", get_discount_type_udf(col("Discount_rate")))
        
        # 2. Distance Feature
        # Treat null as max distance (e.g., 11) instead of -1 to maintain ordinality
        # Handle "null" string explicitly before casting
        df = df.withColumn("distance_val", 
            when((col("Distance") == "null") | (col("Distance").isNull()), 11)
            .otherwise(col("Distance").cast("int"))
        )
        
        # 3. Date Features (New)
        # Convert string date to DateType
        df = df.withColumn("dt_received", to_date(col("Date_received"), "yyyyMMdd"))
        
        # Extract Day of Week (1=Sunday, 2=Monday, ..., 7=Saturday)
        df = df.withColumn("day_of_week", dayofweek(col("dt_received")))
        
        # Extract Day of Month for Salary/Consumption Cycle feature
        # Refined bins to capture salary payment phenomenon (1st/5th, 15th, 25th)
        # 0: Early Pay (1-5)
        # 1: Mid Pay (15-20)
        # 2: Late Pay (25-31)
        # 3: Normal Days (Others)
        df = df.withColumn("day_of_month", dayofmonth(col("dt_received")))
        df = df.withColumn("salary_cycle", 
            when(col("day_of_month").between(1, 5), 0)
            .when(col("day_of_month").between(15, 20), 1)
            .when(col("day_of_month").between(25, 31), 2)
            .otherwise(3)
        )
        
        if is_train:
            # Label Generation
            df = df.withColumn("dt_used", to_date(col("Date"), "yyyyMMdd"))
            
            df = df.withColumn("label", 
                when(
                    (col("Date") != "null") & 
                    (datediff(col("dt_used"), col("dt_received")) <= 15), 
                    1.0
                ).otherwise(0.0)
            )
        
        return df

    print("Preprocessing data with enhanced features...")
    df_train = preprocess(df_train_raw, is_train=True)
    df_test = preprocess(df_test_raw, is_train=False)
    
    # Assemble Features
    # Added: discount_man, discount_type, day_of_week, salary_cycle
    feature_cols = ["discount_rate_val", "distance_val", "discount_man", "discount_type", "day_of_week", "salary_cycle"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # Handle potential nulls in new features (though UDFs handle most, day_of_week needs care if date is null)
    df_train = df_train.na.fill(0, subset=["day_of_week", "salary_cycle"])
    df_test = df_test.na.fill(0, subset=["day_of_week", "salary_cycle"])

    df_train_vec = assembler.transform(df_train).select("features", "label")
    df_test_vec = assembler.transform(df_test)
    
    print(f"Training Logistic Regression model using features: {feature_cols}...")
    lr = LogisticRegression(featuresCol="features", labelCol="label")
    model = lr.fit(df_train_vec)
    
    print("Predicting on test set...")
    predictions = model.transform(df_test_vec)
    
    print("Top 5 Predictions:")
    predictions.select("User_id", "Coupon_id", "Date_received", "probability", "prediction").show(5)

    # Persist predictions for downstream analysis (cast probability vector to string for CSV compatibility)
    predictions.select(
        col("User_id"),
        col("Coupon_id"),
        col("Date_received"),
        col("probability").cast("string").alias("probability"),
        col("prediction")
    ).coalesce(1) \
     .write \
     .mode("overwrite") \
     .option("header", True) \
     .csv(predictions_output)
    
    spark.stop()

if __name__ == "__main__":
    main()
