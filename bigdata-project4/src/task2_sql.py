from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os

def main():
    spark = SparkSession.builder \
        .appName("Task2_SQL") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Base path determination
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)

    # Paths
    offline_path = "file://" + os.path.join(project_root, "data", "raw", "ccf_offline_stage1_train.csv")
    task1_output_path = "file://" + os.path.join(project_root, "output", "task1", "online_consumption_table")
    task2_output_root = os.path.join(project_root, "output", "task2")
    coupon_time_output = os.path.join(task2_output_root, "coupon_time_dist")
    merchant_ratio_output = os.path.join(task2_output_root, "merchant_pos_ratio")
    
    # --- Task 2.1: Coupon Usage Time Distribution ---
    print("Running Task 2.1...")
    df_offline = spark.read.csv(offline_path, header=True)
    
    # Filter used coupons: Date != 'null' AND Coupon_id != 'null'
    df_used = df_offline.filter((col("Date") != "null") & (col("Coupon_id") != "null"))
    
    # Extract day from Date (yyyyMMdd)
    df_used = df_used.withColumn("day", col("Date").substr(7, 2).cast("int"))
    
    # Categorize: Early (<=10), Mid (11-20), Late (>20)
    df_categorized = df_used.withColumn("period", 
        when(col("day") <= 10, "early")
        .when(col("day") <= 20, "mid")
        .otherwise("late")
    )
    
    # Pivot to count occurrences
    df_counts = df_categorized.groupBy("Coupon_id").pivot("period", ["early", "mid", "late"]).count().na.fill(0)
    
    # Calculate probabilities
    df_total = df_counts.withColumn("total", col("early") + col("mid") + col("late"))
    
    df_result_2_1 = df_total.select(
        col("Coupon_id"),
        (col("early") / col("total")).alias("early_prob"),
        (col("mid") / col("total")).alias("mid_prob"),
        (col("late") / col("total")).alias("late_prob")
    )
    
    print("Task 2.1 Sample Output:")
    df_result_2_1.show(10, truncate=False)

    # Persist results to structured output
    df_result_2_1.coalesce(1).write.mode("overwrite").option("header", True).csv(coupon_time_output)
    
    # --- Task 2.2: Merchant Positive Ratio ---
    print("\nRunning Task 2.2...")
    
    try:
        # Read output from Task 1.2
        rdd_merchant = spark.sparkContext.textFile(task1_output_path)
        
        if rdd_merchant.isEmpty():
            print("Task 1 output is empty. Skipping Task 2.2.")
        else:
            def parse_line(line):
                parts = line.split(" ")
                # Format: Merchant_id Neg Norm Pos
                return (parts[0], int(parts[1]), int(parts[2]), int(parts[3]))
            
            df_merchant = spark.createDataFrame(rdd_merchant.map(parse_line), ["Merchant_id", "Neg", "Norm", "Pos"])
            
            df_merchant = df_merchant.withColumn("Total", col("Neg") + col("Norm") + col("Pos"))
            
            # Filter out merchants with 0 total (should not happen if from data, but safe to check)
            df_merchant = df_merchant.filter(col("Total") > 0)
            
            df_merchant = df_merchant.withColumn("Pos_Ratio", col("Pos") / col("Total"))
            
            # Sort by Positive Ratio Descending
            df_result_2_2 = df_merchant.orderBy(col("Pos_Ratio").desc()).limit(10)
            
            print("Task 2.2 Top 10 Merchants by Positive Ratio:")
            df_result_2_2.select("Merchant_id", "Pos_Ratio", "Pos", "Total").show(truncate=False)
            df_result_2_2.coalesce(1).write.mode("overwrite").option("header", True).csv(merchant_ratio_output)
            
    except Exception as e:
        print(f"Error in Task 2.2 (likely due to missing Task 1 output): {e}")

    spark.stop()

if __name__ == "__main__":
    main()
