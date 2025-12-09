from pyspark import SparkContext, SparkConf
import os
import shutil

def main():
    conf = SparkConf().setAppName("Task1_RDD").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Base path determination
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)

    # Input path (raw online dataset)
    input_path = "file://" + os.path.join(project_root, "data", "raw", "ccf_online_stage1_train.csv")
    
    # Output paths (structured by task)
    output_path_1 = os.path.join(project_root, "output", "task1", "coupon_counts")
    # Keep the table name aligned with the lab doc: online_consumption_table
    output_path_2 = os.path.join(project_root, "output", "task1", "online_consumption_table")

    # Clean up previous outputs
    if os.path.exists(output_path_1):
        shutil.rmtree(output_path_1)
    if os.path.exists(output_path_2):
        shutil.rmtree(output_path_2)

    # Read data
    raw_rdd = sc.textFile(input_path)
    header = raw_rdd.first()
    data_rdd = raw_rdd.filter(lambda line: line != header)

    # Parse CSV line
    def parse_line(line):
        parts = line.split(',')
        # Table 2: User_id,Merchant_id,Action,Coupon_id,Discount_rate,Date_received,Date
        return {
            'Merchant_id': parts[1],
            'Coupon_id': parts[3],
            'Date': parts[6]
        }

    parsed_rdd = data_rdd.map(parse_line)

    # --- Task 1.1: Coupon Usage Count ---
    # Filter: Date != 'null' AND Coupon_id != 'null'
    coupon_usage_rdd = parsed_rdd.filter(lambda row: row['Date'] != 'null' and row['Coupon_id'] != 'null')
    coupon_counts = coupon_usage_rdd.map(lambda row: (row['Coupon_id'], 1)) \
                                    .reduceByKey(lambda a, b: a + b) \
                                    .sortBy(lambda x: x[1], ascending=False)
    
    # Format for output: <Coupon_id> <Total Usage Count>
    coupon_counts.map(lambda x: f"{x[0]} {x[1]}").saveAsTextFile("file://" + output_path_1)
    
    print("Task 1.1 Top 10 Coupons:")
    for coupon, count in coupon_counts.take(10):
        print(f"{coupon} {count}")

    # --- Task 1.2: Merchant Coupon Usage ---
    # Logic:
    # Date=null & Coupon_id != null -> Negative (1, 0, 0)
    # Date!=null & Coupon_id = null -> Normal (0, 1, 0)
    # Date!=null & Coupon_id != null -> Positive (0, 0, 1)
    
    def map_merchant_usage(row):
        merchant_id = row['Merchant_id']
        coupon_id = row['Coupon_id']
        date = row['Date']
        
        neg, norm, pos = 0, 0, 0
        
        if date == 'null' and coupon_id != 'null':
            neg = 1
        elif date != 'null' and coupon_id == 'null':
            norm = 1
        elif date != 'null' and coupon_id != 'null':
            pos = 1
            
        return (merchant_id, (neg, norm, pos))

    merchant_stats = parsed_rdd.map(map_merchant_usage) \
                               .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2])) \
                               .sortBy(lambda x: x[0], ascending=True) # Sort by Merchant_id

    # Format: <Mechant_id> <负样本数量> <普通消费数量> <正样本数量>
    merchant_stats.map(lambda x: f"{x[0]} {x[1][0]} {x[1][1]} {x[1][2]}").saveAsTextFile("file://" + output_path_2)

    print("\nTask 1.2 Top 10 Merchants:")
    for merchant, stats in merchant_stats.take(10):
        print(f"{merchant} {stats[0]} {stats[1]} {stats[2]}")

    sc.stop()

if __name__ == "__main__":
    main()
