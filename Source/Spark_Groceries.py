# ==============================================================================
# 1. IMPORT CÁC THƯ VIỆN CẦN THIẾT
# ==============================================================================
import os
import findspark
import requests
import time
import psutil
import pandas as pd
import matplotlib.pyplot as plt
import json
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, avg, sum as spark_sum, count, concat_ws, lit, rand

# ==============================================================================
# 2. THIẾT LẬP MÔI TRƯỜNG VÀ CẤU HÌNH
# ==============================================================================
os.environ['HADOOP_HOME'] = '/home/mqang/Desktop/hadoop-3.2.1'
os.environ['HADOOP_CONF_DIR'] = '/home/mqang/Desktop/hadoop-3.2.1/etc/hadoop'
findspark.init()
HISTORY_SERVER_API = "http://mqang-legion-5-16ach6:18080/api/v1"

# ==============================================================================
# 3. HÀM LẤY METRICS TỪ HISTORY SERVER
# ==============================================================================
def get_metrics_from_history(app_id):
    metrics = {
        'app_id': app_id, 'max_executor_memory_mb': 0, 'total_executor_run_time_sec': 0,
        'shuffle_read_mb': 0, 'shuffle_write_mb': 0, 'succeeded': False
    }
    print(f"--- Fetching metrics for App ID: {app_id} ---")
    try:
        stages_url = f"{HISTORY_SERVER_API}/applications/{app_id}/stages"
        response = requests.get(stages_url, params={'status': 'complete'}, timeout=15)
        response.raise_for_status()
        stages_data = response.json()
        if not stages_data: return metrics
        metrics['total_executor_run_time_sec'] = sum(s.get('executorRunTime', 0) for s in stages_data) / 1000.0
        metrics['shuffle_read_mb'] = sum(s.get('shuffleReadBytes', 0) for s in stages_data) / (1024 ** 2)
        metrics['shuffle_write_mb'] = sum(s.get('shuffleWriteBytes', 0) for s in stages_data) / (1024 ** 2)
        executors_url = f"{HISTORY_SERVER_API}/applications/{app_id}/allexecutors"
        response = requests.get(executors_url, timeout=15)
        response.raise_for_status()
        executors_data = response.json()
        non_driver_execs = [e for e in executors_data if e.get('id') != 'driver']
        if non_driver_execs:
            print(f"Found {len(non_driver_execs)} non-driver executors. Analyzing peak memory.")
            peak_mems_bytes = []
            for e in non_driver_execs:
                peak_metrics = e.get('peakMemoryMetrics')
                if peak_metrics:
                    jvm_heap = peak_metrics.get('JVMHeapMemory', 0); jvm_offheap = peak_metrics.get('JVMOffHeapMemory', 0)
                    peak_mems_bytes.append(jvm_heap + jvm_offheap)
                else: peak_mems_bytes.append(e.get('maxMemory', 0))
            if peak_mems_bytes: metrics['max_executor_memory_mb'] = max(peak_mems_bytes) / (1024 ** 2)
        metrics['succeeded'] = True
        print(f"Successfully fetched metrics for app {app_id}. Max executor memory: {metrics['max_executor_memory_mb']:.2f} MB")
    except Exception as e:
        print(f"Error fetching/parsing metrics for app {app_id}: {e}")
    return metrics

# ==============================================================================
# 4. HÀM TIỀN XỬ LÝ DỮ LIỆU GROCERIES
# ==============================================================================
def preprocess_groceries_data(spark, file_path):
    """Đọc và chuyển đổi dữ liệu Groceries từ định dạng CSV."""
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    # Tạo các cột cần thiết để tương thích với logic thuật toán
    df = df.withColumn("InvoiceNo", concat_ws("_", col("Member_number"), col("Date"))) \
           .withColumn("Item", col("itemDescription")) \
           .withColumn("Quantity", lit(1)) \
           .withColumn("UnitPrice", (rand(seed=42) * 9 + 1).cast("double")) \
           .withColumn("CustomerID", col("Member_number").cast("string"))

    clean_df = df.dropna(subset=["InvoiceNo", "Item", "Quantity", "UnitPrice", "CustomerID"])
    clean_df = clean_df.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))
    return clean_df

# ==============================================================================
# 5. HÀM CHẠY CÁC THUẬT TOÁN SPARK
# ==============================================================================
def run_huomil(conf, file_path, minUtil, minUO_1item, minUO):
    """Chạy thuật toán HUOMIL và in ra đầy đủ các kết quả trung gian."""
    print("\n" + "="*20 + " Starting HUOMIL Application (Groceries) " + "="*20)
    spark = SparkSession.builder.config(conf=conf.setAppName("Groceries_HUOMIL_App")).getOrCreate()
    sc = spark.sparkContext; app_id = sc.applicationId
    print(f"HUOMIL running with Application ID: {app_id}")
    process = psutil.Process(os.getpid()); start_time = time.time(); cpu_start = process.cpu_times()

    clean_df = preprocess_groceries_data(spark, file_path); clean_df.cache()
    profit_df = clean_df.groupBy("Item").agg(avg("UnitPrice").alias("unit_profit"))
    huomil_df = clean_df.join(profit_df, "Item")
    huomil_df = huomil_df.withColumn("item_util", col("Quantity") * col("unit_profit"))
    tu_df = huomil_df.groupBy("InvoiceNo").agg(spark_sum("item_util").alias("TU"))
    #huomil_df = huomil_df.join(tu_df, "InvoiceNo").filter(col("TU") >= minUtil)
    huomil_df = huomil_df.join(tu_df, "InvoiceNo").filter(col("TU") > 0)
    print(f"HUOMIL Transactions: {huomil_df.select('InvoiceNo').distinct().count()}")
    
    uolist_df = huomil_df.select("Item", "InvoiceNo", "item_util", "TU") \
                         .groupBy("Item", "InvoiceNo", "TU").agg(spark_sum("item_util").alias("IU"))
    uolist_df.cache()
    
    print("\n====== HUOMIL: VALID ITEMS in UOList ======"); uolist_df.show(); print("="*40)

    candidate_1item = uolist_df.groupBy("Item").agg(
        spark_sum("IU").alias("total_util"),
        (spark_sum(col("IU") / col("TU")) / count("*")).alias("avg_uo")
    ).filter((col("total_util") >= minUtil) & (col("avg_uo") >= minUO_1item))
    
    print("\n====== HUOMIL: 1-ITEMSETS ======"); candidate_1item.show(); print("="*30)
    results_1item_count = candidate_1item.count()

    valid_1items = candidate_1item.select("Item").rdd.flatMap(lambda x: x).collect()
    valid_items_df_filtered = uolist_df.filter(col("Item").isin(valid_1items))
    
    uolist1 = valid_items_df_filtered.selectExpr("Item as item1", "InvoiceNo", "IU as IU1", "TU")
    uolist2 = valid_items_df_filtered.selectExpr("Item as item2", "InvoiceNo", "IU as IU2", "TU")
    joined_2 = uolist1.join(uolist2, on=["InvoiceNo", "TU"]).filter(col("item1") < col("item2"))
    print(f"\nHUOMIL 2-itemset candidates: {joined_2.count()}")
    
    results_2item = joined_2.groupBy("item1", "item2").agg(
        spark_sum(col("IU1") + col("IU2")).alias("total_util"),
        (spark_sum((col("IU1") + col("IU2")) / col("TU")) / count("*")).alias("avg_uo")
    ).filter((col("total_util") >= minUtil) & (col("avg_uo") >= minUO))
    results_2item = results_2item.withColumn("itemset", concat_ws(",", "item1", "item2"))
    
    print("\n====== HUOMIL: 2-ITEMSETS (Sorted by total_util) ======")
    results_2item.orderBy(col("total_util").desc()).show(20, truncate=False); print("="*50)
    
    itemsets_2_set = set(results_2item.select("itemset").toPandas()["itemset"])
    results_2item_count = len(itemsets_2_set)
    
    duration = time.time() - start_time; cpu_end = process.cpu_times()
    driver_cpu = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
    driver_mem = process.memory_info().rss / (1024 ** 2)
    
    clean_df.unpersist(); uolist_df.unpersist(); spark.stop()
    print("="*20 + " Finished HUOMIL Application " + "="*20)
    return { "app_id": app_id, "itemsets_1_count": results_1item_count, "itemsets_2_count": results_2item_count, "itemsets_2_set": itemsets_2_set, "duration": duration, "driver_cpu": driver_cpu, "driver_mem_mb": driver_mem }

def run_huopm(conf, file_path, minUtil, minUO):
    """Chạy thuật toán HUOPM và in ra đầy đủ các kết quả trung gian."""
    print("\n" + "="*20 + " Starting HUOPM Application (Groceries) " + "="*20)
    spark = SparkSession.builder.config(conf=conf.setAppName("Groceries_HUOPM_App")).getOrCreate()
    sc = spark.sparkContext; app_id = sc.applicationId
    print(f"HUOPM running with Application ID: {app_id}")
    process = psutil.Process(os.getpid()); start_time = time.time(); cpu_start = process.cpu_times()

    clean_df = preprocess_groceries_data(spark, file_path); clean_df.cache()
    huopm_df = clean_df.withColumn("item_util", col("Quantity") * col("UnitPrice"))
    tu_df = huopm_df.groupBy("InvoiceNo").agg(spark_sum("item_util").alias("TU"))
    #huopm_df = huopm_df.join(tu_df, "InvoiceNo").filter(col("TU") >= minUtil)
    huopm_df = huopm_df.join(tu_df, "InvoiceNo").filter(col("TU") > 0)
    print(f"HUOPM Transactions: {huopm_df.select('InvoiceNo').distinct().count()}")

    uolist_df = huopm_df.select("Item", "InvoiceNo", "item_util", "TU") \
                        .groupBy("Item", "InvoiceNo", "TU").agg(spark_sum("item_util").alias("IU"))
    uolist_df.cache()

    results_1item = uolist_df.groupBy("Item").agg(
        spark_sum("IU").alias("total_util"),
        (spark_sum(col("IU") / col("TU")) / count("*")).alias("avg_uo")
    ).filter((col("total_util") >= minUtil) & (col("avg_uo") >= minUO))
    
    print("\n====== HUOPM: 1-ITEMSETS ======"); results_1item.show(); print("="*30)
    results_1item_count = results_1item.count()

    valid_1items = results_1item.select("Item").rdd.flatMap(lambda x: x).collect()
    valid_items_df_filtered = uolist_df.filter(col("Item").isin(valid_1items))

    uolist1 = valid_items_df_filtered.selectExpr("Item as item1", "InvoiceNo", "IU as IU1", "TU")
    uolist2 = valid_items_df_filtered.selectExpr("Item as item2", "InvoiceNo", "IU as IU2", "TU")
    joined_2 = uolist1.join(uolist2, on=["InvoiceNo", "TU"]).filter(col("item1") < col("item2"))
    print(f"\nHUOPM 2-itemset candidates: {joined_2.count()}")

    results_2item = joined_2.groupBy("item1", "item2").agg(
        spark_sum(col("IU1") + col("IU2")).alias("total_util"),
        (spark_sum((col("IU1") + col("IU2")) / col("TU")) / count("*")).alias("avg_uo"),
    ).filter((col("total_util") >= minUtil) & (col("avg_uo") >= minUO))
    results_2item = results_2item.withColumn("itemset", concat_ws(",", "item1", "item2"))

    print("\n====== HUOPM: 2-ITEMSETS (Sorted by total_util) ======");
    results_2item.orderBy(col("total_util").desc()).show(20, truncate=False); print("="*50)
    
    itemsets_2_set = set(results_2item.select("itemset").toPandas()["itemset"])
    results_2item_count = len(itemsets_2_set)

    duration = time.time() - start_time; cpu_end = process.cpu_times()
    driver_cpu = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
    driver_mem = process.memory_info().rss / (1024 ** 2)

    clean_df.unpersist(); uolist_df.unpersist(); spark.stop()
    print("="*20 + " Finished HUOPM Application " + "="*20)
    return { "app_id": app_id, "itemsets_1_count": results_1item_count, "itemsets_2_count": results_2item_count, "itemsets_2_set": itemsets_2_set, "duration": duration, "driver_cpu": driver_cpu, "driver_mem_mb": driver_mem }

# ==============================================================================
# 6. HÀM CHÍNH ĐIỀU PHỐI, PHÂN TÍCH VÀ TRỰC QUAN HÓA
# ==============================================================================
def main():
    """Hàm chính điều phối toàn bộ quá trình: chạy, thu thập, phân tích và lưu biểu đồ."""
    conf = SparkConf().set("spark.eventLog.enabled", "true") \
        .set("spark.eventLog.dir", "hdfs://localhost:9000/tmp/spark-events") \
        .set("spark.sql.shuffle.partitions", "200").set("spark.driver.memory", "4g") \
        .set("spark.executor.memory", "2g").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .set("spark.sql.adaptive.enabled", "true").set("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")

    # Tạo đường dẫn file và các ngưỡng cho tập dữ liệu Groceries
    file_path = "hdfs://localhost:9000/user/mqang/input/Groceries_data.csv"
    minUtil = 12
    minUO_1item = 0.25
    minUO = 0.25

    print("="*20 + " Calculating Initial Groceries Data Statistics " + "="*20)
    spark_stats = SparkSession.builder.config(conf=conf.setAppName("Groceries_Stats_App")).getOrCreate()
    spark_stats.sparkContext.setLogLevel("WARN")
    try:
        clean_df_stats = preprocess_groceries_data(spark_stats, file_path)
        tu_df_stats = clean_df_stats.groupBy("InvoiceNo").agg((spark_sum(col("Quantity") * col("UnitPrice"))).alias("TU"))
        print("\nTransaction Utility (TU) Statistics for Groceries:"); print(tu_df_stats.select("TU").describe().toPandas())
    finally:
        spark_stats.stop(); print("="*20 + " Finished Initial Groceries Data Statistics " + "="*20)

    huomil_results = run_huomil(conf, file_path, minUtil, minUO_1item, minUO)
    huopm_results = run_huopm(conf, file_path, minUtil, minUO)

    wait_time = 60; print(f"\nWaiting {wait_time} seconds for History Server..."); time.sleep(wait_time)
    huomil_spark_metrics = get_metrics_from_history(huomil_results['app_id'])
    huopm_spark_metrics = get_metrics_from_history(huopm_results['app_id'])
    huomil_results.update(huomil_spark_metrics); huopm_results.update(huopm_spark_metrics)

    intersection = huomil_results['itemsets_2_set'].intersection(huopm_results['itemsets_2_set'])
    union = huomil_results['itemsets_2_set'].union(huopm_results['itemsets_2_set'])
    jaccard_similarity = len(intersection) / len(union) if union else 0

    print("\n\n" + "="*20 + " FINAL GROCERIES COMPARISON " + "="*20)
    print(f"Jaccard Similarity between 2-itemsets: {jaccard_similarity:.4f}\n")
    df_compare = pd.DataFrame([
        { "Algorithm": "HUOMIL", "1-itemsets": huomil_results['itemsets_1_count'], "2-itemsets": huomil_results['itemsets_2_count'], "Execution Time (s)": huomil_results['duration'], "Driver CPU (s)": huomil_results['driver_cpu'], "Driver Memory (MB)": huomil_results['driver_mem_mb'], "Max Executor Memory (MB)": huomil_results.get('max_executor_memory_mb', 0), "Shuffle Read (MB)": huomil_results.get('shuffle_read_mb', 0) },
        { "Algorithm": "HUOPM", "1-itemsets": huopm_results['itemsets_1_count'], "2-itemsets": huopm_results['itemsets_2_count'], "Execution Time (s)": huopm_results['duration'], "Driver CPU (s)": huopm_results['driver_cpu'], "Driver Memory (MB)": huopm_results['driver_mem_mb'], "Max Executor Memory (MB)": huopm_results.get('max_executor_memory_mb', 0), "Shuffle Read (MB)": huopm_results.get('shuffle_read_mb', 0) }
    ]); print(df_compare.to_string())

    fig, axs = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle("HUOMIL vs HUOPM Performance Comparison on Groceries Dataset", fontsize=16)
    metrics_to_plot = [("Execution Time (s)", "skyblue"), ("Max Executor Memory (MB)", "purple"), ("Shuffle Read (MB)", "orange"), ("Driver CPU (s)", "salmon"), ("Driver Memory (MB)", "lightgreen")]
    for i, (metric, color) in enumerate(metrics_to_plot):
        ax = axs.flat[i]; ax.bar(df_compare['Algorithm'], df_compare[metric], color=color)
        ax.set_title(metric); ax.set_ylabel(metric.split('(')[-1].replace(')', '').strip()); ax.set_xlabel("Algorithm")
        for index, value in enumerate(df_compare[metric]): ax.text(index, value, f'{value:.2f}', ha='center', va='bottom')
    ax = axs.flat[5]; bar_width = 0.35; x = range(len(df_compare['Algorithm']))
    bar1 = ax.bar([i - bar_width/2 for i in x], df_compare['1-itemsets'], bar_width, label='1-itemsets', color='cyan')
    bar2 = ax.bar([i + bar_width/2 for i in x], df_compare['2-itemsets'], bar_width, label='2-itemsets', color='teal')
    ax.set_title("Itemset Counts"); ax.set_ylabel("Count"); ax.set_xticks(x); ax.set_xticklabels(df_compare['Algorithm'])
    ax.set_xlabel("Algorithm"); ax.legend()
    for bar in bar1: yval = bar.get_height(); ax.text(bar.get_x() + bar.get_width()/2.0, yval, int(yval), va='bottom', ha='center')
    for bar in bar2: yval = bar.get_height(); ax.text(bar.get_x() + bar.get_width()/2.0, yval, int(yval), va='bottom', ha='center')
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    output_image_path = "groceries_performance_comparison.png"
    plt.savefig(output_image_path)
    print(f"\nBiểu đồ đã được lưu thành công vào file: {output_image_path}")

# ==============================================================================
# 7. ĐIỂM BẮT ĐẦU THỰC THI SCRIPT
# ==============================================================================
if __name__ == "__main__":
    main()
