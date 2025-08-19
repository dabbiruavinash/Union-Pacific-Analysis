from pyspark.sql import SparkSession
from pyspark import SparkConf

class SparkSessionManager:
    _instance = None
    
    def __new__(cls, app_name="UnionPacificAnalysis"):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize_spark(app_name)
        return cls._instance
    
    def _initialize_spark(self, app_name):
        conf = SparkConf() \
            .setAppName(app_name) \
            .set("spark.sql.adaptive.enabled", "true") \
            .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .set("spark.sql.adaptive.skewJoin.enabled", "true") \
            .set("spark.sql.autoBroadcastJoinThreshold", "10485760") \
            .set("spark.sql.parquet.compression.codec", "snappy")
        
        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .enableHiveSupport() \
            .getOrCreate()
    
    def get_spark(self):
        return self.spark
    
    def stop_spark(self):
        if self.spark:
            self.spark.stop()
            self._instance = None