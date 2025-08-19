from pyspark.sql import SparkSession

class AQEConfigManager:
    def __init__(self, spark):
        self.spark = spark
        self.default_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.autoBroadcastJoinThreshold": "10485760"
        }
    
    def configure_aqe(self, config: Dict = None) -> None:
        """Configure Adaptive Query Execution settings"""
        final_config = {**self.default_config, **(config or {})}
        
        for key, value in final_config.items():
            self.spark.conf.set(key, value)
    
    def get_aqe_stats(self) -> Dict:
        """Get AQE statistics and configuration"""
        aqe_config = {}
        for key in self.default_config.keys():
            aqe_config[key] = self.spark.conf.get(key, "not set")
        
        return aqe_config
    
    def optimize_for_workload(self, workload_type: str) -> None:
        """Optimize AQE for specific workload types"""
        optimizations = {
            "etl": {
                "spark.sql.adaptive.coalescePartitions.minPartitionSize": "67108864",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728"
            },
            "analytics": {
                "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "3",
                "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "268435456"
            }
        }
        
        if workload_type in optimizations:
            self.configure_aqe(optimizations[workload_type])