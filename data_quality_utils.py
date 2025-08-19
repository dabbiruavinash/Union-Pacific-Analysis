from pyspark.sql import DataFrame, functions as F
from typing import Dict, List

class DataQualityChecker:
    def __init__(self, spark):
        self.spark = spark
    
    def check_data_quality(self, df: DataFrame, rules: Dict) -> Dict:
        """Comprehensive data quality checks"""
        results = {}
        
        for column, checks in rules.items():
            column_results = {}
            
            if "not_null" in checks:
                null_count = df.filter(F.col(column).isNull()).count()
                column_results["not_null"] = {
                    "passed": null_count == 0,
                    "null_count": null_count
                }
            
            if "unique" in checks:
                total_count = df.count()
                distinct_count = df.select(column).distinct().count()
                column_results["unique"] = {
                    "passed": total_count == distinct_count,
                    "duplicate_count": total_count - distinct_count
                }
            
            if "range" in checks:
                min_val, max_val = checks["range"]
                out_of_range = df.filter((F.col(column) < min_val) | (F.col(column) > max_val)).count()
                column_results["range"] = {
                    "passed": out_of_range == 0,
                    "out_of_range_count": out_of_range
                }
            
            results[column] = column_results
        
        return results
    
    def generate_data_profile(self, df: DataFrame) -> Dict:
        """Generate comprehensive data profile"""
        profile = {}
        
        for field in df.schema.fields:
            col_name = field.name
            col_type = field.dataType
            
            stats = df.agg(
                F.count(col_name).alias("count"),
                F.countDistinct(col_name).alias("distinct_count"),
                F.mean(col_name).alias("mean"),
                F.stddev(col_name).alias("stddev"),
                F.min(col_name).alias("min"),
                F.max(col_name).alias("max")
            ).first()
            
            profile[col_name] = {
                "data_type": str(col_type),
                "count": stats["count"],
                "distinct_count": stats["distinct_count"],
                "null_count": df.count() - stats["count"],
                "mean": stats["mean"],
                "stddev": stats["stddev"],
                "min": stats["min"],
                "max": stats["max"]
            }
        
        return profile