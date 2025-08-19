import json
from typing import Dict, Any

class ConfigurationManager:
    def __init__(self, config_path: str = "config/app_config.json"):
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            "spark": {
                "app_name": "UnionPacificAnalysis",
                "master": "local[*]",
                "config": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                }
            },
            "data_quality": {
                "rules": {
                    "train_id": ["not_null", "unique"],
                    "status": ["not_null"]
                }
            }
        }
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark configuration"""
        return self.config.get("spark", {}).get("config", {})
    
    def get_data_quality_rules(self) -> Dict[str, list]:
        """Get data quality rules"""
        return self.config.get("data_quality", {}).get("rules", {})