from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from config.settings import SCDType

class SCDTransformer:
    @staticmethod
    def apply_scd1(current_df: DataFrame, updates_df: DataFrame, key_columns: list) -> DataFrame:
        """SCD Type 1 - Overwrite existing records"""
        current_df.createOrReplaceTempView("current")
        updates_df.createOrReplaceTempView("updates")
        
        query = f"""
        SELECT 
            COALESCE(u.*, c.*) as merged_data
        FROM current c
        FULL OUTER JOIN updates u
        ON {' AND '.join([f'c.{col} = u.{col}' for col in key_columns])}
        """
        
        return current_df.sparkSession.sql(query)
    
    @staticmethod
    def apply_scd2(current_df: DataFrame, updates_df: DataFrame, key_columns: list) -> DataFrame:
        """SCD Type 2 - Maintain history with effective dates"""
        window_spec = Window.partitionBy(key_columns).orderBy(F.col("effective_date").desc())
        
        # Mark current records as expired
        expired_records = current_df.withColumn("is_current", F.lit(False))
        
        # Combine with new records
        all_records = updates_df.unionByName(expired_records)
        
        # Add version numbers
        result = all_records.withColumn("version", F.row_number().over(window_spec)) \
                           .withColumn("is_current", F.col("version") == 1)
        
        return result
    
    @staticmethod
    def apply_scd3(current_df: DataFrame, updates_df: DataFrame, key_columns: list, 
                  tracked_columns: list) -> DataFrame:
        """SCD Type 3 - Keep limited history with previous values"""
        # Add previous value columns
        for col in tracked_columns:
            current_df = current_df.withColumn(f"prev_{col}", F.lit(None))
        
        # Update with new values, moving current to previous
        updated = updates_df.join(current_df, key_columns, "left_outer") \
            .select(
                *[updates_df[col] for col in updates_df.columns],
                *[F.coalesce(current_df[col], updates_df[col]).alias(f"prev_{col}") 
                for col in tracked_columns]
            )
        
        return updated