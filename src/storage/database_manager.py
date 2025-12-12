import pandas as pd
import os
from typing import Dict, Optional
import time

try:
    from deltalake import DeltaTable, write_deltalake
    import pyarrow as pa
    DELTA_AVAILABLE = True
except ImportError:
    DELTA_AVAILABLE = False
    import warnings
    warnings.warn("Delta Lake not available. Install with: pip install deltalake pyarrow")


class DatabaseManager:    
    def __init__(self, config, logger):
        self.config = config
        self.pipeline_logger = logger
        self.logger = logger.get_logger("DatabaseManager")
        self.delta_base_path = config.get_output_config().get('delta_base_path', 'output/delta_tables')
        self.setup_database()
    
    def setup_database(self):
        os.makedirs(self.delta_base_path, exist_ok=True)
        self.logger.info(f"Setting up Delta Lake storage at {self.delta_base_path}")
    
    def get_table_path(self, table_name: str) -> str:
        return os.path.join(self.delta_base_path, table_name)
    
    def save_dataframe_to_db(self, df: pd.DataFrame, table_name: str, mode: str = 'overwrite'):
    
        start_time = time.time()
        
        try:
            table_path = self.get_table_path(table_name)
            
            if DELTA_AVAILABLE:
                df_clean = df.copy()
                
                for col in df_clean.columns:
                    if df_clean[col].dtype == 'object':
                        df_clean[col] = df_clean[col].where(pd.notnull(df_clean[col]), None)
                    elif df_clean[col].dtype.name == 'category':
                        df_clean[col] = df_clean[col].astype(str)
                
                df_clean = df_clean.reset_index(drop=True)
                
                try:
                    arrow_table = pa.Table.from_pandas(df_clean, preserve_index=False)
                    write_deltalake(
                        table_path,
                        arrow_table,
                        mode=mode
                    )
                except (TypeError, ValueError) as e1:
                    self.logger.info(f"Direct table write failed, trying RecordBatchReader: {e1}")
                    try:
                        arrow_table = pa.Table.from_pandas(df_clean, preserve_index=False)
                        batches = arrow_table.to_batches()
                        reader = pa.RecordBatchReader.from_batches(arrow_table.schema, batches)
                        write_deltalake(
                            table_path,
                            reader,
                            mode=mode
                        )
                    except Exception as e2:
                        # Delta Lake has compatibility issues, fall back to Parquet
                        self.logger.warning(f"Delta Lake write failed: {e2}. Using Parquet format instead.")
                        raise e2  # Re-raise to trigger Parquet fallback
                        
        except (TypeError, ValueError, ImportError, Exception) as delta_error:
            if DELTA_AVAILABLE:
                self.logger.info(f"Delta Lake write failed for {table_name}, using Parquet format instead: {delta_error}")
            else:
                self.logger.info(f"Using Parquet format for {table_name} (Delta Lake not available)")
            
            os.makedirs(table_path, exist_ok=True)
            parquet_path = os.path.join(table_path, f"{table_name}.parquet")
            
            if mode == 'append' and os.path.exists(parquet_path):
                existing_df = pd.read_parquet(parquet_path, engine='pyarrow')
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.to_parquet(parquet_path, index=False, engine='pyarrow')
            else:
                df.to_parquet(parquet_path, index=False, engine='pyarrow')
            
            duration = time.time() - start_time
            self.logger.info(f"Saved {len(df)} rows to Parquet '{table_name}' at {parquet_path}")
            self.pipeline_logger.log_pipeline_step(f"Save {table_name} to Parquet", "SUCCESS", duration)
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Failed to save {table_name}: {str(e)}")
            self.pipeline_logger.log_pipeline_step(f"Save {table_name}", "FAILED", duration)
            raise
    
    def load_dataframe_from_db(self, table_name: str, version: Optional[int] = None) -> pd.DataFrame:
        start_time = time.time()
        
        try:
            table_path = self.get_table_path(table_name)
            
            if DELTA_AVAILABLE:
                delta_table = DeltaTable(table_path, version=version)
                df = delta_table.to_pandas()
                
                duration = time.time() - start_time
                self.logger.info(f"Loaded {len(df)} rows from Delta table '{table_name}'")
                self.pipeline_logger.log_pipeline_step(f"Load {table_name} from Delta", "SUCCESS", duration)
            else:
                parquet_path = os.path.join(table_path, f"{table_name}.parquet")
                if not os.path.exists(parquet_path):
                    raise FileNotFoundError(f"Table {table_name} not found at {parquet_path}")
                
                df = pd.read_parquet(parquet_path, engine='pyarrow')
                
                if version is not None:
                    self.logger.warning(f"Version parameter ignored. Parquet doesn't support time travel. Use Delta Lake for version queries.")
                
                duration = time.time() - start_time
                self.logger.info(f"Loaded {len(df)} rows from Parquet '{table_name}'")
                self.pipeline_logger.log_pipeline_step(f"Load {table_name} from Parquet", "SUCCESS", duration)
            
            return df
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Failed to load {table_name}: {str(e)}")
            self.pipeline_logger.log_pipeline_step(f"Load {table_name}", "FAILED", duration)
            raise
    
    def upsert_data(self, df: pd.DataFrame, table_name: str, merge_key: str):
        start_time = time.time()
        
        try:
            table_path = self.get_table_path(table_name)
            parquet_path = os.path.join(table_path, f"{table_name}.parquet")
            
            if DELTA_AVAILABLE:
                    
                if os.path.exists(table_path) and os.path.exists(os.path.join(table_path, '_delta_log')):
                    existing_table = DeltaTable(table_path)
                    existing_df = existing_table.to_pandas()
                    
                    existing_df = existing_df[~existing_df[merge_key].isin(df[merge_key])]
                    
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    
                    for col in combined_df.columns:
                        if combined_df[col].dtype == 'object':
                            combined_df[col] = combined_df[col].astype(str).replace('nan', None)
                    
                    arrow_table = pa.Table.from_pandas(combined_df, preserve_index=False)
                    write_deltalake(
                        table_path,
                        arrow_table,
                        mode='overwrite'
                    )
                else:
                    
                    df_clean = df.copy()
                    for col in df_clean.columns:
                        if df_clean[col].dtype == 'object':
                            df_clean[col] = df_clean[col].astype(str).replace('nan', None)
                    
                    arrow_table = pa.Table.from_pandas(df_clean, preserve_index=False)
                    write_deltalake(
                        table_path,
                        arrow_table,
                        mode='overwrite'
                    )
            else:
                os.makedirs(table_path, exist_ok=True)
                
                if os.path.exists(parquet_path):
                    existing_df = pd.read_parquet(parquet_path, engine='pyarrow')
                    
                    existing_df = existing_df[~existing_df[merge_key].isin(df[merge_key])]
                    
                    combined_df = pd.concat([existing_df, df], ignore_index=True)

                    combined_df.to_parquet(parquet_path, index=False, engine='pyarrow')
                else:

                    df.to_parquet(parquet_path, index=False, engine='pyarrow')
            
            duration = time.time() - start_time
            self.logger.info(f"Upserted {len(df)} rows to table '{table_name}'")
            self.pipeline_logger.log_pipeline_step(f"Upsert {table_name}", "SUCCESS", duration)
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Failed to upsert {table_name}: {str(e)}")
            self.pipeline_logger.log_pipeline_step(f"Upsert {table_name}", "FAILED", duration)
            raise
    
    def save_all_data(self, data: Dict[str, pd.DataFrame]):
        """Save all DataFrames to Delta tables"""
        self.logger.info("Saving all data to Delta Lake")
        
        for table_name, df in data.items():
            self.save_dataframe_to_db(df, table_name, mode='overwrite')
        
        self.logger.info("All data saved to Delta Lake successfully")
    
    def get_table_info(self) -> Dict:
        try:
            table_info = {}
            
            if not os.path.exists(self.delta_base_path):
                return table_info
            
            for item in os.listdir(self.delta_base_path):
                table_path = os.path.join(self.delta_base_path, item)
                if os.path.isdir(table_path):
                    try:
                        if DELTA_AVAILABLE:
                            delta_log_path = os.path.join(table_path, '_delta_log')
                            if os.path.exists(delta_log_path):
                                delta_table = DeltaTable(table_path)
                                df = delta_table.to_pandas()
                                
                                table_info[item] = {
                                    'row_count': len(df),
                                    'columns': list(df.columns),
                                    'path': table_path,
                                    'format': 'delta'
                                }
                        else:
                            parquet_path = os.path.join(table_path, f"{item}.parquet")
                            if os.path.exists(parquet_path):
                                df = pd.read_parquet(parquet_path, engine='pyarrow')
                                
                                table_info[item] = {
                                    'row_count': len(df),
                                    'columns': list(df.columns),
                                    'path': parquet_path,
                                    'format': 'parquet'
                                }
                    except Exception as e:
                        self.logger.warning(f"Could not read table {item}: {str(e)}")
            
            return table_info
                
        except Exception as e:
            self.logger.error(f"Failed to get table info: {str(e)}")
            return {}
    
    def get_table_history(self, table_name: str) -> Dict:
        """Get version history for a Delta table"""
        if not DELTA_AVAILABLE:
            raise ImportError("Delta Lake is not installed. Install with: pip install deltalake")
        
        try:
            table_path = self.get_table_path(table_name)
            delta_table = DeltaTable(table_path)
            history = delta_table.history()
            return history
        except Exception as e:
            self.logger.error(f"Failed to get history for {table_name}: {str(e)}")
            return {}