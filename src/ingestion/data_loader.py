import pandas as pd
import os
from typing import Dict, Optional
import time


class DataLoader:    
    def __init__(self, config, logger):
        self.config = config
        self.pipeline_logger = logger
        self.logger = logger.get_logger("DataLoader")
        self.data_sources = config.get_data_sources()
    
    def load_csv_file(self, file_path: str, encoding: str = 'utf-8') -> Optional[pd.DataFrame]:
        start_time = time.time()
        
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"File not found: {file_path}")
                return None
            
            df = pd.read_csv(file_path, encoding=encoding)
            
            duration = time.time() - start_time
            self.logger.info(f"Successfully loaded {file_path}: {len(df)} rows, {len(df.columns)} columns")
            self.pipeline_logger.log_pipeline_step(f"Load {os.path.basename(file_path)}", "SUCCESS", duration)
            
            return df
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Failed to load {file_path}: {str(e)}")
            self.pipeline_logger.log_pipeline_step(f"Load {os.path.basename(file_path)}", "FAILED", duration)
            return None
    
    def load_all_data(self) -> Dict[str, pd.DataFrame]:
        self.logger.info("Starting data ingestion process")
        
        data = {}
        
        creators_file = self.data_sources.get('creators_file')
        if creators_file:
            creators_df = self.load_csv_file(creators_file)
            if creators_df is not None:
                data['creators'] = creators_df
        
        videos_file = self.data_sources.get('videos_file')
        if videos_file:
            videos_df = self.load_csv_file(videos_file)
            if videos_df is not None:
                data['videos'] = videos_df
        
        self.logger.info(f"Data ingestion completed. Loaded {len(data)} datasets")
        return data
    
    def get_data_info(self, df: pd.DataFrame, dataset_name: str) -> Dict:
        memory_usage_bytes = df.memory_usage(deep=True).sum()
        memory_usage_mb = round(memory_usage_bytes / (1024 * 1024), 2)
        
        info = {
            'rows': len(df),
            'columns': len(df.columns),
            'missing_values': df.isnull().sum().sum(),
            'memory_usage_mb': memory_usage_mb
        }
        
        self.logger.info(f"Dataset info for {dataset_name}: {info}")
        return info