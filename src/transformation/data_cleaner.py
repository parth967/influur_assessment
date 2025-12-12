import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import time

class DataCleaner:    
    def __init__(self, config, logger):
        self.config = config
        self.pipeline_logger = logger
        self.logger = logger.get_logger("DataCleaner")
        self.quality_config = config.get_data_quality_config()
    
    def validate_required_columns(self, df: pd.DataFrame, dataset_name: str) -> bool:
        required_cols = self.quality_config.get('required_columns', {}).get(dataset_name, [])
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            self.logger.warning(f"Missing required columns in {dataset_name}: {missing_cols}")
            return False
        return True
    
    def clean_creators_data(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
            Required field is  : creators
            cleanup steps:
                follower_count: median
                avg_views: median
                category: median
            engagement_rate: views per follower 
                lets take avg_views and we can divied it by follower
                and default will be 0 

        '''
        self.logger.info("Starting creators data cleaning")
        cleaned_df = df.copy()
        
        if not self.validate_required_columns(cleaned_df, 'creators'):
            raise ValueError("Creators data missing required columns")
        
        if 'follower_count' in cleaned_df.columns:
            cleaned_df['follower_count'].fillna(cleaned_df['follower_count'].median(), inplace=True)
        
        if 'avg_views' in cleaned_df.columns:
            cleaned_df['avg_views'].fillna(cleaned_df['avg_views'].median(), inplace=True)
        
        if 'category' in cleaned_df.columns:
            cleaned_df['category'].fillna('other', inplace=True)
        
        if 'follower_count' in cleaned_df.columns and 'avg_views' in cleaned_df.columns:
            cleaned_df['engagement_rate'] = cleaned_df['avg_views'] / (cleaned_df['follower_count'] + 1)
            cleaned_df['engagement_rate'] = cleaned_df['engagement_rate'].fillna(0)
        
        self.logger.info("Creators data cleaning completed")
        return cleaned_df
    
    def clean_videos_data(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
            Required field is  : videos
            like/ comments and share are numeric cols so lets put 0 to avoid exception where val is null
            cleanup steps:
                views : ignore if video does not have views 
                like rate : like per view
                comment_rate : comment per view
                share_rate: share per view
                
            total engagement: total count 
            engagement rate : total engagement per view 
        
        '''
        self.logger.info("Starting videos data cleaning")
        cleaned_df = df.copy()
        
        if not self.validate_required_columns(cleaned_df, 'videos'):
            raise ValueError("Videos data missing required columns")
        
        numeric_cols = ['likes', 'comments', 'shares']
        for col in numeric_cols:
            if col in cleaned_df.columns:
                cleaned_df[col].fillna(0, inplace=True)
        
        if 'views' in cleaned_df.columns:
            cleaned_df = cleaned_df.dropna(subset=['views'])
        
        if 'likes' in cleaned_df.columns:
            cleaned_df['like_rate'] = cleaned_df['likes'] / (cleaned_df['views'] + 1)
        
        if 'comments' in cleaned_df.columns:
            cleaned_df['comment_rate'] = cleaned_df['comments'] / (cleaned_df['views'] + 1)
        
        if 'shares' in cleaned_df.columns:
            cleaned_df['share_rate'] = cleaned_df['shares'] / (cleaned_df['views'] + 1)
        
        engagement_cols = ['likes', 'comments', 'shares']
        available_cols = [col for col in engagement_cols if col in cleaned_df.columns]
        if available_cols:
            cleaned_df['total_engagement'] = cleaned_df[available_cols].sum(axis=1)
            cleaned_df['engagement_rate'] = cleaned_df['total_engagement'] / (cleaned_df['views'] + 1)
        
        self.logger.info("Videos data cleaning completed")
        return cleaned_df
    
    def clean_all_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        self.logger.info("Starting data cleaning process")
        cleaned_data = {}
        
        if 'creators' in data:
            cleaned_data['creators'] = self.clean_creators_data(data['creators'])
        
        if 'videos' in data:
            cleaned_data['videos'] = self.clean_videos_data(data['videos'])
        
        self.logger.info("Data cleaning process completed")
        return cleaned_data