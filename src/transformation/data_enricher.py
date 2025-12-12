import pandas as pd
import numpy as np
from typing import Dict, List
import time
import re
from collections import Counter

class DataEnricher:    
    def __init__(self, config, logger):
        self.config = config
        self.pipeline_logger = logger
        self.logger = logger.get_logger("DataEnricher")
        self.analysis_config = config.get_analysis_config()
    
    def enrich_creators_data(self, creators_df: pd.DataFrame, videos_df: pd.DataFrame) -> pd.DataFrame:
        '''
            first aggregate video stats grouped by creator 
                count - num of vid, sum - total views across all vid, mean- avg views per vid, max- most views vis
                engagement rate - avg engagement ate
            merge creator data based on id 
            set avg engagement rate is around 0.15 so >= 0.15 will be high
            so viral = avg engagement rate > 0.15 and max_views >= 200000
        '''
        self.logger.info("Starting creators data enrichment")
        
        video_stats = videos_df.groupby('creator_id').agg({
            'views': ['count', 'sum', 'mean', 'max'],
            'engagement_rate': 'mean'
        }).round(2)
        
        video_stats.columns = ['total_videos', 'total_views', 'avg_views_per_video', 'max_views', 'avg_engagement_rate']
        video_stats = video_stats.reset_index()
        
        enriched_df = creators_df.merge(video_stats, on='creator_id', how='left')
        enriched_df = enriched_df.fillna(0)
        
        enriched_df['performance_tier'] = 'low'
        enriched_df.loc[enriched_df['avg_engagement_rate'] >= 0.15, 'performance_tier'] = 'high'
        enriched_df.loc[
            (enriched_df['avg_engagement_rate'] >= 0.15) & (enriched_df['max_views'] >= 200000), 
            'performance_tier'
        ] = 'viral'
        
        self.logger.info("Creators data enrichment completed")
        return enriched_df
    
    def enrich_videos_data(self, videos_df: pd.DataFrame, creators_df: pd.DataFrame) -> pd.DataFrame:
        """
         - merge creator info with video based on creator id 
          - video performance 
             if views> 200000 - viral 
             views> 400000 - super viral 
        """
        self.logger.info("Starting videos data enrichment")
        
        creator_cols = ['creator_id', 'username', 'follower_count', 'category']
        creator_info = creators_df[creator_cols].copy()
        enriched_df = videos_df.merge(creator_info, on='creator_id', how='left')
        
        enriched_df['video_performance'] = 'normal'
        enriched_df.loc[enriched_df['views'] >= 200000, 'video_performance'] = 'viral'
        enriched_df.loc[enriched_df['views'] >= 400000, 'video_performance'] = 'super_viral'
        
        self.logger.info("Videos data enrichment completed")
        return enriched_df
    
    def enrich_all_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        self.logger.info("Starting data enrichment process")
        
        enriched_data = data.copy()
        
        if 'creators' in data and 'videos' in data:
            enriched_data['creators'] = self.enrich_creators_data(
                data['creators'], 
                data['videos']
            )
            
            enriched_data['videos'] = self.enrich_videos_data(
                data['videos'], 
                enriched_data['creators']
            )
            
            enriched_data['creators_final'] = self.create_final_creator_table(
                enriched_data['creators'],
                enriched_data['videos']
            )
        
        self.logger.info("Data enrichment process completed")
        return enriched_data
    
    def compute_top_category(self, creators_df: pd.DataFrame, videos_df: pd.DataFrame) -> pd.DataFrame:
        """
        - So, compute the top category for each creator based on vid performance.
        - and top category is determined by the categiry with highest total views per creator.
        """
        self.logger.info("Computing top category per creator")
        
        category_performance = videos_df.groupby(['creator_id', 'category']).agg({
            'views': 'sum',
            'engagement_rate': 'mean'
        }).reset_index()
        
        top_category_idx = category_performance.groupby('creator_id')['views'].idxmax()
        top_category_df = category_performance.loc[top_category_idx, ['creator_id', 'category']].rename(
            columns={'category': 'top_category'}
        )
        
        result_df = creators_df.merge(top_category_df, on='creator_id', how='left')
        
        if 'category' in result_df.columns:
            result_df['top_category'] = result_df['top_category'].fillna(result_df['category'])
        result_df['top_category'] = result_df['top_category'].fillna('other')
        
        self.logger.info("Top category computation completed")
        return result_df
    
    def extract_top_keywords(self, creators_df: pd.DataFrame, videos_df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
        """
        - Extract top keywords from video caption and creator bios.
        - and return creators df with top_keywords column.
        """
        self.logger.info(f"Extracting top {top_n} keywords per creator")
        
        stopwords = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
            'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been', 'be', 'have', 'has', 'had',
            'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must',
            'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they',
            'my', 'your', 'his', 'her', 'its', 'our', 'their', 'me', 'him', 'us', 'them'
        }
        
        def extract_keywords_from_text(text: str) -> List[str]:
            """Extract keywords from a single text string"""
            if pd.isna(text) or not isinstance(text, str):
                return []
            
            words = re.findall(r'\b[a-z]{3,}\b', text.lower())
            
            keywords = [w for w in words if w not in stopwords and len(w) >= 3]
            return keywords
        
        creator_keywords = {}
        
        for creator_id in creators_df['creator_id'].unique():
            creator_videos = videos_df[videos_df['creator_id'] == creator_id]
            captions = creator_videos['caption'].dropna().tolist() if 'caption' in creator_videos.columns else []
            
            creator_row = creators_df[creators_df['creator_id'] == creator_id]
            bio = creator_row['bio'].iloc[0] if 'bio' in creator_row.columns and len(creator_row) > 0 else ''
            
            all_text = ' '.join(captions)
            if bio:
                all_text += ' ' + str(bio)
            
            keywords = extract_keywords_from_text(all_text)
            keyword_counts = Counter(keywords)
            
            top_keywords_list = [word for word, count in keyword_counts.most_common(top_n)]
            creator_keywords[creator_id] = ', '.join(top_keywords_list) if top_keywords_list else ''
        
        result_df = creators_df.copy()
        result_df['top_keywords'] = result_df['creator_id'].map(creator_keywords).fillna('')
        
        self.logger.info("Top keywords extraction completed")
        return result_df
    
    def calculate_virality_score(
        self, 
        avg_engagement_rate: float, 
        max_views: float, 
        total_views: float, 
        follower_count: float
    ) -> float:
        """
        - calc a numeric virality score (0-100) based on mult factors.
        
        Factors:
            - engagement rate (40% w): high eng = higher score
            - Max views (40% w): high peak views = higher score
            - Reach multiplier (20% w): vw relative to follower count
        """
        engagement_score = min(avg_engagement_rate / 0.5, 1.0) if avg_engagement_rate > 0 else 0.0
        
        views_score = min(max_views / 1000000.0, 1.0) if max_views > 0 else 0.0
        
        if follower_count > 0:
            reach_multiplier = min(total_views / (follower_count * 10.0), 1.0)
        else:
            reach_multiplier = min(total_views / 100000.0, 1.0) if total_views > 0 else 0.0
        
        virality_score = (engagement_score * 0.4 + views_score * 0.4 + reach_multiplier * 0.2) * 100
        
        return round(virality_score, 2)
    
    def add_virality_score(self, creators_df: pd.DataFrame) -> pd.DataFrame:
        """
        - add virality_score column to creators df.
        """
        self.logger.info("Calculating virality scores")
        
        result_df = creators_df.copy()
        
        avg_engagement = result_df.get('avg_engagement_rate', pd.Series([0] * len(result_df)))
        max_views = result_df.get('max_views', pd.Series([0] * len(result_df)))
        total_views = result_df.get('total_views', pd.Series([0] * len(result_df)))
        follower_count = result_df.get('follower_count', pd.Series([0] * len(result_df)))
        
        avg_engagement = avg_engagement.fillna(0)
        max_views = max_views.fillna(0)
        total_views = total_views.fillna(0)
        follower_count = follower_count.fillna(0)
        
        virality_scores = []
        for idx in result_df.index:
            score = self.calculate_virality_score(
                float(avg_engagement.iloc[idx]),
                float(max_views.iloc[idx]),
                float(total_views.iloc[idx]),
                float(follower_count.iloc[idx])
            )
            virality_scores.append(score)
        
        result_df['virality_score'] = virality_scores
        
        self.logger.info("Virality scores calculated")
        return result_df
    
    def create_final_creator_table(self, creators_df: pd.DataFrame, videos_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create the final creator table
        
        Req cols:
        - creator_id
        - username
        - follower_count
        - avg_views
        - top_category
        - avg_engagement
        - virality_score
        - top_keywords
        """
        self.logger.info("Creating final creator table")
        
        final_df = creators_df.copy()
        
        final_df = self.compute_top_category(final_df, videos_df)
        
        final_df = self.extract_top_keywords(final_df, videos_df)
        
        final_df = self.add_virality_score(final_df)
        
        required_columns = {
            'creator_id': 'creator_id',
            'username': 'username',
            'follower_count': 'follower_count',
            'avg_views': 'avg_views',
            'top_category': 'top_category',
            'avg_engagement_rate': 'avg_engagement',
            'virality_score': 'virality_score',
            'top_keywords': 'top_keywords'
        }
        
        final_table = pd.DataFrame()
        for req_col, source_col in required_columns.items():
            if source_col in final_df.columns:
                final_table[req_col] = final_df[source_col]
            else:
                if req_col == 'top_category':
                    final_table[req_col] = final_df.get('category', 'other')
                elif req_col == 'avg_engagement':
                    final_table[req_col] = final_df.get('avg_engagement_rate', 0.0)
                elif req_col == 'virality_score':
                    final_table[req_col] = 0.0
                elif req_col == 'top_keywords':
                    final_table[req_col] = ''
                else:
                    final_table[req_col] = final_df.get(source_col, '')
        
        final_table = final_table.fillna({
            'follower_count': 0,
            'avg_views': 0,
            'top_category': 'other',
            'avg_engagement': 0.0,
            'virality_score': 0.0,
            'top_keywords': ''
        })
        
        self.logger.info(f"Final creator table created with {len(final_table)} rows and {len(final_table.columns)} columns")
        return final_table