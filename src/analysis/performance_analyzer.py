"""
Performance analysis module for generating insights
"""
import pandas as pd
import numpy as np
from typing import Dict, List
import json
import time


class PerformanceAnalyzer:    
    def __init__(self, config, logger):
        self.config = config
        self.pipeline_logger = logger
        self.logger = logger.get_logger("PerformanceAnalyzer")
        self.analysis_config = config.get_analysis_config()
    
    def run_full_analysis(self, data: Dict[str, pd.DataFrame]) -> Dict:
        self.logger.info("Starting full performance analysis")
        
        results = {}

        results['trend_detection'] = self.detect_trending_content(data['videos'], data['creators'])
        results['virality_spikes'] = self.detect_virality_spikes(data['videos'])
        results['category_clustering'] = self.cluster_categories(data['videos'])
        
        self.logger.info("Full performance analysis completed")
        return results
    
    def detect_trending_content(self, videos_df: pd.DataFrame, creators_df: pd.DataFrame) -> Dict:
        """
        Detect trending content based on recent performance metrics.
        Simple trend detection: identifies content with high engagement relative to average.
        """
        self.logger.info("Detecting trending content")
        
        avg_engagement_platform = videos_df['engagement_rate'].mean()
        
        trending_threshold = avg_engagement_platform * 1.5
        trending_videos = videos_df[videos_df['engagement_rate'] > trending_threshold].copy()
        
        if 'virality_score' in creators_df.columns:
            avg_virality = creators_df['virality_score'].mean()
            trending_creators = creators_df[
                (creators_df['avg_engagement_rate'] > avg_engagement_platform) &
                (creators_df['virality_score'] > avg_virality)
            ].copy()
        else:
            trending_creators = creators_df[
                creators_df['avg_engagement_rate'] > avg_engagement_platform * 1.2
            ].copy()
        
        category_trends = videos_df.groupby('category').agg({
            'engagement_rate': 'mean',
            'views': 'mean',
            'video_id': 'count'
        }).round(3)
        
        avg_category_engagement = category_trends['engagement_rate'].mean()
        trending_categories = category_trends[
            category_trends['engagement_rate'] > avg_category_engagement
        ].sort_values('engagement_rate', ascending=False)
        
        trend_analysis = {
            'trending_videos_count': len(trending_videos),
            'trending_creators_count': len(trending_creators),
            'trending_categories': trending_categories.to_dict('index'),
            'top_trending_videos': trending_videos.nlargest(5, 'engagement_rate')[
                ['video_id', 'username', 'views', 'engagement_rate', 'category']
            ].to_dict('records') if len(trending_videos) > 0 else [],
            'top_trending_creators': trending_creators.nlargest(5, 'virality_score')[
                ['username', 'avg_engagement_rate', 'virality_score', 'top_category']
            ].to_dict('records') if len(trending_creators) > 0 and 'virality_score' in trending_creators.columns else 
            trending_creators.nlargest(5, 'avg_engagement_rate')[
                ['username', 'avg_engagement_rate', 'category']
            ].to_dict('records') if len(trending_creators) > 0 else []
        }
        
        self.logger.info(f"Trend detection completed: {len(trending_videos)} trending videos, {len(trending_creators)} trending creators")
        return trend_analysis
    
    def detect_virality_spikes(self, videos_df: pd.DataFrame) -> Dict:
        """
        Detect burstiness/virality spikes in video performance.
        Identifies videos with sudden spikes in views/engagement relative to creator's average.
        """
        self.logger.info("Detecting virality spikes")
        
        creator_avg_views = videos_df.groupby('creator_id')['views'].mean()
        creator_avg_engagement = videos_df.groupby('creator_id')['engagement_rate'].mean()
        
        videos_with_avg = videos_df.copy()
        videos_with_avg['creator_avg_views'] = videos_with_avg['creator_id'].map(creator_avg_views)
        videos_with_avg['creator_avg_engagement'] = videos_with_avg['creator_id'].map(creator_avg_engagement)
        
        spike_threshold_views = 2.0
        spike_threshold_engagement = 2.0
        
        view_spikes = videos_with_avg[
            videos_with_avg['views'] > (videos_with_avg['creator_avg_views'] * spike_threshold_views)
        ].copy()
        
        engagement_spikes = videos_with_avg[
            videos_with_avg['engagement_rate'] > (videos_with_avg['creator_avg_engagement'] * spike_threshold_engagement)
        ].copy()
        
        combined_spikes = videos_with_avg[
            (videos_with_avg['views'] > (videos_with_avg['creator_avg_views'] * spike_threshold_views)) &
            (videos_with_avg['engagement_rate'] > (videos_with_avg['creator_avg_engagement'] * spike_threshold_engagement))
        ].copy()
        
        if len(view_spikes) > 0:
            view_spikes['spike_intensity'] = view_spikes['views'] / (view_spikes['creator_avg_views'] + 1)
        
        if len(engagement_spikes) > 0:
            engagement_spikes['spike_intensity'] = engagement_spikes['engagement_rate'] / (engagement_spikes['creator_avg_engagement'] + 0.001)
        
        spike_analysis = {
            'total_view_spikes': len(view_spikes),
            'total_engagement_spikes': len(engagement_spikes),
            'total_combined_spikes': len(combined_spikes),
            'top_view_spikes': view_spikes.nlargest(5, 'spike_intensity')[
                ['video_id', 'username', 'views', 'creator_avg_views', 'spike_intensity', 'category']
            ].to_dict('records') if len(view_spikes) > 0 else [],
            'top_engagement_spikes': engagement_spikes.nlargest(5, 'spike_intensity')[
                ['video_id', 'username', 'engagement_rate', 'creator_avg_engagement', 'spike_intensity', 'category']
            ].to_dict('records') if len(engagement_spikes) > 0 else []
        }
        
        self.logger.info(f"Virality spike detection completed: {len(combined_spikes)} combined spikes detected")
        return spike_analysis
    
    def cluster_categories(self, videos_df: pd.DataFrame) -> Dict:
        """
        Simple category clustering based on performance similarity.
        Groups categories with similar engagement patterns into clusters.
        """
        self.logger.info("Clustering categories by performance")
        
        category_metrics = videos_df.groupby('category').agg({
            'engagement_rate': ['mean', 'std'],
            'views': ['mean', 'std'],
            'likes': 'mean',
            'comments': 'mean',
            'shares': 'mean',
            'video_id': 'count'
        }).round(3)
        
        category_metrics.columns = ['_'.join(col).strip() for col in category_metrics.columns.values]
        category_metrics = category_metrics.reset_index()
        

        def assign_cluster(engagement_rate):
            if engagement_rate < 0.10:
                return 'low_engagement'
            elif engagement_rate < 0.15:
                return 'medium_engagement'
            else:
                return 'high_engagement'
        
        category_metrics['engagement_cluster'] = category_metrics['engagement_rate_mean'].apply(assign_cluster)
        
        avg_views = category_metrics['views_mean'].mean()
        category_metrics['volume_cluster'] = category_metrics['views_mean'].apply(
            lambda x: 'high_volume' if x > avg_views * 1.2 else 'medium_volume' if x > avg_views * 0.8 else 'low_volume'
        )
        
        category_metrics['combined_cluster'] = (
            category_metrics['engagement_cluster'] + '_' + category_metrics['volume_cluster']
        )
        
        cluster_summary = {}
        for cluster in category_metrics['combined_cluster'].unique():
            cluster_categories = category_metrics[category_metrics['combined_cluster'] == cluster]
            cluster_summary[cluster] = {
                'categories': cluster_categories['category'].tolist(),
                'avg_engagement': float(cluster_categories['engagement_rate_mean'].mean()),
                'avg_views': float(cluster_categories['views_mean'].mean()),
                'category_count': len(cluster_categories)
            }
        
        clustering_result = {
            'category_clusters': cluster_summary,
            'category_metrics': category_metrics.to_dict('records'),
            'total_clusters': len(cluster_summary)
        }
        
        self.logger.info(f"Category clustering completed: {len(cluster_summary)} clusters identified")
        return clustering_result