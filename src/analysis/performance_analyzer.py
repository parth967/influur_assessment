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
    
    def analyze_creator_performance(self, creators_df: pd.DataFrame) -> Dict:
        '''
            -  getting top n creator
            - top creator by follower count 
            - top creator by engagement rate
            - also get category level stats (calc num of creator / avg enagagement rate / total vw)
        '''

        self.logger.info("Starting creator performance analysis")
        
        analysis = {}
        top_n = self.analysis_config.get('top_n_creators', 10)
        
        analysis['top_creators_by_followers'] = creators_df.nlargest(top_n, 'follower_count')[
            ['username', 'follower_count', 'category']
        ].to_dict('records')
        
        analysis['top_creators_by_engagement'] = creators_df.nlargest(top_n, 'avg_engagement_rate')[
            ['username', 'avg_engagement_rate', 'category']
        ].to_dict('records')
        
        category_stats = creators_df.groupby('category').agg({
            'creator_id': 'count',
            'avg_engagement_rate': 'mean',
            'total_views': 'sum'
        }).round(3)
        analysis['category_performance'] = category_stats.to_dict('index')
        
        analysis['overall_stats'] = {
            'total_creators': len(creators_df),
            'avg_engagement_rate': creators_df['avg_engagement_rate'].mean(),
            'total_views_platform': creators_df['total_views'].sum()
        }
        
        self.logger.info("Creator performance analysis completed")
        return analysis
    
    def analyze_video_performance(self, videos_df: pd.DataFrame) -> Dict:
        '''
            - top n vid by vews 
            - get vid perfomance distribution (fall in viral/super viral/ avg/ low)
            - category level vid stats
        '''
        self.logger.info("Starting video performance analysis")
        
        analysis = {}
        top_n = self.analysis_config.get('top_n_creators', 10)
        
        analysis['top_videos_by_views'] = videos_df.nlargest(top_n, 'views')[
            ['video_id', 'username', 'views', 'engagement_rate']
        ].to_dict('records')
        
        performance_dist = videos_df['video_performance'].value_counts().to_dict()
        analysis['video_performance_distribution'] = performance_dist
        
        video_category_stats = videos_df.groupby('category').agg({
            'views': 'mean',
            'engagement_rate': 'mean',
            'video_id': 'count'
        }).round(3)
        analysis['video_category_performance'] = video_category_stats.to_dict('index')
        
        analysis['overall_video_stats'] = {
            'total_videos': len(videos_df),
            'avg_views': videos_df['views'].mean(),
            'total_platform_views': videos_df['views'].sum(),
            'viral_video_count': len(videos_df[videos_df['video_performance'].isin(['viral', 'super_viral'])])
        }
        
        self.logger.info("Video performance analysis completed")
        return analysis
    
    def generate_insights(self, creators_df: pd.DataFrame, videos_df: pd.DataFrame) -> Dict:
        '''
            Q: What type of content best on the platform
             - get best performing content category (group vid by category, calc avg engagement for each category and finds highest engagement_rate)
            Q Does having more followers mean creator get better engagement
             - corr: followers vs engagement (calc relatation between follower counts & avg egagement rate)
                if < 0.3 then week else if < 0.7 then moderate else strong 
            Q Do high-performing creators post more often compared to everyone else?
                - average videos posted by high performers
                - average videos posted by all creators
            Q What kind of vid goes viral
                - identify top 3 category 
                - Calculates avg engagement rate of viral content
        '''
        start_time = time.time()
        self.logger.info("Generating insights")
        
        insights = {}
        
        best_category = videos_df.groupby('category')['engagement_rate'].mean().idxmax()
        insights['content_strategy'] = {
            'best_performing_category': best_category,
            'avg_engagement_rate': videos_df.groupby('category')['engagement_rate'].mean()[best_category]
        }
        
        correlation = creators_df['follower_count'].corr(creators_df['avg_engagement_rate'])
        insights['follower_engagement_correlation'] = {
            'correlation_coefficient': correlation,
            'interpretation': 'weak' if abs(correlation) < 0.3 else 'moderate' if abs(correlation) < 0.7 else 'strong'
        }
        
        high_performers = creators_df[creators_df['performance_tier'] == 'high']
        if len(high_performers) > 0:
            avg_videos_high_performers = high_performers['total_videos'].mean()
            avg_videos_all = creators_df['total_videos'].mean()
            
            insights['posting_frequency'] = {
                'high_performers_avg_videos': avg_videos_high_performers,
                'all_creators_avg_videos': avg_videos_all,
                'difference': avg_videos_high_performers - avg_videos_all
            }
        
        viral_videos = videos_df[videos_df['video_performance'] == 'viral']
        if len(viral_videos) > 0:
            viral_categories = viral_videos['category'].value_counts().head(3).to_dict()
            insights['viral_content_patterns'] = {
                'top_viral_categories': viral_categories,
                'avg_engagement_rate_viral': viral_videos['engagement_rate'].mean()
            }
        
        duration = time.time() - start_time
        self.pipeline_logger.log_pipeline_step("Generate insights", "SUCCESS", duration)
        
        return insights
    
    def run_full_analysis(self, data: Dict[str, pd.DataFrame]) -> Dict:
        self.logger.info("Starting full performance analysis")
        
        results = {}
        
        if 'creators' in data:
            results['creator_analysis'] = self.analyze_creator_performance(data['creators'])
        
        if 'videos' in data:
            results['video_analysis'] = self.analyze_video_performance(data['videos'])
        
        if 'creators' in data and 'videos' in data:
            results['insights'] = self.generate_insights(data['creators'], data['videos'])
            
            # Optional bonus features
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
        
        # Calculate average engagement rate across all videos
        avg_engagement_platform = videos_df['engagement_rate'].mean()
        
        # Identify trending videos (engagement rate > 1.5x platform average)
        trending_threshold = avg_engagement_platform * 1.5
        trending_videos = videos_df[videos_df['engagement_rate'] > trending_threshold].copy()
        
        # Identify trending creators (above average engagement and high virality score)
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
        
        # Identify trending categories (categories with above-average engagement)
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
        
        # Calculate creator averages
        creator_avg_views = videos_df.groupby('creator_id')['views'].mean()
        creator_avg_engagement = videos_df.groupby('creator_id')['engagement_rate'].mean()
        
        # Merge with video data
        videos_with_avg = videos_df.copy()
        videos_with_avg['creator_avg_views'] = videos_with_avg['creator_id'].map(creator_avg_views)
        videos_with_avg['creator_avg_engagement'] = videos_with_avg['creator_id'].map(creator_avg_engagement)
        
        # Detect spikes: views > 2x creator average OR engagement > 2x creator average
        spike_threshold_views = 2.0
        spike_threshold_engagement = 2.0
        
        view_spikes = videos_with_avg[
            videos_with_avg['views'] > (videos_with_avg['creator_avg_views'] * spike_threshold_views)
        ].copy()
        
        engagement_spikes = videos_with_avg[
            videos_with_avg['engagement_rate'] > (videos_with_avg['creator_avg_engagement'] * spike_threshold_engagement)
        ].copy()
        
        # Combined spikes (both views and engagement)
        combined_spikes = videos_with_avg[
            (videos_with_avg['views'] > (videos_with_avg['creator_avg_views'] * spike_threshold_views)) &
            (videos_with_avg['engagement_rate'] > (videos_with_avg['creator_avg_engagement'] * spike_threshold_engagement))
        ].copy()
        
        # Calculate spike intensity (how much above average)
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
        
        # Calculate category performance metrics
        category_metrics = videos_df.groupby('category').agg({
            'engagement_rate': ['mean', 'std'],
            'views': ['mean', 'std'],
            'likes': 'mean',
            'comments': 'mean',
            'shares': 'mean',
            'video_id': 'count'
        }).round(3)
        
        # Flatten column names
        category_metrics.columns = ['_'.join(col).strip() for col in category_metrics.columns.values]
        category_metrics = category_metrics.reset_index()
        
        # Simple clustering based on engagement rate ranges
        # Low engagement: < 0.10, Medium: 0.10-0.15, High: > 0.15
        def assign_cluster(engagement_rate):
            if engagement_rate < 0.10:
                return 'low_engagement'
            elif engagement_rate < 0.15:
                return 'medium_engagement'
            else:
                return 'high_engagement'
        
        category_metrics['engagement_cluster'] = category_metrics['engagement_rate_mean'].apply(assign_cluster)
        
        # Cluster by view volume
        avg_views = category_metrics['views_mean'].mean()
        category_metrics['volume_cluster'] = category_metrics['views_mean'].apply(
            lambda x: 'high_volume' if x > avg_views * 1.2 else 'medium_volume' if x > avg_views * 0.8 else 'low_volume'
        )
        
        # Combined cluster
        category_metrics['combined_cluster'] = (
            category_metrics['engagement_cluster'] + '_' + category_metrics['volume_cluster']
        )
        
        # Group by clusters
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