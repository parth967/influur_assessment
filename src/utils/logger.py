"""
Logging utility for the data engineering pipeline
"""
import logging
import os
from datetime import datetime


class PipelineLogger:
    """Centralized logging for the data pipeline"""
    
    def __init__(self, config):
        self.config = config
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging configuration"""
        # Create logs directory if it doesn't exist
        log_dir = os.path.dirname(self.config['logging']['file'])
        os.makedirs(log_dir, exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, self.config['logging']['level']),
            format=self.config['logging']['format'],
            handlers=[
                logging.FileHandler(self.config['logging']['file']),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("Pipeline logger initialized")
    
    def get_logger(self, name=None):
        """Get logger instance"""
        if name:
            return logging.getLogger(name)
        return self.logger
    
    def log_data_quality_issue(self, table_name, issue_type, details):
        """Log data quality issues"""
        self.logger.warning(f"Data Quality Issue - Table: {table_name}, Type: {issue_type}, Details: {details}")
    
    def log_pipeline_step(self, step_name, status, duration=None):
        """Log pipeline step completion"""
        message = f"Pipeline Step: {step_name} - Status: {status}"
        if duration:
            message += f" - Duration: {duration:.2f}s"
        self.logger.info(message)
    
    def log_metrics(self, metrics_dict):
        """Log pipeline metrics"""
        self.logger.info(f"Pipeline Metrics: {metrics_dict}")