"""
Configuration loader utility
"""
import json
import os


class ConfigLoader:
    """Load and manage configuration settings"""
    
    def __init__(self, config_path="config/config.json"):
        self.config_path = config_path
        self.config = self.load_config()
    
    def load_config(self):
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def get(self, key_path, default=None):
        """Get configuration value using dot notation (e.g., 'data_sources.creators_file')"""
        keys = key_path.split('.')
        value = self.config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_data_sources(self):
        """Get data source configuration"""
        return self.config.get('data_sources', {})
    
    def get_output_config(self):
        """Get output configuration"""
        return self.config.get('output', {})
    
    def get_logging_config(self):
        """Get logging configuration"""
        return self.config.get('logging', {})
    
    def get_data_quality_config(self):
        """Get data quality configuration"""
        return self.config.get('data_quality', {})
    
    def get_analysis_config(self):
        """Get analysis configuration"""
        return self.config.get('analysis', {})