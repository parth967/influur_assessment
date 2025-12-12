from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
import sys
import os

sys.path.append('/opt/airflow/src')

from utils.config_loader import ConfigLoader
from utils.logger import PipelineLogger
from ingestion.data_loader import DataLoader
from transformation.data_cleaner import DataCleaner
from transformation.data_enricher import DataEnricher
from analysis.performance_analyzer import PerformanceAnalyzer
from storage.database_manager import DatabaseManager
from storage.report_generator import ReportGenerator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'tiktok_data_pipeline',
    default_args=default_args,
    description='Complete TikTok data engineering pipeline',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['tiktok', 'data-engineering', 'etl']
)

def get_pipeline_components():
    config = ConfigLoader('/opt/airflow/config/config.json')
    logger = PipelineLogger(config.config)
    
    return {
        'config': config,
        'logger': logger,
        'data_loader': DataLoader(config, logger),
        'data_cleaner': DataCleaner(config, logger),
        'data_enricher': DataEnricher(config, logger),
        'analyzer': PerformanceAnalyzer(config, logger),
        'db_manager': DatabaseManager(config, logger),
        'report_generator': ReportGenerator(config, logger)
    }

def setup_directories(**context):
    import os
    
    directories = [
        '/opt/airflow/output',
        '/opt/airflow/output/reports',
        '/opt/airflow/logs'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")
    
    return "Directories setup completed"

def validate_input_files(**context):
    import os
    
    files_to_check = [
        '/opt/airflow/data/creators (1).csv',
        '/opt/airflow/data/videos (1).csv'
    ]
    
    for file_path in files_to_check:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Required input file not found: {file_path}")
        
        if os.path.getsize(file_path) == 0:
            raise ValueError(f"Input file is empty: {file_path}")
    
    print("All input files validated successfully")
    return "Input validation completed"

def ingest_data(**context):
    components = get_pipeline_components()
    logger = components['logger'].get_logger("DataIngestion")
    
    logger.info("Starting data ingestion task")
    
    data_loader = components['data_loader']
    raw_data = data_loader.load_all_data()
    
    if not raw_data:
        raise ValueError("No data was loaded successfully")
    
    data_info = {}
    for dataset_name, df in raw_data.items():
        info = data_loader.get_data_info(df, dataset_name)
        data_info[dataset_name] = {
            'rows': info['rows'],
            'columns': info['columns'],
            'memory_usage_mb': info['memory_usage_mb']
        }
        
        temp_path = f"/opt/airflow/output/temp_{dataset_name}_raw.csv"
        df.to_csv(temp_path, index=False)
        logger.info(f"Saved raw {dataset_name} data to {temp_path}")
    
    logger.info(f"Data ingestion completed: {data_info}")
    return data_info

def clean_data(**context):
    components = get_pipeline_components()
    logger = components['logger'].get_logger("DataCleaning")
    
    logger.info("Starting data cleaning task")
    
    import pandas as pd
    
    raw_data = {}
    for dataset_name in ['creators', 'videos']:
        temp_path = f"/opt/airflow/output/temp_{dataset_name}_raw.csv"
        if os.path.exists(temp_path):
            raw_data[dataset_name] = pd.read_csv(temp_path)
            logger.info(f"Loaded raw {dataset_name} data from {temp_path}")
    
    data_cleaner = components['data_cleaner']
    cleaned_data = data_cleaner.clean_all_data(raw_data)
    
    cleaning_info = {}
    for dataset_name, df in cleaned_data.items():
        temp_path = f"/opt/airflow/output/temp_{dataset_name}_cleaned.csv"
        df.to_csv(temp_path, index=False)
        
        cleaning_info[dataset_name] = {
            'rows_after_cleaning': len(df),
            'columns_after_cleaning': len(df.columns),
            'new_columns': [col for col in df.columns if col not in raw_data[dataset_name].columns]
        }
        logger.info(f"Saved cleaned {dataset_name} data to {temp_path}")
    
    logger.info(f"Data cleaning completed: {cleaning_info}")
    return cleaning_info

def enrich_data(**context):
    components = get_pipeline_components()
    logger = components['logger'].get_logger("DataEnrichment")
    
    logger.info("Starting data enrichment task")
    
    import pandas as pd
    
    cleaned_data = {}
    for dataset_name in ['creators', 'videos']:
        temp_path = f"/opt/airflow/output/temp_{dataset_name}_cleaned.csv"
        if os.path.exists(temp_path):
            cleaned_data[dataset_name] = pd.read_csv(temp_path)
            logger.info(f"Loaded cleaned {dataset_name} data from {temp_path}")
    
    data_enricher = components['data_enricher']
    enriched_data = data_enricher.enrich_all_data(cleaned_data)
    
    enrichment_info = {}
    for dataset_name, df in enriched_data.items():
        temp_path = f"/opt/airflow/output/temp_{dataset_name}_enriched.csv"
        df.to_csv(temp_path, index=False)
        
        # Handle creators_final which doesn't exist in cleaned_data
        base_dataset = dataset_name.replace('_final', '')
        if base_dataset in cleaned_data:
            enriched_columns = [col for col in df.columns if col not in cleaned_data[base_dataset].columns]
        else:
            enriched_columns = list(df.columns)
        
        enrichment_info[dataset_name] = {
            'rows_after_enrichment': len(df),
            'columns_after_enrichment': len(df.columns),
            'enriched_columns': enriched_columns
        }
        logger.info(f"Saved enriched {dataset_name} data to {temp_path}")
    
    logger.info(f"Data enrichment completed: {enrichment_info}")
    return enrichment_info

def analyze_performance(**context):
    components = get_pipeline_components()
    logger = components['logger'].get_logger("PerformanceAnalysis")
    
    logger.info("Starting performance analysis task")
    
    import pandas as pd
    
    enriched_data = {}
    for dataset_name in ['creators', 'videos']:
        temp_path = f"/opt/airflow/output/temp_{dataset_name}_enriched.csv"
        if os.path.exists(temp_path):
            enriched_data[dataset_name] = pd.read_csv(temp_path)
            logger.info(f"Loaded enriched {dataset_name} data from {temp_path}")
    
    analyzer = components['analyzer']
    analysis_results = analyzer.run_full_analysis(enriched_data)
    
    # Save analysis results
    import json
    analysis_path = "/opt/airflow/output/temp_analysis_results.json"
    with open(analysis_path, 'w') as f:
        json.dump(analysis_results, f, indent=2, default=str)
    
    logger.info(f"Saved analysis results to {analysis_path}")
    
    # Extract key metrics for XCom
    analysis_summary = {
        'total_creators_analyzed': len(enriched_data.get('creators', [])),
        'total_videos_analyzed': len(enriched_data.get('videos', [])),
        'analysis_categories': list(analysis_results.keys())
    }
    
    logger.info(f"Performance analysis completed: {analysis_summary}")
    return analysis_summary

def store_to_database(**context):
    components = get_pipeline_components()
    logger = components['logger'].get_logger("DatabaseStorage")
    
    logger.info("Starting database storage task")
    
    import pandas as pd
    
    enriched_data = {}
    for dataset_name in ['creators', 'videos', 'creators_final']:
        temp_path = f"/opt/airflow/output/temp_{dataset_name}_enriched.csv"
        if os.path.exists(temp_path):
            enriched_data[dataset_name] = pd.read_csv(temp_path)
            logger.info(f"Loaded enriched {dataset_name} data from {temp_path}")
    
    db_manager = components['db_manager']
    db_manager.save_all_data(enriched_data)
    
    table_info = db_manager.get_table_info()
    
    logger.info(f"Database storage completed: {table_info}")
    return table_info

def generate_reports(**context):
    components = get_pipeline_components()
    logger = components['logger'].get_logger("ReportGeneration")
    
    logger.info("Starting report generation task")
    
    import pandas as pd
    import json
    
    enriched_data = {}
    for dataset_name in ['creators', 'videos', 'creators_final']:
        temp_path = f"/opt/airflow/output/temp_{dataset_name}_enriched.csv"
        if os.path.exists(temp_path):
            enriched_data[dataset_name] = pd.read_csv(temp_path)
            logger.info(f"Loaded enriched {dataset_name} data from {temp_path}")
    
    analysis_path = "/opt/airflow/output/temp_analysis_results.json"
    with open(analysis_path, 'r') as f:
        analysis_results = json.load(f)
    
    report_generator = components['report_generator']
    report_generator.generate_all_reports(analysis_results, enriched_data)
    
    reports_dir = "/opt/airflow/output/reports"
    generated_reports = []
    if os.path.exists(reports_dir):
        generated_reports = [f for f in os.listdir(reports_dir) if f.endswith(('.json', '.csv'))]
    
    logger.info(f"Report generation completed. Generated reports: {generated_reports}")
    return generated_reports

def cleanup_temp_files(**context):
    import os
    import glob
    
    temp_files = glob.glob("/opt/airflow/output/temp_*.csv") + glob.glob("/opt/airflow/output/temp_*.json")
    
    cleaned_files = []
    for temp_file in temp_files:
        try:
            os.remove(temp_file)
            cleaned_files.append(temp_file)
        except Exception as e:
            print(f"Error removing {temp_file}: {e}")
    
    print(f"Cleaned up {len(cleaned_files)} temporary files")
    return cleaned_files

def send_completion_notification(**context):
    ti = context['ti']
    
    ingestion_info = ti.xcom_pull(task_ids='ingest_data')
    cleaning_info = ti.xcom_pull(task_ids='clean_data')
    enrichment_info = ti.xcom_pull(task_ids='enrich_data')
    analysis_info = ti.xcom_pull(task_ids='analyze_performance')
    storage_info = ti.xcom_pull(task_ids='store_to_database')
    reports_info = ti.xcom_pull(task_ids='generate_reports')
    
    pipeline_summary = {
        'execution_date': context['execution_date'].isoformat(),
        'dag_run_id': context['dag_run'].run_id,
        'ingestion': ingestion_info,
        'cleaning': cleaning_info,
        'enrichment': enrichment_info,
        'analysis': analysis_info,
        'storage': storage_info,
        'reports': reports_info,
        'status': 'SUCCESS'
    }
    
    summary_path = "/opt/airflow/output/pipeline_execution_summary.json"
    import json
    with open(summary_path, 'w') as f:
        json.dump(pipeline_summary, f, indent=2, default=str)
    
    print("Pipeline execution completed successfully!")
    print(f"Summary saved to: {summary_path}")
    
    return pipeline_summary

start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

setup_task = PythonOperator(
    task_id='setup_directories',
    python_callable=setup_directories,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_input_files',
    python_callable=validate_input_files,
    dag=dag
)

creators_sensor = FileSensor(
    task_id='wait_for_creators_file',
    filepath='/opt/airflow/data/creators (1).csv',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag
)

videos_sensor = FileSensor(
    task_id='wait_for_videos_file',
    filepath='/opt/airflow/data/videos (1).csv',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag
)

ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag
)

enrich_task = PythonOperator(
    task_id='enrich_data',
    python_callable=enrich_data,
    dag=dag
)

analyze_task = PythonOperator(
    task_id='analyze_performance',
    python_callable=analyze_performance,
    dag=dag
)

store_task = PythonOperator(
    task_id='store_to_database',
    python_callable=store_to_database,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    dag=dag,
    trigger_rule='all_done'  # Run even if some tasks fail
)

notification_task = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_completion_notification,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
    trigger_rule='all_done'
)

start_task >> setup_task >> validate_task

validate_task >> [creators_sensor, videos_sensor]

[creators_sensor, videos_sensor] >> ingest_task >> clean_task >> enrich_task

enrich_task >> [analyze_task, store_task]

[analyze_task, store_task] >> report_task >> notification_task

[notification_task, report_task] >> cleanup_task >> end_task