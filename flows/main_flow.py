from prefect import flow, task
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flows.extract_moex import MOEXDataCollector
from dask_jobs.transform import MOEXDataProcessor
from datetime import datetime, timedelta

@task(name="Extract MOEX Data")
def extract_task():
    """Задача сбора данных"""
    collector = MOEXDataCollector()
    
    securities = [
        'SBER', 'GAZP', 'LKOH', 'GMKN', 'YNDX', 'NVTK', 'TATN', 'ROSN', 
        'MGNT', 'PLZL', 'AFLT', 'ALRS', 'CHMF', 'FEES', 'HYDR', 'IRAO',
        'MAGN', 'MTSS', 'NLMK', 'PHOR', 'RTKM', 'RUAL', 'SBERP', 'SNGS',
        'TCSG', 'VTBR', 'AFKS', 'MOEX', 'PIKK', 'OZON'
    ]
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    df = collector.collect_multiple_securities(securities, start_date, end_date)
    df.to_csv('data/moex_raw_data.csv', index=False)
    
    return 'data/moex_raw_data.csv'

@task(name="Transform Data")
def transform_task(input_file, use_dask=False):
    """Задача обработки данных"""
    
    # Создаем процессор (с Dask или без)
    processor = MOEXDataProcessor(
        input_file,
        use_dask_cluster=use_dask,
        scheduler_address='localhost:8786' if use_dask else None
    )
    
    processor.clean_data()
    processor.calculate_indicators()
    
    # Статистика
    stats = processor.get_statistics()
    print(f"\n📊 Статистика: {stats}")
    
    processor.save_results(
        daily_output='data/moex_processed_daily.csv'
    )
    
    weekly_df = processor.aggregate_weekly()
    weekly_df.to_csv('data/moex_processed_weekly.csv', index=False)
    
    processor.close()
    
    return 'data/moex_processed_daily.csv'

@task(name="Load to Database")
def load_task(processed_file):
    """Задача загрузки в БД"""
    print(f"Данные готовы к загрузке: {processed_file}")
    return True

@flow(name="MOEX Analytics Pipeline")
def moex_pipeline(use_dask=False):
    """
    Основной ETL пайплайн
    
    use_dask: использовать ли Dask кластер для обработки
    """
    print(f"\n🚀 Запуск пайплайна (Dask: {use_dask})\n")
    
    raw_file = extract_task()
    processed_file = transform_task(raw_file, use_dask=use_dask)
    load_task(processed_file)

if __name__ == "__main__":
    # Проверяем флаг --dask
    use_dask = '--dask' in sys.argv
    
    moex_pipeline(use_dask=use_dask)