import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
import numpy as np

class MOEXDataProcessor:
    """Класс для обработки данных с использованием Dask"""
    
    def __init__(self, input_file, use_dask_cluster=False, scheduler_address=None):
        """
        Инициализация процессора
        
        input_file: путь к CSV файлу
        use_dask_cluster: использовать ли Dask кластер
        scheduler_address: адрес Dask scheduler (например, 'localhost:8786')
        """
        self.use_dask_cluster = use_dask_cluster
        self.client = None
        
        if use_dask_cluster and scheduler_address:
            try:
                # Подключение к Dask кластеру
                self.client = Client(scheduler_address)
                print(f"✅ Подключено к Dask кластеру: {scheduler_address}")
                print(f"Dashboard: {self.client.dashboard_link}")
            except Exception as e:
                print(f"⚠️ Не удалось подключиться к Dask кластеру: {e}")
                print("Использую локальные вычисления")
                self.use_dask_cluster = False
        
        # Загрузка данных
        if self.use_dask_cluster:
            # Загружаем через Dask для параллельной обработки
            self.df = dd.read_csv(
                input_file,
                parse_dates=['TRADEDATE'],
                dtype={
                    'SECID': 'object',
                    'TRADE_SESSION_DATE': 'object'  # Добавьте эту строку
                },
                blocksize='64MB'
            )

            print(f"Данные загружены через Dask: {self.df.npartitions} партиций")
        else:
            # Загружаем через pandas
            self.df = pd.read_csv(input_file, parse_dates=['TRADEDATE'])
            print(f"Данные загружены через Pandas: {len(self.df)} строк")
    
    def clean_data(self):
        """Очистка данных"""
        print("Очистка данных...")
        
        if self.use_dask_cluster:
            # Dask версия
            self.df = self.df.dropna(subset=['CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOLUME'])
            self.df = self.df.drop_duplicates(subset=['TRADEDATE', 'SECID'])
            # Сортировка в Dask
            self.df = self.df.set_index('TRADEDATE').reset_index()
        else:
            # Pandas версия
            self.df = self.df.dropna(subset=['CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOLUME'])
            self.df = self.df.drop_duplicates(subset=['TRADEDATE', 'SECID'])
            self.df = self.df.sort_values(['SECID', 'TRADEDATE'])
        
        return self
    
    def calculate_indicators(self):
        """Расчет технических индикаторов"""
        print("Расчет индикаторов...")
        
        if self.use_dask_cluster:
            # Для Dask: преобразуем в pandas для сложных операций
            # (rolling операции в Dask сложны, поэтому делаем compute)
            print("⚠️ Преобразование в Pandas для расчета индикаторов...")
            df_pandas = self.df.compute()
            
            def calculate_for_security(group):
                group = group.sort_values('TRADEDATE')
                
                group['DAILY_RETURN'] = group['CLOSE'].pct_change() * 100
                group['MA_7'] = group['CLOSE'].rolling(window=7, min_periods=1).mean()
                group['MA_30'] = group['CLOSE'].rolling(window=30, min_periods=1).mean()
                group['VOLATILITY_7'] = group['DAILY_RETURN'].rolling(window=7, min_periods=1).std()
                group['VOLUME_CHANGE'] = group['VOLUME'].pct_change() * 100
                
                return group
            
            df_pandas = df_pandas.groupby('SECID', group_keys=False).apply(calculate_for_security)
            
            # Конвертируем обратно в Dask с большим количеством партиций
            self.df = dd.from_pandas(df_pandas, npartitions=10)
            print(f"✅ Индикаторы рассчитаны, создано {self.df.npartitions} партиций")
            
        else:
            # Pandas версия (как раньше)
            def calculate_for_security(group):
                group = group.sort_values('TRADEDATE')
                
                group['DAILY_RETURN'] = group['CLOSE'].pct_change() * 100
                group['MA_7'] = group['CLOSE'].rolling(window=7, min_periods=1).mean()
                group['MA_30'] = group['CLOSE'].rolling(window=30, min_periods=1).mean()
                group['VOLATILITY_7'] = group['DAILY_RETURN'].rolling(window=7, min_periods=1).std()
                group['VOLUME_CHANGE'] = group['VOLUME'].pct_change() * 100
                
                return group
            
            self.df = self.df.groupby('SECID', group_keys=False).apply(calculate_for_security)
        
        return self
    
    def aggregate_weekly(self):
        """Агрегация по неделям"""
        print("Агрегация по неделям...")
        
        if self.use_dask_cluster:
            # Compute для агрегации
            df_computed = self.df.compute()
        else:
            df_computed = self.df.copy()
        
        df_computed.set_index('TRADEDATE', inplace=True)
        
        weekly = df_computed.groupby('SECID').resample('W').agg({
            'OPEN': 'first',
            'HIGH': 'max',
            'LOW': 'min',
            'CLOSE': 'last',
            'VOLUME': 'sum',
            'DAILY_RETURN': 'mean',
            'VOLATILITY_7': 'mean'
        }).reset_index()
        
        if not self.use_dask_cluster:
            self.df.reset_index(inplace=True)
        
        return weekly
    
    def save_results(self, daily_output, weekly_output=None):
        """Сохранение результатов"""
        print("Сохранение результатов...")
        
        if self.use_dask_cluster:
            # Для Dask: сохраняем параллельно
            print("💾 Сохранение через Dask (параллельно)...")
            
            # Compute и сохраняем
            df_computed = self.df.compute()
            df_computed.to_csv(daily_output, index=False)
            
            print(f"✅ Дневные данные сохранены: {daily_output}")
        else:
            # Pandas версия
            self.df.to_csv(daily_output, index=False)
            print(f"✅ Дневные данные сохранены: {daily_output}")
    
    def get_statistics(self):
        """Получить статистику обработки"""
        if self.use_dask_cluster:
            stats = {
                'total_rows': len(self.df),
                'partitions': self.df.npartitions,
                'memory_usage': self.df.memory_usage(deep=True).sum().compute() / 1024**2,
                'cluster_info': str(self.client) if self.client else 'No cluster'
            }
        else:
            stats = {
                'total_rows': len(self.df),
                'partitions': 1,
                'memory_usage': self.df.memory_usage(deep=True).sum() / 1024**2,
                'cluster_info': 'Local pandas'
            }
        
        return stats
    
    def close(self):
        """Закрыть соединение с кластером"""
        if self.client:
            self.client.close()
            print("✅ Соединение с Dask кластером закрыто")


# Пример использования
if __name__ == "__main__":
    import sys
    
    # Проверяем аргументы командной строки
    use_cluster = '--cluster' in sys.argv
    scheduler = 'localhost:8786' if use_cluster else None
    
    if use_cluster:
        print("🚀 Запуск с Dask кластером")
    else:
        print("🐼 Запуск с локальным Pandas")
    
    # Создаем процессор
    processor = MOEXDataProcessor(
        'data/moex_raw_data.csv',
        use_dask_cluster=use_cluster,
        scheduler_address=scheduler
    )
    
    # Обработка
    processor.clean_data()
    processor.calculate_indicators()
    
    # Статистика
    stats = processor.get_statistics()
    print("\n📊 Статистика обработки:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Сохранение
    processor.save_results(
        daily_output='data/moex_processed_daily.csv'
    )
    
    # Агрегация
    weekly_df = processor.aggregate_weekly()
    weekly_df.to_csv('data/moex_processed_weekly.csv', index=False)
    
    print("\n✅ Обработка завершена!")
    
    # Закрываем соединение
    processor.close()