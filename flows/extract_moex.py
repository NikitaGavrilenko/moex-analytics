import requests
import pandas as pd
from datetime import datetime, timedelta
import time

class MOEXDataCollector:
    """Класс для сбора данных с Московской биржи"""
    
    BASE_URL = "https://iss.moex.com/iss"
    
    def __init__(self):
        self.session = requests.Session()
    
    def get_history(self, secid, start_date, end_date):
        """Получить историю торгов для конкретной акции"""
        url = f"{self.BASE_URL}/history/engines/stock/markets/shares/boards/TQBR/securities/{secid}.json"
        
        params = {
            'from': start_date,
            'till': end_date,
            'start': 0
        }
        
        all_data = []
        
        while True:
            response = self.session.get(url, params=params)
            data = response.json()
            
            history = data['history']
            columns = history['columns']
            rows = history['data']
            
            if not rows:
                break
            
            df_chunk = pd.DataFrame(rows, columns=columns)
            all_data.append(df_chunk)
            
            if len(rows) < 100:
                break
            
            params['start'] += 100
            time.sleep(0.3)
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            result['SECID'] = secid
            return result
        
        return pd.DataFrame()
    
    def collect_multiple_securities(self, secids, start_date, end_date):
        """Собрать данные по нескольким акциям"""
        all_data = []
        
        for secid in secids:
            print(f"Загрузка данных для {secid}...")
            df = self.get_history(secid, start_date, end_date)
            
            if not df.empty:
                all_data.append(df)
            
            time.sleep(0.5)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        
        return pd.DataFrame()
    
    def get_top_securities(self, limit=30):
        """
        Получить список топовых акций по капитализации и объему торгов
        """
        url = f"{self.BASE_URL}/engines/stock/markets/shares/boards/TQBR/securities.json"
        
        response = self.session.get(url)
        data = response.json()
        
        securities = data['securities']
        columns = securities['columns']
        rows = securities['data']
        
        df = pd.DataFrame(rows, columns=columns)
                
        # Фильтруем только активные акции
        df = df[df['SECID'].notna()]
        
        # Если есть колонка с капитализацией или ценой, сортируем по ней
        if 'PREVPRICE' in df.columns:
            # Убираем строки без цены
            df = df[df['PREVPRICE'].notna() & (df['PREVPRICE'] > 0)]
            # Сортируем по цене (как прокси для популярности)
            df = df.sort_values('PREVPRICE', ascending=False)
        
        # Берем первые N акций
        result = df.head(limit)['SECID'].tolist()
        
        # Если результат пустой, возвращаем None
        return result if result else None


if __name__ == "__main__":
    collector = MOEXDataCollector()
    
    # РАСШИРЕННЫЙ СПИСОК - топ-30 акций
    print("Получение списка топ-30 акций...")
    securities = collector.get_top_securities(limit=50)
    
    # Если API не работает, используем фиксированный список
    if not securities:
        securities = [
            'SBER', 'GAZP', 'LKOH', 'GMKN', 'YNDX', 'NVTK', 'TATN', 'ROSN', 
            'MGNT', 'PLZL', 'AFLT', 'ALRS', 'CHMF', 'FEES', 'HYDR', 'IRAO',
            'MAGN', 'MTSS', 'NLMK', 'PHOR', 'RTKM', 'RUAL', 'SBERP', 'SNGS',
            'TCSG', 'VTBR', 'AFKS', 'CBOM', 'DSKY', 'MOEX'
        ]
    
    print(f"Выбрано акций: {len(securities)}")
    print(f"Список: {', '.join(securities)}")
    
    # Увеличиваем период до 1 года
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    print(f"Сбор данных с {start_date} по {end_date}...")
    df = collector.collect_multiple_securities(securities, start_date, end_date)
    
    df.to_csv('data/moex_raw_data.csv', index=False)
    print(f"Данные сохранены: {len(df)} записей по {df['SECID'].nunique()} акциям")
