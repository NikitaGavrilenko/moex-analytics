import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np

# Настройка страницы
st.set_page_config(
    page_title="MOEX Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Кастомные стили
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

# Заголовок
st.title("📊 Аналитика торговых данных Московской биржи")
st.markdown("---")

# Загрузка данных
@st.cache_data
def load_data():
    df = pd.read_csv('C:/Users/Nik/PycharmProjects/тобд/moex_analytics/data/moex_processed_daily.csv', parse_dates=['TRADEDATE'])
    return df

try:
    df = load_data()
except:
    st.error("❌ Файл data/moex_processed_daily.csv не найден. Запустите сначала: python flows/main_flow.py")
    st.stop()

# Боковая панель с фильтрами
st.sidebar.header("🎯 Фильтры")

# Информация о датасете
st.sidebar.info(f"""
**Статистика данных:**
- Акций: {df['SECID'].nunique()}
- Записей: {len(df):,}
- Период: {df['TRADEDATE'].min().date()} — {df['TRADEDATE'].max().date()}
""")

# Фильтр: выбор акций
available_securities = sorted(df['SECID'].unique())
selected_securities = st.sidebar.multiselect(
    "Выберите акции:",
    options=available_securities,
    default=available_securities[:5]
)

if not selected_securities:
    st.warning("⚠️ Выберите хотя бы одну акцию")
    st.stop()

# Фильтр: период
col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input(
        "От:",
        value=df['TRADEDATE'].max() - pd.Timedelta(days=90),
        min_value=df['TRADEDATE'].min(),
        max_value=df['TRADEDATE'].max()
    )
with col2:
    end_date = st.date_input(
        "До:",
        value=df['TRADEDATE'].max(),
        min_value=df['TRADEDATE'].min(),
        max_value=df['TRADEDATE'].max()
    )

# Фильтр: тип сравнения
comparison_type = st.sidebar.radio(
    "Тип сравнения:",
    ["Абсолютные цены", "Нормализованные (% изменения)"]
)

# Применение фильтров
df_filtered = df[
    (df['SECID'].isin(selected_securities)) &
    (df['TRADEDATE'] >= pd.to_datetime(start_date)) &
    (df['TRADEDATE'] <= pd.to_datetime(end_date))
]

if df_filtered.empty:
    st.error("❌ Нет данных для выбранных фильтров")
    st.stop()

# === ОСНОВНАЯ ПАНЕЛЬ ===

# Метрики (KPI)
st.header("📈 Ключевые показатели")
col1, col2, col3, col4 = st.columns(4)

# Расчет метрик для выбранных акций
latest_date = df_filtered['TRADEDATE'].max()
previous_date = df_filtered[df_filtered['TRADEDATE'] < latest_date]['TRADEDATE'].max()

latest_data = df_filtered[df_filtered['TRADEDATE'] == latest_date]
previous_data = df_filtered[df_filtered['TRADEDATE'] == previous_date]

avg_return = df_filtered['DAILY_RETURN'].mean()
avg_volatility = df_filtered['VOLATILITY_7'].mean()
total_volume = latest_data['VOLUME'].sum()
num_growing = (latest_data['DAILY_RETURN'] > 0).sum()

with col1:
    st.metric(
        "Средняя доходность",
        f"{avg_return:.2f}%",
        delta=f"{avg_return:.2f}%",
        delta_color="normal"
    )

with col2:
    st.metric(
        "Средняя волатильность",
        f"{avg_volatility:.2f}%"
    )

with col3:
    st.metric(
        "Объем торгов (последний день)",
        f"{total_volume/1e6:.1f}M",
        help="В миллионах единиц"
    )

with col4:
    st.metric(
        "Растущих акций",
        f"{num_growing}/{len(selected_securities)}"
    )

st.markdown("---")

# === ГРАФИК 1: Динамика цен ===
st.header("📉 Динамика цен")

tab1, tab2 = st.tabs(["Линейный график", "Свечной график"])

with tab1:
    # Линейный график
    fig1 = go.Figure()
    
    for secid in selected_securities:
        df_sec = df_filtered[df_filtered['SECID'] == secid].sort_values('TRADEDATE')
        
        if comparison_type == "Нормализованные (% изменения)":
            # Нормализация: первый день = 100%
            base_price = df_sec['CLOSE'].iloc[0]
            y_values = (df_sec['CLOSE'] / base_price - 1) * 100
            y_label = "Изменение цены (%)"
        else:
            y_values = df_sec['CLOSE']
            y_label = "Цена закрытия (руб.)"
        
        fig1.add_trace(go.Scatter(
            x=df_sec['TRADEDATE'],
            y=y_values,
            mode='lines',
            name=secid,
            hovertemplate='<b>%{fullData.name}</b><br>' +
                          'Дата: %{x|%Y-%m-%d}<br>' +
                          f'{y_label}: %{{y:.2f}}<extra></extra>'
        ))
    
    fig1.update_layout(
        xaxis_title='Дата',
        yaxis_title=y_label,
        hovermode='x unified',
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    st.plotly_chart(fig1, use_container_width=True)

with tab2:
    # Свечной график для одной акции
    selected_for_candle = st.selectbox(
        "Выберите акцию для свечного графика:",
        selected_securities
    )
    
    df_candle = df_filtered[df_filtered['SECID'] == selected_for_candle].tail(90)
    
    fig_candle = go.Figure(data=[go.Candlestick(
        x=df_candle['TRADEDATE'],
        open=df_candle['OPEN'],
        high=df_candle['HIGH'],
        low=df_candle['LOW'],
        close=df_candle['CLOSE'],
        name=selected_for_candle
    )])
    
    # Добавляем скользящие средние
    fig_candle.add_trace(go.Scatter(
        x=df_candle['TRADEDATE'],
        y=df_candle['MA_7'],
        mode='lines',
        name='MA 7',
        line=dict(color='orange', width=1)
    ))
    
    fig_candle.add_trace(go.Scatter(
        x=df_candle['TRADEDATE'],
        y=df_candle['MA_30'],
        mode='lines',
        name='MA 30',
        line=dict(color='blue', width=1)
    ))
    
    fig_candle.update_layout(
        title=f'Свечной график: {selected_for_candle}',
        xaxis_title='Дата',
        yaxis_title='Цена (руб.)',
        height=500,
        xaxis_rangeslider_visible=False
    )
    
    st.plotly_chart(fig_candle, use_container_width=True)

st.markdown("---")

# === ГРАФИК 2: Сравнение показателей ===
st.header("📊 Сравнение показателей")

col1, col2 = st.columns(2)

with col1:
    # Волатильность
    volatility = df_filtered.groupby('SECID')['VOLATILITY_7'].mean().sort_values(ascending=False)
    
    fig2 = px.bar(
        x=volatility.index,
        y=volatility.values,
        labels={'x': 'Акция', 'y': 'Волатильность (%)'},
        title='Средняя волатильность',
        color=volatility.values,
        color_continuous_scale='Reds'
    )
    fig2.update_layout(showlegend=False, height=400)
    st.plotly_chart(fig2, use_container_width=True)

with col2:
    # Доходность
    returns = df_filtered.groupby('SECID')['DAILY_RETURN'].mean().sort_values(ascending=False)
    
    colors = ['green' if x > 0 else 'red' for x in returns.values]
    
    fig3 = go.Figure(data=[go.Bar(
        x=returns.index,
        y=returns.values,
        marker_color=colors
    )])
    
    fig3.update_layout(
        title='Средняя дневная доходность',
        xaxis_title='Акция',
        yaxis_title='Доходность (%)',
        height=400
    )
    st.plotly_chart(fig3, use_container_width=True)

st.markdown("---")

# === ГРАФИК 3: Risk-Return диаграмма ===
st.header("🎯 Risk-Return профиль")

risk_return = df_filtered.groupby('SECID').agg({
    'DAILY_RETURN': 'mean',
    'VOLATILITY_7': 'mean',
    'VOLUME': 'mean'
}).reset_index()

risk_return.columns = ['SECID', 'Return', 'Risk', 'Volume']

fig_scatter = px.scatter(
    risk_return,
    x='Risk',
    y='Return',
    size='Volume',
    color='Return',
    hover_name='SECID',
    labels={
        'Risk': 'Риск (волатильность, %)',
        'Return': 'Доходность (%)',
        'Volume': 'Объем торгов'
    },
    title='Соотношение риск-доходность',
    color_continuous_scale='RdYlGn',
    size_max=50
)

# Добавляем разделительные линии
fig_scatter.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
fig_scatter.add_vline(x=risk_return['Risk'].median(), line_dash="dash", line_color="gray", opacity=0.5)

fig_scatter.update_layout(height=500)
st.plotly_chart(fig_scatter, use_container_width=True)

st.markdown("---")

# === ГРАФИК 4: Корреляционная матрица ===
st.header("🔗 Корреляция доходностей")

if len(selected_securities) > 1:
    pivot_returns = df_filtered.pivot_table(
        index='TRADEDATE',
        columns='SECID',
        values='DAILY_RETURN'
    )
    
    correlation_matrix = pivot_returns.corr()
    
    fig_corr = go.Figure(data=go.Heatmap(
        z=correlation_matrix.values,
        x=correlation_matrix.columns,
        y=correlation_matrix.columns,
        colorscale='RdBu',
        zmid=0,
        text=correlation_matrix.values.round(2),
        texttemplate='%{text}',
        textfont={"size": 10},
        colorbar=dict(title="Корреляция")
    ))
    
    fig_corr.update_layout(
        title='Матрица корреляций дневных доходностей',
        height=600,
        xaxis={'side': 'bottom'}
    )
    
    st.plotly_chart(fig_corr, use_container_width=True)
else:
    st.info("Выберите минимум 2 акции для отображения корреляций")

st.markdown("---")

# === ГРАФИК 5: Распределение доходностей ===
st.header("📊 Распределение доходностей")

fig_dist = go.Figure()

for secid in selected_securities:
    df_sec = df_filtered[df_filtered['SECID'] == secid]
    fig_dist.add_trace(go.Histogram(
        x=df_sec['DAILY_RETURN'],
        name=secid,
        opacity=0.6,
        nbinsx=40
    ))

fig_dist.update_layout(
    title='Распределение дневных доходностей',
    xaxis_title='Доходность (%)',
    yaxis_title='Частота',
    barmode='overlay',
    height=400
)

st.plotly_chart(fig_dist, use_container_width=True)

st.markdown("---")

# === ТАБЛИЦА СО СТАТИСТИКОЙ ===
st.header("📋 Детальная статистика")

stats = df_filtered.groupby('SECID').agg({
    'CLOSE': ['min', 'max', 'mean', 'last'],
    'DAILY_RETURN': ['mean', 'std', 'min', 'max'],
    'VOLUME': 'mean',
    'VOLATILITY_7': 'mean'
}).round(2)

stats.columns = [
    'Мин. цена', 'Макс. цена', 'Средняя цена', 'Текущая цена',
    'Средняя доходность', 'СКО доходности', 'Мин. доходность', 'Макс. доходность',
    'Средний объем', 'Волатильность'
]

# Раскрашиваем таблицу
def highlight_values(val):
    if isinstance(val, (int, float)):
        if val > 0:
            return 'background-color: #90EE90'
        elif val < 0:
            return 'background-color: #FFB6C1'
    return ''

st.dataframe(
    stats.style.applymap(highlight_values, subset=['Средняя доходность', 'Мин. доходность', 'Макс. доходность']),
    use_container_width=True
)

# === ЭКСПОРТ ДАННЫХ ===
st.markdown("---")
st.header("💾 Экспорт данных")

col1, col2 = st.columns(2)

with col1:
    csv = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Скачать отфильтрованные данные (CSV)",
        data=csv,
        file_name=f'moex_filtered_{pd.Timestamp.now().strftime("%Y%m%d")}.csv',
        mime='text/csv'
    )

with col2:
    csv_stats = stats.to_csv().encode('utf-8')
    st.download_button(
        label="📥 Скачать статистику (CSV)",
        data=csv_stats,
        file_name=f'moex_stats_{pd.Timestamp.now().strftime("%Y%m%d")}.csv',
        mime='text/csv'
    )

# === СЫРЫЕ ДАННЫЕ ===
with st.expander("🔍 Посмотреть сырые данные"):
    st.dataframe(df_filtered, use_container_width=True)

# Футер
st.markdown("---")
st.caption("📊 MOEX Analytics Dashboard | Данные: Московская Биржа | Обновлено: " + 
           df['TRADEDATE'].max().strftime("%Y-%m-%d"))