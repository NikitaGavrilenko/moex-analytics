from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
import pandas as pd

# Загрузка данных
df = pd.read_csv('C:/Users/Nik/PycharmProjects/тобд/moex_analytics/data/moex_processed_daily.csv', parse_dates=['TRADEDATE'])

# Инициализация приложения
app = Dash(__name__)

# Макет приложения
app.layout = html.Div([
    html.H1("📊 MOEX Analytics Dashboard"),
    
    html.Div([
        html.Label("Выберите акции:"),
        dcc.Dropdown(
            id='securities-dropdown',
            options=[{'label': sec, 'value': sec} for sec in df['SECID'].unique()],
            value=list(df['SECID'].unique()[:5]),
            multi=True
        )
    ], style={'width': '50%', 'margin': '20px'}),
    
    dcc.Graph(id='price-chart'),
    
    html.Div([
        dcc.Graph(id='volatility-chart', style={'width': '48%', 'display': 'inline-block'}),
        dcc.Graph(id='returns-chart', style={'width': '48%', 'display': 'inline-block'})
    ])
])

# Callback для обновления графиков
@app.callback(
    [Output('price-chart', 'figure'),
     Output('volatility-chart', 'figure'),
     Output('returns-chart', 'figure')],
    [Input('securities-dropdown', 'value')]
)
def update_charts(selected_securities):
    df_filtered = df[df['SECID'].isin(selected_securities)]
    
    # График цен
    fig1 = go.Figure()
    for secid in selected_securities:
        df_sec = df_filtered[df_filtered['SECID'] == secid]
        fig1.add_trace(go.Scatter(
            x=df_sec['TRADEDATE'],
            y=df_sec['CLOSE'],
            mode='lines',
            name=secid
        ))
    fig1.update_layout(title='Динамика цен', xaxis_title='Дата', yaxis_title='Цена')
    
    # График волатильности
    volatility = df_filtered.groupby('SECID')['VOLATILITY_7'].mean().sort_values(ascending=False)
    fig2 = go.Figure(data=[go.Bar(x=volatility.index, y=volatility.values)])
    fig2.update_layout(title='Волатильность', xaxis_title='Акция', yaxis_title='%')
    
    # График доходности
    returns = df_filtered.groupby('SECID')['DAILY_RETURN'].mean().sort_values(ascending=False)
    fig3 = go.Figure(data=[go.Bar(x=returns.index, y=returns.values)])
    fig3.update_layout(title='Средняя доходность', xaxis_title='Акция', yaxis_title='%')
    
    return fig1, fig2, fig3

if __name__ == '__main__':
    app.run_server(debug=True)