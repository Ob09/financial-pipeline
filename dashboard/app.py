import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import requests
import pandas as pd

API_BASE = "https://financial-pipeline-seven.vercel.app"

app = dash.Dash(
    __name__,
    title="Financial Market Dashboard"
)

app.layout = html.Div([

    html.Div([
        html.H1("Financial Market Dashboard",
                style={"color": "#ffffff", "margin": "0", "fontSize": "24px"}),
        html.P("Real-time stock analysis powered by Apache Spark",
               style={"color": "#888888", "margin": "5px 0 0 0", "fontSize": "14px"})
    ], style={"background": "#1a1a2e", "padding": "20px 30px", "borderBottom": "1px solid #333"}),

    html.Div([

        html.Div([
            html.Label("Select Stock", style={"color": "#cccccc", "fontSize": "14px"}),
            dcc.Dropdown(
                id="stock-dropdown",
                placeholder="Select a stock...",
                style={"background": "#16213e", "color": "#000000"}
            )
        ], style={"width": "300px"}),

        html.Div(id="latest-stats", style={"display": "flex", "gap": "20px", "alignItems": "center"})

    ], style={"background": "#16213e", "padding": "15px 30px",
              "display": "flex", "alignItems": "center", "gap": "30px"}),

    html.Div([
        dcc.Graph(id="price-chart", style={"height": "400px"}),
        dcc.Graph(id="rsi-chart", style={"height": "200px"}),
        dcc.Graph(id="volatility-chart", style={"height": "200px"}),
    ], style={"background": "#0f0f23", "padding": "20px"}),

    html.Div([
        html.H3("Market Summary — Latest Data",
                style={"color": "#ffffff", "marginBottom": "15px"}),
        html.Div(id="summary-table")
    ], style={"background": "#16213e", "padding": "20px 30px"}),

    dcc.Interval(id="interval", interval=60000, n_intervals=0)

], style={"background": "#0f0f23", "minHeight": "100vh", "fontFamily": "Arial, sans-serif"})


@app.callback(
    Output("stock-dropdown", "options"),
    Input("interval", "n_intervals")
)
def update_dropdown(n):
    try:
        response = requests.get(f"{API_BASE}/api/stocks", timeout=30)
        stocks = response.json()["stocks"]
        return [{"label": s, "value": s} for s in stocks]
    except Exception:
        return []


@app.callback(
    [Output("price-chart", "figure"),
     Output("rsi-chart", "figure"),
     Output("volatility-chart", "figure"),
     Output("latest-stats", "children")],
    Input("stock-dropdown", "value")
)
def update_charts(symbol):
    empty_fig = go.Figure()
    empty_fig.update_layout(
        paper_bgcolor="#0f0f23",
        plot_bgcolor="#0f0f23",
        font={"color": "#ffffff"}
    )

    if not symbol:
        return empty_fig, empty_fig, empty_fig, []

    try:
        response = requests.get(f"{API_BASE}/api/stocks/{symbol}", timeout=30)
        data = response.json()["data"]
        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])

        price_fig = go.Figure()
        price_fig.add_trace(go.Scatter(
            x=df["date"], y=df["close"],
            name="Close Price", line={"color": "#00d4ff", "width": 2}
        ))
        price_fig.add_trace(go.Scatter(
            x=df["date"], y=df["ma_7"],
            name="MA 7", line={"color": "#ff6b6b", "width": 1, "dash": "dash"}
        ))
        price_fig.add_trace(go.Scatter(
            x=df["date"], y=df["ma_30"],
            name="MA 30", line={"color": "#ffd93d", "width": 1, "dash": "dash"}
        ))
        price_fig.update_layout(
            title=f"{symbol} — Price with Moving Averages",
            paper_bgcolor="#0f0f23", plot_bgcolor="#0f0f23",
            font={"color": "#ffffff"},
            xaxis={"gridcolor": "#333333"},
            yaxis={"gridcolor": "#333333"},
            legend={"bgcolor": "#1a1a2e"},
            margin={"t": 40, "b": 20}
        )

        rsi_fig = go.Figure()
        rsi_fig.add_trace(go.Scatter(
            x=df["date"], y=df["rsi"],
            name="RSI", line={"color": "#a29bfe", "width": 2}
        ))
        rsi_fig.add_hline(y=70, line={"color": "#ff6b6b", "dash": "dash"}, annotation_text="Overbought")
        rsi_fig.add_hline(y=30, line={"color": "#55efc4", "dash": "dash"}, annotation_text="Oversold")
        rsi_fig.update_layout(
            title="RSI (14)",
            paper_bgcolor="#0f0f23", plot_bgcolor="#0f0f23",
            font={"color": "#ffffff"},
            xaxis={"gridcolor": "#333333"},
            yaxis={"gridcolor": "#333333", "range": [0, 100]},
            margin={"t": 40, "b": 20}
        )

        vol_fig = go.Figure()
        vol_fig.add_trace(go.Scatter(
            x=df["date"], y=df["volatility_30"],
            name="Volatility", line={"color": "#fd79a8", "width": 2},
            fill="tozeroy", fillcolor="rgba(253,121,168,0.1)"
        ))
        vol_fig.update_layout(
            title="30-Day Volatility",
            paper_bgcolor="#0f0f23", plot_bgcolor="#0f0f23",
            font={"color": "#ffffff"},
            xaxis={"gridcolor": "#333333"},
            yaxis={"gridcolor": "#333333"},
            margin={"t": 40, "b": 20}
        )

        latest = df.iloc[-1]
        stats = [
            html.Div([
                html.P("Close", style={"color": "#888", "margin": "0", "fontSize": "12px"}),
                html.P(f"${latest['close']:.2f}", style={"color": "#00d4ff", "margin": "0", "fontSize": "18px", "fontWeight": "bold"})
            ]),
            html.Div([
                html.P("RSI", style={"color": "#888", "margin": "0", "fontSize": "12px"}),
                html.P(f"{latest['rsi']:.1f}" if latest['rsi'] else "N/A",
                       style={"color": "#a29bfe", "margin": "0", "fontSize": "18px", "fontWeight": "bold"})
            ]),
            html.Div([
                html.P("Daily Return", style={"color": "#888", "margin": "0", "fontSize": "12px"}),
                html.P(f"{latest['daily_return']:.2f}%" if latest['daily_return'] else "N/A",
                       style={"color": "#55efc4" if (latest['daily_return'] or 0) >= 0 else "#ff6b6b",
                              "margin": "0", "fontSize": "18px", "fontWeight": "bold"})
            ]),
        ]

        return price_fig, rsi_fig, vol_fig, stats

    except Exception as e:
        return empty_fig, empty_fig, empty_fig, []


@app.callback(
    Output("summary-table", "children"),
    Input("interval", "n_intervals")
)
def update_summary(n):
    try:
        response = requests.get(f"{API_BASE}/api/summary", timeout=30)
        data = response.json()["summary"]
        df = pd.DataFrame(data)

        rows = []
        for _, row in df.iterrows():
            rsi = row.get("rsi")
            rsi_color = "#ff6b6b" if rsi and rsi > 70 else "#55efc4" if rsi and rsi < 30 else "#ffffff"
            ret = row.get("daily_return")
            ret_color = "#55efc4" if ret and ret >= 0 else "#ff6b6b"

            rows.append(html.Tr([
                html.Td(row["symbol"], style={"color": "#00d4ff", "fontWeight": "bold", "padding": "8px 12px"}),
                html.Td(f"${row['close']:.2f}" if row.get("close") else "N/A", style={"padding": "8px 12px", "color": "#ffffff"}),
                html.Td(f"{row['ma_7']:.2f}" if row.get("ma_7") else "N/A", style={"padding": "8px 12px", "color": "#ffffff"}),
                html.Td(f"{row['ma_30']:.2f}" if row.get("ma_30") else "N/A", style={"padding": "8px 12px", "color": "#ffffff"}),
                html.Td(f"{rsi:.1f}" if rsi else "N/A", style={"padding": "8px 12px", "color": rsi_color}),
                html.Td(f"{ret:.2f}%" if ret else "N/A", style={"padding": "8px 12px", "color": ret_color}),
            ], style={"borderBottom": "1px solid #333"}))

        header = html.Tr([
            html.Th(col, style={"padding": "8px 12px", "color": "#888888", "textAlign": "left",
                               "borderBottom": "2px solid #444"})
            for col in ["Symbol", "Close", "MA 7", "MA 30", "RSI", "Daily Return"]
        ])

        return html.Table([header] + rows,
                         style={"width": "100%", "borderCollapse": "collapse", "color": "#ffffff"})
    except Exception:
        return html.P("No data available", style={"color": "#888"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)