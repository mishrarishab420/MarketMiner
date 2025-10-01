# dashboard.py
import base64
import io
import json
import math
import dash
from dash import dcc, html, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from scipy import stats

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX], suppress_callback_exceptions=True)
server = app.server
app.title = "Insightify — Professional Data Analysis Dashboard"

# CONSTANT (fixed) columns we always analyze
FIXED = ['title', 'mrp', 'current_price', 'discount', 'rating', 'review']
# We'll accept case-insensitive column names by normalizing incoming df columns.

# ---------- Helper functions ----------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Strip, lower column names and map to nice names where possible
    mapping = {}
    cols = df.columns.tolist()
    for c in cols:
        c_norm = c.strip()
        mapping[c] = c_norm
    df = df.rename(columns=mapping)
    # lower-case keys for internal detection
    df.columns = [c.lower().strip() for c in df.columns]
    return df

def coerce_and_prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # remove url if present
    if 'url' in df.columns:
        df.drop(columns=['url'], inplace=True)
    # Ensure FIXED cols exist
    for c in FIXED:
        if c not in df.columns:
            df[c] = np.nan

    # numeric conversions
    for col in ['mrp','current_price','discount','rating','review']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # rows with missing title are not helpful
    if 'title' in df.columns:
        df = df.dropna(subset=['title'])

    # basic fixes:
    if 'mrp' in df.columns and 'current_price' in df.columns:
        mask = df['mrp'].isna() & df['current_price'].notna()
        df.loc[mask, 'mrp'] = df.loc[mask, 'current_price']
    if 'discount' in df.columns:
        df['discount'] = df['discount'].fillna(0)
        df.loc[df['discount'] < 0, 'discount'] = 0

    return df

def compute_overview(df: pd.DataFrame) -> dict:
    overview = {}
    overview['total_products'] = int(len(df))
    overview['avg_price'] = round(float(df['current_price'].dropna().mean()), 2) if not df['current_price'].dropna().empty else None
    overview['median_price'] = round(float(df['current_price'].dropna().median()), 2) if not df['current_price'].dropna().empty else None
    overview['avg_rating'] = round(float(df['rating'].dropna().mean()), 2) if 'rating' in df.columns and not df['rating'].dropna().empty else None
    overview['missing_values_pct'] = round(df.isna().mean().mean() * 100, 2)
    overview['num_numeric'] = int(len(df.select_dtypes(include='number').columns))
    overview['num_categorical'] = int(len(df.select_dtypes(exclude='number').columns))
    return overview

def get_dynamic_filter_columns(df: pd.DataFrame):
    # All columns except FIXED are treated as filters (except any url)
    return [c for c in df.columns if c not in FIXED]

def detect_anomalies_iqr(series: pd.Series):
    # return boolean mask of anomalies using IQR
    if series.dropna().empty:
        return pd.Series([False]*len(series), index=series.index)
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return (series < lower) | (series > upper)

def summarize_numeric(df: pd.DataFrame, numeric_cols):
    rows = []
    for c in numeric_cols:
        s = df[c].dropna()
        rows.append({
            'column': c,
            'count': int(s.count()),
            'mean': float(s.mean()) if not s.empty else None,
            'median': float(s.median()) if not s.empty else None,
            'std': float(s.std()) if not s.empty else None,
            'min': float(s.min()) if not s.empty else None,
            'max': float(s.max()) if not s.empty else None,
            'missing': int(df[c].isna().sum())
        })
    return rows

# ---------- Layout ----------
app.layout = dbc.Container([
    # Navbar
    dbc.Navbar(
        dbc.Container([
            dbc.NavbarBrand("Insightify — Professional Data Analysis Dashboard", className="ms-2"),
        ]),
        color="primary", dark=True, sticky="top", className="mb-4"
    ),

    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H5("Upload Data", className="card-title"),
                    html.P("Upload CSV or Excel (xls/xlsx). Dashboard is generated automatically on upload.",
                           className="card-text text-muted"),
                    dcc.Upload(
                        id='upload-data',
                        children=html.Div(['Drag and Drop or ', html.A('Select CSV / Excel File')]),
                        style={
                            'width': '100%', 'height': '60px', 'lineHeight': '60px',
                            'borderWidth': '1px', 'borderStyle': 'dashed',
                            'borderRadius': '6px', 'textAlign': 'center', 'margin': '10px 0'
                        },
                        multiple=False
                    ),
                    html.Div(id='file-name', style={'padding':'6px 0'})
                ])
            ], className="mb-4"),
            width=12
        )
    ]),

    # Store
    dcc.Store(id='stored-data', storage_type='session'),

    # Controls and filters
    dbc.Card([
        dbc.CardBody([
            html.H5("Filters", className="card-title"),
            html.Div(id='controls-area', children=[], style={'display':'flex','gap':'20px','flexWrap':'wrap','padding':'5px 0'})
        ])
    ], className="mb-4"),

    # KPIs
    dbc.Card([
        dbc.CardBody([
            dbc.Row(id='kpi-area', className="gy-3", style={'gap':'16px'})
        ])
    ], className="mb-4"),

    # Graphs grid
    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H5("Price Distribution", className="card-title"),
                    dcc.Graph(id='price-dist')
                ])
            ]), md=6, className="mb-4"
        ),
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H5("Rating Distribution", className="card-title"),
                    dcc.Graph(id='rating-dist')
                ])
            ]), md=6, className="mb-4"
        ),
    ], className="g-4"),
    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H5("Price vs Rating (scatter)", className="card-title"),
                    dcc.Graph(id='price-vs-rating')
                ])
            ]), md=6, className="mb-4"
        ),
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H5("Correlation Heatmap (numeric)", className="card-title"),
                    dcc.Graph(id='corr-heatmap')
                ])
            ]), md=6, className="mb-4"
        ),
    ], className="g-4"),

    # Missing data heatmap & numeric summary
    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H5("Missing Data Heatmap", className="card-title"),
                    dcc.Graph(id='missing-heatmap')
                ])
            ]), md=6, className="mb-4"
        ),
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H5("Numeric Summary", className="card-title"),
                    dash_table.DataTable(id='numeric-summary',
                                         columns=[{'name':k,'id':k} for k in ['column','count','mean','median','std','min','max','missing']],
                                         page_size=10,
                                         style_table={'overflowX':'auto'})
                ])
            ]), md=6, className="mb-4"
        ),
    ], className="g-4"),

    # Top lists and anomaly detection
    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H5("Top Discounted Products", className="card-title"),
                    dash_table.DataTable(id='top-discounted', page_size=10,
                                         columns=[{'name':i,'id':i} for i in ['title','mrp','current_price','discount']])
                ])
            ]), md=6, className="mb-4"
        ),
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H5("Top Rated Products", className="card-title"),
                    dash_table.DataTable(id='top-rated', page_size=10,
                                         columns=[{'name':i,'id':i} for i in ['title','rating','review']])
                ])
            ]), md=6, className="mb-4"
        ),
    ], className="g-4"),

    # Suggestions and export
    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H5("Automated Suggestions", className="card-title"),
                    html.Ul(id='suggestions-list')
                ])
            ]), md=6, className="mb-4"
        ),
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H5("Export / Download", className="card-title"),
                    dbc.Button("Download Cleaned CSV", id='download-cleaned', color="primary", className="me-2"),
                    dcc.Download(id='download-cleaned-file'),
                    dbc.Button("Download Filtered CSV", id='download-filtered', color="secondary", className="me-2"),
                    dcc.Download(id='download-filtered-file')
                ])
            ]), md=6, className="mb-4"
        ),
    ], className="g-4"),

    # Raw data table with search/paging
    dbc.Card([
        dbc.CardBody([
            html.H5("Data Preview (filtered)", className="card-title"),
            dash_table.DataTable(
                id='data-preview',
                page_size=15,
                filter_action='native',
                sort_action='native',
                column_selectable='single',
                row_selectable='multi',
                style_table={'overflowX':'auto'}
            )
        ])
    ], className="mb-4"),

    # Hidden area for debug / JSON previews if needed
    html.Div(id='debug', style={'display':'none'})
], fluid=True, style={"paddingBottom": "2rem"})

# ---------- Callbacks ----------

@app.callback(
    Output('stored-data', 'data'),
    Output('file-name', 'children'),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename')
)
def handle_upload(contents, filename):
    if contents is None:
        return dash.no_update, ""
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        # try as excel first if filename suggests it
        if filename and filename.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(io.BytesIO(decoded))
        else:
            # csv fallback (handles various encodings safely)
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')), low_memory=False)
    except Exception as e:
        # second attempt with latin-1
        try:
            df = pd.read_csv(io.StringIO(decoded.decode('latin-1')), low_memory=False)
        except Exception as e2:
            return dash.no_update, f"Error reading file: {e} / {e2}"

    df = normalize_columns(df)
    df = coerce_and_prepare(df)

    # store minimal JSON (orient records) + meta
    data_json = df.to_json(date_format='iso', orient='split')
    return data_json, html.Div([html.B("Uploaded: "), filename])

@app.callback(
    Output('controls-area', 'children'),
    Output('kpi-area', 'children'),
    Input('stored-data', 'data')
)
def build_controls_and_kpis(data_json):
    if not data_json:
        return [], []
    df = pd.read_json(data_json, orient='split')
    # dynamic filters: unique values for non-fixed columns (limit to 100 options)
    filter_cols = get_dynamic_filter_columns(df)
    controls = []
    for c in filter_cols:
        vals = df[c].dropna().unique()
        # For large unique cardinality, provide a text search input instead of multi-select
        if len(vals) <= 100:
            controls.append(
                html.Div([
                    html.Label(c),
                    dcc.Dropdown(
                        id={'type':'dyn-filter','col':c},
                        options=[{'label':str(v),'value':v} for v in sorted(vals, key=lambda x: str(x))],
                        multi=True,
                        placeholder=f"Filter by {c}"
                    )
                ], style={'minWidth':'200px','maxWidth':'300px'})
            )
        else:
            controls.append(
                html.Div([
                    html.Label(c),
                    dcc.Input(id={'type':'dyn-filter-text','col':c}, placeholder=f"search {c}", type='text')
                ], style={'minWidth':'200px','maxWidth':'300px'})
            )

    # KPIs
    overview = compute_overview(df)
    kpis = []
    kpis.append(
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H6("Total Products", className="card-subtitle mb-2 text-muted"),
                    html.H3(f"{overview['total_products']}", className="card-title"),
                ])
            ], color="light", inverse=False, className="h-100"),
            width="auto"
        )
    )
    kpis.append(
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H6("Avg Price", className="card-subtitle mb-2 text-muted"),
                    html.H3(f"{overview['avg_price'] if overview['avg_price'] is not None else 'N/A'}", className="card-title"),
                ])
            ], color="light", inverse=False, className="h-100"),
            width="auto"
        )
    )
    kpis.append(
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H6("Median Price", className="card-subtitle mb-2 text-muted"),
                    html.H3(f"{overview['median_price'] if overview['median_price'] is not None else 'N/A'}", className="card-title"),
                ])
            ], color="light", inverse=False, className="h-100"),
            width="auto"
        )
    )
    kpis.append(
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H6("Avg Rating", className="card-subtitle mb-2 text-muted"),
                    html.H3(f"{overview['avg_rating'] if overview['avg_rating'] is not None else 'N/A'}", className="card-title"),
                ])
            ], color="light", inverse=False, className="h-100"),
            width="auto"
        )
    )
    kpis.append(
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.H6("Missing % (all cols)", className="card-subtitle mb-2 text-muted"),
                    html.H3(f"{overview['missing_values_pct']}%", className="card-title"),
                ])
            ], color="light", inverse=False, className="h-100"),
            width="auto"
        )
    )
    return controls, kpis

def apply_filters(df: pd.DataFrame, filter_values: dict):
    # filter_values keys are column names; values are either list (multiselect) or text search
    df2 = df.copy()
    for col, val in filter_values.items():
        if val is None or val == [] or (isinstance(val, str) and val.strip() == ""):
            continue
        if isinstance(val, list):
            # allow matching any of the selected items
            df2 = df2[df2[col].isin(val)]
        else:
            # substring case-insensitive match
            mask = df2[col].astype(str).str.contains(str(val), case=False, na=False)
            df2 = df2[mask]
    return df2

@app.callback(
    Output('price-dist', 'figure'),
    Output('rating-dist', 'figure'),
    Output('price-vs-rating', 'figure'),
    Output('corr-heatmap', 'figure'),
    Output('missing-heatmap', 'figure'),
    Output('numeric-summary', 'data'),
    Output('top-discounted', 'data'),
    Output('top-rated', 'data'),
    Output('suggestions-list', 'children'),
    Output('data-preview', 'columns'),
    Output('data-preview', 'data'),
    Input('stored-data', 'data'),
    Input({'type':'dyn-filter','col':dash.ALL}, 'value'),
    Input({'type':'dyn-filter-text','col':dash.ALL}, 'value'),
    State({'type':'dyn-filter','col':dash.ALL}, 'id'),
    State({'type':'dyn-filter-text','col':dash.ALL}, 'id'),
)
def refresh_visuals(data_json, multi_vals, text_vals, multi_ids, text_ids):
    if not data_json:
        empty_fig = go.Figure()
        return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, [], [], [], [], [], []

    df = pd.read_json(data_json, orient='split')

    # Build filter dict: map ids to values
    filter_values = {}
    if multi_ids and multi_vals:
        for ident, val in zip(multi_ids, multi_vals):
            filter_values[ident['col']] = val
    if text_ids and text_vals:
        for ident, val in zip(text_ids, text_vals):
            filter_values[ident['col']] = val

    df_filtered = apply_filters(df, filter_values)

    # Price distribution
    fig_price = px.histogram(df_filtered, x='current_price', nbins=30, title="Price Distribution", marginal="rug")
    fig_price.update_layout(height=350)

    # Rating distribution
    fig_rating = px.histogram(df_filtered, x='rating', nbins=20, title="Rating Distribution")
    fig_rating.update_layout(height=350)

    # Price vs rating scatter
    fig_scatter = px.scatter(df_filtered, x='current_price', y='rating', hover_data=['title'], title='Price vs Rating (size by review)')
    if 'review' in df_filtered.columns:
        fig_scatter.update_traces(marker=dict(size=np.clip((df_filtered['review'].fillna(0).astype(float) / (df_filtered['review'].max() if df_filtered['review'].max()!=0 else 1))*12, 6, 30)))
    fig_scatter.update_layout(height=350)

    # Correlation heatmap
    num_df = df_filtered.select_dtypes(include='number')
    if not num_df.empty and num_df.shape[1] >= 2:
        corr = num_df.corr()
        heatmap = go.Figure(data=go.Heatmap(z=corr.values, x=corr.columns, y=corr.columns, colorscale='Viridis'))
        heatmap.update_layout(title='Numeric Correlation', height=350)
    else:
        heatmap = go.Figure()

    # Missing data heatmap (binary)
    miss = df_filtered.isna().astype(int)
    if not miss.empty:
        miss_fig = go.Figure(data=go.Heatmap(z=miss.T.values, x=miss.index, y=miss.columns, colorscale=[[0, 'white'], [1, 'red']], showscale=False))
        miss_fig.update_layout(title='Missing Data Heatmap (red = missing)', height=250)
    else:
        miss_fig = go.Figure()

    # Numeric summary table
    numeric_cols = num_df.columns.tolist()
    numeric_summary_rows = summarize_numeric(df_filtered, numeric_cols)

    # Top discounted / Top rated
    top_disc = df_filtered.sort_values(by='discount', ascending=False).head(50)[['title','mrp','current_price','discount']].to_dict('records') if 'discount' in df_filtered.columns else []
    top_rated = df_filtered.sort_values(by='rating', ascending=False).head(50)[['title','rating','review']].to_dict('records') if 'rating' in df_filtered.columns else []

    # Suggestions (rules-based)
    suggestions = []
    if 'discount' in df_filtered.columns and not df_filtered['discount'].dropna().empty:
        if df_filtered['discount'].mean() < 5:
            suggestions.append(html.Li("Average discount is low (<5%). Consider promotions or price checks."))
    if 'rating' in df_filtered.columns and not df_filtered['rating'].dropna().empty:
        if df_filtered['rating'].mean() < 3.8:
            suggestions.append(html.Li("Average rating below 3.8: investigate listings or product quality."))
    # Anomaly detection example for current_price
    if 'current_price' in df_filtered.columns and not df_filtered['current_price'].dropna().empty:
        anomalies_mask = detect_anomalies_iqr(df_filtered['current_price'])
        n_anom = int(anomalies_mask.sum())
        if n_anom > 0:
            suggestions.append(html.Li(f"Found {n_anom} price anomalies (IQR). Inspect these rows in the table."))
    if not suggestions:
        suggestions = [html.Li("No immediate issues detected.")]

    # Data preview columns & data (limit to 200 rows returned)
    preview_df = df_filtered.head(200)
    columns = [{'name': c, 'id': c} for c in preview_df.columns]
    data_records = preview_df.to_dict('records')

    return fig_price, fig_rating, fig_scatter, heatmap, miss_fig, numeric_summary_rows, top_disc, top_rated, suggestions, columns, data_records

# Downloads: cleaned (original cleaned) and filtered
@app.callback(
    Output('download-cleaned-file', 'data'),
    Input('download-cleaned', 'n_clicks'),
    State('stored-data', 'data'),
    prevent_initial_call=True
)
def download_cleaned(n, data_json):
    if not data_json:
        return dash.no_update
    df = pd.read_json(data_json, orient='split')
    return dcc.send_data_frame(df.to_csv, "insightify_cleaned.csv", index=False)

@app.callback(
    Output('download-filtered-file', 'data'),
    Input('download-filtered', 'n_clicks'),
    State('data-preview', 'data'),
    State('data-preview', 'columns'),
    prevent_initial_call=True
)
def download_filtered(n, preview_data, preview_cols):
    if not preview_data:
        return dash.no_update
    df = pd.DataFrame(preview_data)
    return dcc.send_data_frame(df.to_csv, "insightify_filtered_preview.csv", index=False)

# Run
if __name__ == "__main__":
    app.run_server(debug=False)