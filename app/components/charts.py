import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def plot_nav_history(nav_df, scheme_name):
    """Plot NAV history over time."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=nav_df.index, y=nav_df["nav"], mode="lines", name="NAV", line=dict(color="#1f77b4", width=2)))

    fig.update_layout(title=f"NAV History: {scheme_name}", xaxis_title="Date", yaxis_title="NAV", template="plotly_white", hovermode="x unified", height=400, margin=dict(l=20, r=20, t=40, b=20))
    return fig


def plot_rolling_returns(rolling_df, window_years):
    """Plot rolling returns heatmap or line chart."""
    fig = px.line(rolling_df, x=rolling_df.index, y=rolling_df.values, title=f"{window_years}Y Rolling Returns", labels={"y": "Annualized Return", "index": "Date"})

    fig.add_hline(y=0, line_dash="dash", line_color="red")
    fig.update_layout(template="plotly_white", height=400)
    # Format yaxis as percentage
    fig.update_yaxes(tickformat=".1%")
    return fig


def plot_drawdown(drawdown_df):
    """Plot drawdown chart."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=drawdown_df.index, y=drawdown_df, fill="tozeroy", name="Drawdown", line=dict(color="red")))

    fig.update_layout(title="Drawdown History", xaxis_title="Date", yaxis_title="Drawdown (%)", template="plotly_white", height=400)
    fig.update_yaxes(tickformat=".1%")
    return fig


def plot_returns_distribution(nav_df):
    """Plot distribution of daily returns."""
    returns = nav_df["nav"].pct_change().dropna()
    fig = px.histogram(returns, nbins=50, title="Daily Returns Distribution", labels={"value": "Daily Return"}, color_discrete_sequence=["#2ca02c"])
    fig.update_layout(template="plotly_white", showlegend=False, height=400)
    fig.update_xaxes(tickformat=".1%")
    return fig


def plot_benchmark_comparison(fund_nav, bench_nav, fund_name, bench_name):
    """Plot rebased comparison of fund vs benchmark."""
    # Ensure benchmark_nav is a 1D Series
    if hasattr(bench_nav, "squeeze"):
        bench_nav = bench_nav.squeeze()

    # Align and rebase to 100
    df = pd.DataFrame({"fund": fund_nav, "bench": bench_nav}).dropna()
    if df.empty:
        return go.Figure()

    df = (df / df.iloc[0]) * 100

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df["fund"], name=fund_name, line=dict(width=2)))
    fig.add_trace(go.Scatter(x=df.index, y=df["bench"], name=bench_name, line=dict(width=2, dash="dash")))

    fig.update_layout(title=f"{fund_name} vs {bench_name} (Rebased to 100)", xaxis_title="Date", yaxis_title="Normalized Value", template="plotly_white", height=450)
    return fig


def plot_capture_ratios(capture_dict):
    """Plot bar chart for upside/downside capture."""
    categories = ["Upside Capture", "Downside Capture"]
    values = [capture_dict["upside"], capture_dict["downside"]]

    fig = go.Figure(data=[go.Bar(x=categories, y=values, marker_color=["#2ca02c", "#d62728"])])

    fig.add_hline(y=100, line_dash="dash", line_color="black")

    fig.update_layout(title="Market Capture Ratios (%)", yaxis_title="Ratio (%)", template="plotly_white", height=400, yaxis=dict(ticksuffix="%"))
    return fig
