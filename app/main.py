import os
import re
import sys
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import streamlit as st

# Professional-Grade: Robust Path Resolution for Local & Cloud Environments
root_dir = os.path.dirname(os.path.abspath(__file__))
if root_dir not in sys.path:
    sys.path.append(root_dir)
if os.path.dirname(root_dir) not in sys.path:
    sys.path.append(os.path.dirname(root_dir))

from components.charts import (  # noqa: E402
    plot_benchmark_comparison,
    plot_calendar_returns,
    plot_capture_ratios,
    plot_drawdown,
    plot_market_sensitivity,
    plot_nav_history,
    plot_periodic_metrics,
    plot_stress_scenarios,
)
from core.analytics import MFAnalytics  # noqa: E402
from core.data_fetcher import MFDataFetcher  # noqa: E402
from core.logger import get_logger, log_event  # noqa: E402

# Initialize professional logger
logger = get_logger(__name__)

# Page Configuration
st.set_page_config(page_title="ConvexLab | Portfolio Intelligence", page_icon="🔬", layout="wide")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* Professional Typography Override - Replaces Global Force to fix Icon breaking */
    [data-testid="stHeader"], [data-testid="stSidebar"], .stMarkdown, .stMetric, .stSelectbox, .stTextInput, .stRadio, .stTab, .st-at, .st-ae {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
    }

    /* Ensure Icon Fonts are never touched */
    [data-testid="stIcon"], i, svg {
        font-family: inherit !important;
    }

    h1 {
        font-size: 1.6rem !important;
        margin-bottom: 0.5rem !important;
    }
    h2 {
        font-size: 1.3rem !important;
        margin-top: 1.5rem !important;
        margin-bottom: 0.8rem !important;
    }
    h3, h4 {
        font-size: 1.1rem !important;
        margin-top: 0.4rem !important;
        margin-bottom: 0.3rem !important;
        font-weight: 700 !important;
    }

    /* Hide the automatic Streamlit/Markdown anchor links (the 'chain' icons) */
    h1 a, h2 a, h3 a, h4 a {
        display: none !important;
    }

    .main { background-color: transparent; }
    .stMetric {
        background-color: #1e293b;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        border: 1px solid rgba(255,255,255,0.05);
    }
    .metric-card {
        background-color: #1e293b;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        border: 1px solid rgba(255,255,255,0.05);
        margin-bottom: 20px;
    }
    [data-testid="stSidebar"] {
        background-color: #1e293b;
        border-right: 1px solid rgba(212, 175, 55, 0.3);
        box-shadow: 4px 0 15px rgba(0,0,0,0.4);
    }
    /* Specific overrides for sidebar collapse/expand arrows & icons */
    [data-testid="stSidebarCollapseButton"] svg, [data-testid="stSidebarNav"] svg, section[data-testid="stSidebar"] button svg {
        fill: #d4af37 !important;
        color: #d4af37 !important;
    }
    [data-testid="stSidebarCollapseButton"] button:hover svg {
        fill: #e2e8f0 !important;
        color: #e2e8f0 !important;
    }
    /* Restored Safe Top Margin (To prevent cutoff) */
    .stAppViewBlockContainer, .stMainBlockContainer, [data-testid="stAppViewBlockContainer"] {
        padding-top: 3.5rem !important;
        padding-bottom: 2rem !important;
    }

    /* Give some breathing room for headers */
    h1, h2, h3 {
        margin-top: 1rem !important;
    }

    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
        padding-top: 0.2rem !important;
        gap: 0.8rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource(show_spinner="Initializing Analytics Engine...")
def get_analytics_toolkit() -> Tuple[MFDataFetcher, MFAnalytics]:
    """Force-initialize the analytical suite - Cache-breaker v2.2.0."""
    return MFDataFetcher(), MFAnalytics()


fetcher, analytics = get_analytics_toolkit()
Riverside_Cache_Breaker = "2.3.0"

# Sidebar - Search and Selection
with st.sidebar:
    # Professional Identity: 🔬 ConvexLab v1.0.0
    st.markdown("<h1 style='margin-top: 15px; font-size: 1.6rem; font-weight: 700; margin-bottom: 0px;'>🔬 ConvexLab</h1>", unsafe_allow_html=True)
    st.markdown("<p style='font-size: 0.85rem; color: #94a3b8; font-weight: 500; margin-top: -5px; margin-bottom: 5px;'>Portfolio Intelligence and Analytics</p>", unsafe_allow_html=True)

    # High-Density Headers: Harmonized Spacing
    st.markdown(
        "<p style='border-top: 1px solid rgba(255,255,255,0.1); margin-top: 25px; padding-top: 10px; font-weight: 700; font-size: 1.1rem;'>🎯 Asset Selection</p>",
        unsafe_allow_html=True,
    )

    # Fund Discovery
    st.markdown("**1. Primary Mutual Fund**")
    search_query = st.text_input("Fund Name", placeholder="Search Fund (e.g. HDFC Flexi)", label_visibility="collapsed")
    selected_code = None
    if search_query:
        try:
            search_results = fetcher.search_funds(search_query)
            if search_results:
                schemes = list(search_results.values())
                default_ix = 0
                for i, name in enumerate(schemes):
                    if "Direct" in name and "Growth" in name:
                        default_ix = i
                        break
                selected_name = st.selectbox("Schemes", options=schemes, index=default_ix, label_visibility="collapsed")
                selected_code = [k for k, v in search_results.items() if v == selected_name][0]
            else:
                st.error("Not found.")
        except Exception as e:
            st.error(f"Search error: {e}")
            logger.error(f"Fund search failed for query '{search_query}': {e}")

    # Benchmark Selection (Immediately follows Fund)
    st.markdown("<br>", unsafe_allow_html=True)  # Visual Spacing
    st.markdown("**2. Comparison Benchmark**")
    bench_type = st.radio("Benchmark Type", ["Index", "Fund"], horizontal=True, label_visibility="collapsed")
    benchmark_code = None
    benchmark_name = "Benchmark"
    benchmark_ticker = None
    if bench_type == "Index":
        # Institutional-Grade: Comprehensive mapping of standard Indian indices
        INDEX_MAPPING = {"Nifty 50": "^NSEI", "Nifty Next 50": "^NSMIDCP", "Nifty 100": "^CNX100", "Nifty 200": "^CNX200", "Nifty 500": "^CRSLDX"}
        bench_option = st.selectbox("Index", options=list(INDEX_MAPPING.keys()), index=0, label_visibility="collapsed")
        benchmark_ticker = INDEX_MAPPING[bench_option]
        benchmark_name = bench_option
    else:
        bench_search = st.text_input("Benchmark Fund Search", placeholder="Search Benchmark Fund (e.g. Nifty Index Fund)", label_visibility="collapsed")
        if bench_search:
            bench_results = fetcher.search_funds(bench_search)
            if bench_results:
                benchmark_name = st.selectbox("Select", options=list(bench_results.values()), label_visibility="collapsed")
                benchmark_code = [k for k, v in bench_results.items() if v == benchmark_name][0]

    # Horizon Horizon Selection
    st.markdown(
        "<p style='border-top: 1px solid rgba(255,255,255,0.1); margin-top: 25px; padding-top: 10px; font-weight: 700; font-size: 1.1rem;'>⏳ Horizon</p>",
        unsafe_allow_html=True,
    )
    analysis_period = st.radio("Period", ["All Time", "1 Year", "3 Years", "5 Years", "10 Years", "Custom Range"], index=0, label_visibility="collapsed")

    import datetime

    custom_start_date: Optional[datetime.date] = None
    custom_end_date: Optional[datetime.date] = None

    if analysis_period == "Custom Range":
        c1, c2 = st.columns(2)
        with c1:
            custom_start_date = st.date_input("Start", value=pd.to_datetime("2020-01-01"))
        with c2:
            custom_end_date = st.date_input("End", value=pd.to_datetime("today"))

    # Technical Calibration (How)
    st.markdown(
        "<p style='border-top: 1px solid rgba(255,255,255,0.1); margin-top: 25px; padding-top: 10px; font-weight: 700; font-size: 1.1rem;'>⚙️ Calibration</p>",
        unsafe_allow_html=True,
    )
    # Fetch real-time rate for initial calibration (fallback to 6.5)
    default_rf = fetcher.get_current_risk_free_rate() * 100
    risk_free_rate = st.slider("Risk-Free Rate (%)", 0.0, 10.0, default_rf, 0.1) / 100
    analytics.rf = risk_free_rate

    st.markdown("---")
    if st.sidebar.button("♻️ Refresh System"):
        st.cache_resource.clear()
        st.rerun()

# Main Content
if selected_code:
    # Initialize variables for reliable AI/UI data sharing
    ret_data: Dict[str, float] = {}
    risk_metrics: Dict[str, float] = {}
    cap_metrics: Dict[str, float] = {}
    stress_res = pd.DataFrame()

    # Secure Fresh State: Reset temporary analytics vault
    st.session_state["period_ratios"] = {}

    with st.spinner(f"Analyzing {selected_name}..."):
        try:
            raw_nav_data = fetcher.get_nav_history(selected_code)
            fund_info = fetcher.get_fund_info(selected_code)
        except Exception as e:
            logger.error(f"AMFI API failure: {e}")
            st.error("⚠️ **API Gateway Error (AMFI)**")
            st.warning(f"{e}")
            st.info("The external AMFI server is currently unresponsive. Please wait 5-10 minutes and try again.")
            st.stop()

        if not raw_nav_data.empty:
            # Determine start date for benchmark based on available fund history
            raw_start_date = raw_nav_data.index[0]

            if bench_type == "Index":
                raw_bench_data = fetcher.get_benchmark_history(benchmark_ticker, start_date=raw_start_date)
            else:
                if benchmark_code:
                    if benchmark_code == selected_code:
                        st.warning("Comparing a fund against itself. Benchmark data will be identical.")
                    try:
                        raw_bench_data_df = fetcher.get_nav_history(benchmark_code)
                        raw_bench_data = raw_bench_data_df["nav"] if not raw_bench_data_df.empty else pd.Series()
                    except Exception as e:
                        st.error(f"Could not load benchmark fund: {e}")
                        raw_bench_data = pd.Series()
                else:
                    raw_bench_data = pd.Series()

            if bench_type == "Index" and raw_bench_data.empty:
                st.warning(f"⚠️ **Benchmark Connection Timeout**: Could not retrieve {benchmark_name} history. Only fund performance will be displayed.")
            elif not raw_bench_data.empty:
                log_event(logger, "BENCHMARK_LOADED", ticker=benchmark_name, status="SUCCESS")

            # 0. Initial App State
            if "app_init" not in st.session_state:
                log_event(logger, "APP_LAUNCH", status="SUCCESS")
                st.session_state.app_init = True
            # Apply Time Period Filtering
            nav_data = raw_nav_data.copy()
            bench_data = raw_bench_data.copy() if not raw_bench_data.empty else pd.Series()

            if analysis_period == "Custom Range":
                if custom_start_date and custom_end_date:
                    start_ts = pd.Timestamp(custom_start_date)
                    end_ts = pd.Timestamp(custom_end_date)
                    nav_data = nav_data[(nav_data.index >= start_ts) & (nav_data.index <= end_ts)]
                    if not bench_data.empty:
                        bench_data = bench_data[(bench_data.index >= start_ts) & (bench_data.index <= end_ts)]
            elif analysis_period != "All Time":
                years = int(analysis_period.split(" ")[0])
                cutoff_date = nav_data.index[-1] - pd.DateOffset(years=years)
                nav_data = nav_data[nav_data.index >= cutoff_date]
                if not bench_data.empty:
                    bench_data = bench_data[bench_data.index >= cutoff_date]

            if nav_data.empty:
                st.error("No data available for the selected range. Please adjust your dates or time horizon.")
                st.stop()

    if not raw_nav_data.empty:
        # Clarify Period Label if actual history is shorter than selection
        actual_days = (nav_data.index[-1] - nav_data.index[0]).days
        actual_yrs = actual_days / 365.25

        # Determine if we should show 'Since Inception' or the selected period
        is_si = False
        if analysis_period == "Custom Range":
            display_label = f"Custom (~{actual_yrs:.1f}Y)"
        elif analysis_period != "All Time":
            try:
                req_yrs = int(analysis_period.split(" ")[0])
                if actual_yrs < (req_yrs - 0.1):  # If significantly shorter than requested
                    is_si = True
            except Exception:
                pass

        if analysis_period != "Custom Range":
            display_label = f"S.I. (~{actual_yrs:.1f}Y)" if (is_si or analysis_period == "All Time") else analysis_period

        # Fund Title & Stats Summary (Zero Margin)
        st.markdown(f"<h2 style='margin-top: 0rem; margin-bottom: 0rem;'>{selected_name}</h2>", unsafe_allow_html=True)
        st.markdown(
            f"<p style='color: #e2e8f0; font-size: 0.92rem; margin-top: -0.6rem; margin-bottom: 0.9rem;'>"
            f"{fund_info.get('scheme_type', 'N/A')} | "
            f"{fund_info.get('scheme_category', 'N/A')} | "
            f"{fund_info.get('fund_house', 'N/A')} | "
            f"⚖️ <span style='font-weight: 600;'>Benchmark:</span> <span style='color: #d4af37; font-weight: 600;'>{benchmark_name}</span> | "
            f"⏳ <span style='font-weight: 600;'>Horizon:</span> <span style='color: #d4af37; font-weight: 600;'>{display_label}</span>"
            f"</p>",
            unsafe_allow_html=True,
        )

        # Top Level Metrics - Single Row
        metrics = analytics.calculate_risk_metrics(nav_data["nav"])
        _, max_dd = analytics.calculate_drawdowns(nav_data["nav"])
        multiplier = analytics.calculate_fund_multiplier(nav_data["nav"])

        m_col0, m_col1, m_col2, m_col3, m_col4 = st.columns(5)
        m_col0.metric("Growth", f"{multiplier:.2f}x", help="Investment multiple. e.g., 2.0x means your money doubled.")
        m_col1.metric("CAGR", f"{metrics.get('cagr', 0):.1%}", help="The effective annual growth rate of your investment, assuming returns are compounded yearly.")
        m_col2.metric("Volatility", f"{metrics.get('volatility', 0):.1%}", help="Annualized Standard Deviation of returns. Measures the 'bounciness' or risk of the fund.")
        m_col3.metric("Sharpe Ratio", f"{metrics.get('sharpe_ratio', 0):.2f}", help="Excess return per unit of risk. Higher is better. Uses the provided Risk-Free Rate.")
        m_col4.metric("Max Drawdown", f"{max_dd:.1%}", help="Largest peak-to-trough decline. Measures the worst-case loss scenario.")

        # 1. Performance History (Rebased to 100)
        if not bench_data.empty:
            st.plotly_chart(plot_benchmark_comparison(nav_data["nav"], bench_data, selected_name, benchmark_name), width="stretch", key="main_perf_comparison")
        else:
            st.plotly_chart(plot_nav_history(nav_data, selected_name), width="stretch", key="main_nav_history")

        # 2. Drawdown History
        drawdown_fund, _ = analytics.calculate_drawdowns(nav_data["nav"])
        drawdown_bench = None
        if not bench_data.empty:
            drawdown_bench, _ = analytics.calculate_drawdowns(bench_data)
        st.plotly_chart(plot_drawdown(drawdown_fund, drawdown_bench, selected_name, benchmark_name), width="stretch", key="main_drawdown_history")

        # 2.5 Historical Stress Scenarios (Historical Resilience)
        if not raw_bench_data.empty:
            stress_df = analytics.calculate_stress_performance(raw_nav_data["nav"], raw_bench_data)
            stress_res = stress_df.to_dict("records")  # Archive for AI synthesis
            if not stress_df.empty:
                # Visualize the stress test via component
                st.plotly_chart(plot_stress_scenarios(stress_df), width="stretch", key="stress_scenarios_chart")

                # Data Table with cleaned labels
                disp_stress = stress_df.copy()
                disp_stress = disp_stress.rename(columns={"Fund Drop": "Fund", "Benchmark Drop": "Benchmark"})

                # Format to percentage
                disp_stress["Fund"] = disp_stress["Fund"].apply(lambda x: f"{x:.1%}")
                disp_stress["Benchmark"] = disp_stress["Benchmark"].apply(lambda x: f"{x:.1%}")

                def fmt_capture(val):
                    if pd.isna(val) or val is None:
                        return "N/A"
                    return f"{val * 100:.0f}%"

                disp_stress["Capture Ratio"] = disp_stress["Capture Ratio"].apply(fmt_capture)

                st.dataframe(
                    disp_stress,
                    hide_index=True,
                    width="stretch",
                    column_config={
                        "Crisis": st.column_config.TextColumn("Crisis / Event", width="medium"),
                        "Period": st.column_config.TextColumn("Historical Dates", width="medium"),
                        "Capture Ratio": st.column_config.TextColumn(help="Percentage of the market's decline captured by the fund. Lower is better."),
                    },
                )

        # 3. Calendar Year Returns
        st.markdown("### 📅 Calendar Year Performance")
        f_cal = analytics.calculate_calendar_returns(raw_nav_data["nav"])
        if not raw_bench_data.empty:
            b_cal = analytics.calculate_calendar_returns(raw_bench_data)
            cal_df = pd.DataFrame({"Fund": f_cal, "Benchmark": b_cal})
        else:
            cal_df = pd.DataFrame({"Fund": f_cal})

        # Sort and limit to last 10 entries
        cal_df = cal_df.sort_index(ascending=False).head(11)  # To show roughly 10 years + current YTD

        cal_c1, cal_c2 = st.columns([1, 1.5])
        with cal_c1:
            # Display table with explicit 'Year' label
            disp_cal = cal_df.copy()
            disp_cal.index.name = "Year"
            disp_cal = disp_cal.reset_index()

            # Format percentage values
            for col in ["Fund", "Benchmark"]:
                if col in disp_cal.columns:
                    disp_cal[col] = disp_cal[col].apply(lambda x: f"{x:.1%}" if pd.notnull(x) else "-")

            st.dataframe(
                disp_cal,
                hide_index=True,
                width="stretch",
                height=420,
                column_config={
                    "Year": st.column_config.TextColumn("Year", width=80),
                    "Fund": st.column_config.TextColumn("Fund", width=100),
                    "Benchmark": st.column_config.TextColumn("Benchmark", width=100),
                },
            )

        with cal_c2:
            # Display comparative bar chart via component
            st.plotly_chart(plot_calendar_returns(cal_df), width="stretch", key="calendar_year_chart")

        # Performance Analysis Data Prep
        periods = {"1 Year": 1, "3 Years": 3, "5 Years": 5, "10 Years": 10}
        ret_metrics_list, vol_data, ratio_data, deep_metrics = [], [], [], []

        for label, yrs in periods.items():
            f_ret, f_vol, f_stats = analytics.get_periodic_metrics(raw_nav_data["nav"], yrs, raw_bench_data)

            # Benchmark standalone metrics for the same window
            b_ret, b_vol = None, None
            if not raw_bench_data.empty:
                b_ret, b_vol, _ = analytics.get_periodic_metrics(raw_bench_data, yrs)

            # Data for compact sections
            ret_metrics_list.append({"Period": label, "Fund": f_ret, "Benchmark": b_ret})
            vol_data.append({"Period": label, "Fund": f_vol, "Benchmark": b_vol})

            # Populate the AI dictionary
            ret_data[label] = f_ret if f_ret else 0.0

            # Archive the rich ratio data for the AI Agent
            if "period_ratios" not in st.session_state:
                st.session_state["period_ratios"] = {}
            st.session_state["period_ratios"][label] = f_stats if f_stats else {}

            f_rat = (f_ret / f_vol) if f_ret and f_vol else None
            b_rat = (b_ret / b_vol) if b_ret and b_vol else None
            ratio_data.append({"Period": label, "Fund": f_rat, "Benchmark": b_rat})

            if f_stats:
                deep_metrics.append(
                    {
                        "Period": label,
                        "Sharpe": f_stats.get("Sharpe", 0),
                        "Sortino": f_stats.get("Sortino", 0),
                        "Calmar": f_stats.get("Calmar", 0),
                        "Info Ratio": f_stats.get("InfoRatio", 0),
                        "Omega": f_stats.get("Omega", 0),
                        "Beta": f_stats.get("Beta", 0),
                        "Jensen Alpha": f_stats.get("Alpha", 0),
                        "Batting Avg": f_stats.get("BattingAvg", 0) / 100.0,
                        "Upside / Downside": f_stats.get("CaptureRatio", 0),
                        "Upside Capture": f_stats.get("Upside", 0) / 100.0,
                        "Downside Capture": f_stats.get("Downside", 0) / 100.0,
                    }
                )

        def display_metric_section(title, data_list, is_pct=True):
            st.markdown(f"### {title}")
            df = pd.DataFrame(data_list)

            # Determine Axis Label based on title
            y_label_map = {"Periodic Returns": "Annual Return", "Periodic Volatility": "Annual Volatility", "Return / Risk Ratio": "Return / Risk"}
            y_label = y_label_map.get(title, "Metric")

            col_tbl, col_cht = st.columns([1, 1.5])
            with col_tbl:
                display_df = df.copy()
                for col in ["Fund", "Benchmark"]:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(lambda x: f"{x:.1%}" if is_pct and pd.notnull(x) else (f"{x:.2f}" if pd.notnull(x) else "-"))

                # Professional Column Config for Periodic blocks
                config = {
                    "Period": st.column_config.TextColumn("Period", width=80),
                    "Fund": st.column_config.TextColumn("Fund", width=100),
                    "Benchmark": st.column_config.TextColumn("Benchmark", width=100),
                }

                if title == "Periodic Returns":
                    help_text = "Annualized performance over the specific window."
                elif title == "Periodic Volatility":
                    help_text = "Annualized risk (Standard Deviation). Lower is preferred for conservative investors."
                else:
                    help_text = "Risk-Adjusted Return (Return / Volatility). Measures return per 1% risk."

                config["Fund"] = st.column_config.TextColumn("Fund", help=help_text, width=100)
                st.dataframe(display_df, hide_index=True, width="stretch", column_config=config)

            with col_cht:
                fig = plot_periodic_metrics(df, is_pct=is_pct, y_label=y_label)
                st.plotly_chart(fig, width="stretch", key=f"bar_{title.lower().replace(' ', '_')}")

        display_metric_section("Periodic Returns", ret_metrics_list)
        display_metric_section("Periodic Volatility", vol_data)
        display_metric_section("Return / Risk Ratio", ratio_data, is_pct=False)

        # Market Participation Insight
        if not bench_data.empty and len(bench_data) > 20:
            c1, c2 = st.columns([1.2, 1])
            with c1:
                # Scatter Plot for Insight: Fund Monthly vs Benchmark Monthly
                df_monthly = analytics.get_monthly_returns(nav_data["nav"], bench_data)
                fig_scatter = plot_market_sensitivity(df_monthly, benchmark_name)
                st.plotly_chart(fig_scatter, width="stretch", key="market_sensitivity_scatter")
                st.info("**Interpretation:** Points above the **orange dashed line** beat the Benchmark. A steeper blue line indicates **High Beta** (more aggressive than the benchmark).")

            with c2:
                cap_metrics = analytics.calculate_capture_ratios(nav_data["nav"], bench_data)
                st.plotly_chart(plot_capture_ratios(cap_metrics), width="stretch", key="capture_ratios_summary")

        if deep_metrics:
            df_full = pd.DataFrame(deep_metrics)

            st.markdown("#### 🧩 Comprehensive Analytics")

            # Category Descriptions for Hover context
            risk_efficiency_help = "Insights into how efficiently the fund generates returns for every unit of risk taken."
            style_consistency_help = "Analysis of the fund's behavioral style, consistency, and active management character."

            # Construct simple flat headers for high-density terminal look
            col_list = ["Period", "Sharpe", "Sortino", "Calmar", "Info Ratio", "Omega", "Beta", "Jensen Alpha", "Batting Avg", "Upside / Downside", "Upside Capture", "Downside Capture"]

            # Re-index full dataframe with flat columns
            df_display = df_full[col_list].copy()

            # Column Config: Map to actual column names for type safety
            col_config = {
                "Period": st.column_config.TextColumn("Period", width=80, pinned=True),
                "Sharpe": st.column_config.TextColumn("Sharpe", help="Excess return per unit of total risk. Higher is better.", width=75),
                "Sortino": st.column_config.TextColumn("Sortino", help="Penalizes downside volatility. Ideal for skewed return profiles.", width=75),
                "Calmar": st.column_config.TextColumn("Calmar", help="CAGR / Max Drawdown. Measures return vs 'crash' risk.", width=75),
                "Info Ratio": st.column_config.TextColumn("Info Ratio", help="Active return vs benchmark per tracking error.", width=105),
                "Omega": st.column_config.TextColumn("Omega", help="Gains vs losses weighted by probability.", width=75),
                "Beta": st.column_config.TextColumn("Beta", help="Market Sensitivity. 1.0 = moves with index. >1.0 Aggressive, <1.0 Defensive.", width=75),
                "Jensen Alpha": st.column_config.TextColumn("Jensen Alpha", help="Annualized excess return above market expectation.", width=105),
                "Batting Avg": st.column_config.TextColumn("Batting Avg", help="Frequency the fund beat the benchmark.", width=100),
                "Upside / Downside": st.column_config.TextColumn("Up/Down Efficiency", help="Efficiency Ratio: Upside capture divided by Downside capture. Higher is better.", width=130),
                "Upside Capture": st.column_config.TextColumn("Upside Capture", help="Gain capture during positive months.", width=125),
                "Downside Capture": st.column_config.TextColumn("Downside Capture", help="Loss capture during negative months.", width=140),
            }

            # Archive for AI Synthesis vault: High-fidelity forensic metrics
            st.session_state["deep_metrics_vault"] = deep_metrics

            # Standard formatting for professional clarity without heatmapping
            styler_df = df_display.style.format(
                {
                    "Period": "{}",
                    "Sharpe": "{:.2f}",
                    "Sortino": "{:.2f}",
                    "Calmar": "{:.2f}",
                    "Info Ratio": "{:.2f}",
                    "Omega": "{:.2f}",
                    "Beta": "{:.2f}",
                    "Jensen Alpha": "{:.1%}",
                    "Batting Avg": "{:.0%}",
                    "Upside / Downside": "{:.2f}",
                    "Upside Capture": "{:.0%}",
                    "Downside Capture": "{:.0%}",
                }
            )

            st.dataframe(
                styler_df,
                hide_index=True,
                width="stretch",
                column_config=col_config,
            )
            st.caption("Detailed analysis of active management character, risk efficiency, and return consistency across horizons.")

        # 4. Rolling Returns Performance
        st.markdown("### 📊 Rolling Returns Performance Profile")

        # Logic to extract profiles and find common horizons
        fund_profile_raw = analytics.calculate_rolling_return_profile(raw_nav_data["nav"], bench_nav_series=raw_bench_data)
        bench_profile_raw = analytics.calculate_rolling_return_profile(raw_bench_data) if not raw_bench_data.empty else {}

        # Determine common labels available for both (Intersection for consistent comparison)
        if bench_profile_raw:
            f_keys = set(fund_profile_raw.keys())
            b_keys = set(bench_profile_raw.keys())
            common_horizons = sorted(list(f_keys.intersection(b_keys)), key=lambda x: int(x.split(" ")[0]))
        else:
            common_horizons = sorted(list(fund_profile_raw.keys()), key=lambda x: int(x.split(" ")[0]))

        # Filter the profiles to only shared windows
        fund_profile = {k: fund_profile_raw[k] for k in common_horizons if fund_profile_raw.get(k)}
        bench_profile = {k: bench_profile_raw[k] for k in common_horizons if bench_profile_raw.get(k)}

        def display_rolling_grid(profile, title, key_suffix):
            if not profile:
                st.info(f"Insufficient {title} history.")
                return

            profile_df = pd.DataFrame(profile)
            row_order = [
                "Minimum Return",
                "Median Return",
                "Maximum Return",
                "% times returns   < -20%",
                "% times returns   -20% to -10%",
                "% times returns   -10% to 0%",
                "% times returns    0% to 5%",
                "% times returns    5% to 10%",
                "% times returns   10% to 15%",
                "% times returns   15% to 20%",
                "% times returns   > 20%",
            ]
            profile_df = profile_df.reindex(row_order)
            disp_profile = profile_df.reset_index().rename(columns={"index": "Metric"})

            # Styler Logic
            formatter: Dict[Any, Any] = {str(c): "{:.0%}" for c in disp_profile.columns if str(c) != "Metric"}
            styled_df = disp_profile.style.format(formatter=formatter, na_rep="-")

            def apply_grid_styling(row):
                colors = [""] * len(row)
                metric = row["Metric"]

                # 1. Summary Block (Min, Median, Max) - High-Visibility Institutional Steel
                if metric in ["Minimum Return", "Median Return", "Maximum Return"]:
                    for i in range(len(row)):
                        colors[i] = "background-color: rgba(226, 232, 240, 0.18); color: #ffffff; font-weight: bold; border-left: 3px solid #d4af37;"
                    return colors

                # 2. Identification of Probability Clusters
                base_rgb = None
                if any(s in metric for s in ["< -20", "-20% to -10", "-10% to 0"]):
                    base_rgb = "239, 68, 68"  # Tailwind Red
                elif any(s in metric for s in ["10% to 15%", "15% to 20%", "> 20"]):
                    base_rgb = "34, 197, 94"  # Tailwind Green
                elif any(s in metric for s in ["0% to 5%", "5% to 10%"]):
                    base_rgb = "245, 158, 11"  # Tailwind Amber

                if base_rgb:
                    # Apply a high-density base tint to the entire row
                    row_base_tint = f"background-color: rgba({base_rgb}, 0.14); color: #ffffff;"
                    for i in range(len(row)):
                        colors[i] = row_base_tint

                    # Apply the Sharp Heatmap to the data columns
                    for i in range(1, len(row)):
                        val = row.iloc[i]
                        if pd.notnull(val) and not isinstance(val, str):
                            # Aggressive Scaling: Higher starting alpha (0.2) and higher peak (0.85)
                            alpha = min(0.2 + (float(val) * 0.75), 0.85)
                            colors[i] = f"background-color: rgba({base_rgb}, {alpha}); color: #ffffff; border: 0.5px solid rgba(255,255,255,0.05);"
                return colors

            styled_df = styled_df.apply(apply_grid_styling, axis=1)

            st.markdown(f"**{title}**")
            st.dataframe(
                styled_df,
                width="stretch",
                height=425,
                hide_index=True,
                key=f"rolling_table_{key_suffix}",
                column_config={
                    "Metric": st.column_config.TextColumn("Outcome Scenarios", width=None),
                },
            )

        c_roll1, c_roll2 = st.columns(2)
        with c_roll1:
            display_rolling_grid(fund_profile, f"{selected_name}", "fund")
        with c_roll2:
            display_rolling_grid(bench_profile, f"{benchmark_name}", "bench")

        st.caption("Trailing rolling returns calculated on a daily basis for the respective holding periods.")

        # Institutional Vault: Archive results for sidebar/AI consumption
        st.session_state["analytical_vault"] = {"name": selected_name, "benchmark": benchmark_name, "returns": ret_data, "profile": fund_profile, "stress": stress_res}

        # 🧠 AI Insight Integration: Verticalized for direct user-flow
        st.markdown("<br>", unsafe_allow_html=True)

        st.info("Transform quantitative insights into a structured investment report.")

        c_btn1, _, _, _ = st.columns(4)
        with c_btn1:
            if st.button("🧠 Generate AI Report", type="primary", use_container_width=True):
                # 1. Generate the Quantitative Briefing
                briefing = analytics.generate_ai_report_markdown(
                    fund_name=selected_name, benchmark_name=benchmark_name, deep_metrics=deep_metrics, rolling_profiles=fund_profile, stress_df=pd.DataFrame(stress_res)
                )
                st.session_state["ai_report_briefing"] = briefing

                # 2. Attempt Live Synthesis
                # 2. Attempt Live Synthesis – use a placeholder so the caption disappears after completion
                placeholder = st.empty()
                placeholder.caption("🔬 Analyst is synthesizing forensics...")
                live_report = analytics.generate_live_report(briefing)
                st.session_state["live_ai_report"] = live_report
                placeholder.empty()  # remove the caption
                st.toast("Analysis Complete!", icon="✅")

        if "ai_report_briefing" in st.session_state:
            with st.expander("📊 Analyst Briefing — (For deeper forensics, copy this into your preferred AI assistant)"):
                st.caption("💡 **Tip:** Use the **copy icon** on the top right of the code block below to quickly paste this data into your preferred AI assistant.")
                st.code(st.session_state["ai_report_briefing"], language="markdown")

        if "live_ai_report" in st.session_state:
            # v1.1.0 UPGRADE: DETERMINISTIC UI RENDERER
            # ----------------------------------------
            # Implements a fail-safe analytical parser that decouples synthesis from styling.
            # 1. Parsing: Extracts high-conviction content via institutional tags ([SUMMARY], etc.).
            # 2. Quiet Luxury Styling: Injects state-of-the-art CSS for gold-left borders and a premium typography stack.
            # 3. Hybrid Rendering: Combines Markdown (for body content) with HTML (for structural titles) for total UI stability.

            # 🎨 Unified CSS and Content Renderer: No more "Ghost Boxes"
            raw = st.session_state["live_ai_report"]
            sects = {"[SUMMARY]": "", "[BREAKDOWN]": "", "[ACTIONABLES]": ""}

            curr = None
            for line in raw.split("\n"):
                tg = line.strip()
                if tg in sects:
                    curr = tg
                elif curr:
                    sects[curr] += line + "\n"

            # Clean and sanitize the sections: Convert remaining Markdown to Institutional HTML
            def clean_sec(txt):
                lines = txt.strip().split("\n")
                res = []
                for line in lines:
                    cl = line.strip()
                    # 1. Convert Bolding to Standard White Bold
                    cl = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", cl)

                    # 2. Handle Universal Bullets (* or -)
                    if cl.startswith("- ") or cl.startswith("* "):
                        bullet_content = cl[2:].strip()

                        # 3. If Bullet starts with Topic Header (e.g., <b>...:</b>), turn it GOLD
                        bullet_content = re.sub(r"^<b>(.*?):<\/b>", r"<span style='color: #d4af37; font-weight: 700;'>\1:</span>", bullet_content)

                        res.append(f'<div class="bullet-item"><span class="bullet-dot">•</span><span class="bullet-text">{bullet_content}</span></div>')
                    elif cl:
                        res.append(cl)
                return "".join(res)

            # Build the Single, Atomic HTML block using a refined Institutional Layout
            report_html = f"""<style>
.brief-container {{
    background-color: #1e293b;
    padding: 30px;
    border-radius: 12px;
    border: 1px solid rgba(212, 175, 55, 0.2);
    border-left: 10px solid #d4af37;
    color: #e2e8f0;
    margin-bottom: 35px;
    font-family: 'Inter', system-ui, sans-serif;
}}
.brief-header {{
    color: #d4af37 !important;
    font-weight: 800 !important;
    font-size: 1rem !important;
    margin-bottom: 6px !important;
    text-transform: uppercase;
    letter-spacing: 0.1rem;
    border-bottom: 1px solid rgba(212, 175, 55, 0.1);
    padding-bottom: 6px;
}}
.brief-content {{
    margin-bottom: 25px !important;
    line-height: 1.8 !important;
    font-size: 1rem !important;
}}
.bullet-item {{
    display: flex;
    align-items: flex-start;
    margin-bottom: 8px;
}}
.bullet-dot {{
    color: #d4af37;
    margin-right: 12px;
    font-weight: 900;
}}
.bullet-text {{
    flex: 1;
}}
</style>
<div class="brief-container">
<p class="brief-header">1. Executive Summary</p>
<div class="brief-content">{clean_sec(sects["[SUMMARY]"])}</div>
<p class="brief-header">2. Quantitative Breakdown</p>
<div class="brief-content">{clean_sec(sects["[BREAKDOWN]"])}</div>
<p class="brief-header">3. Tactical Actionables</p>
<div class="brief-content" style="margin-bottom: 0px !important;">{clean_sec(sects["[ACTIONABLES]"])}</div>
</div>"""

            st.markdown("### 📝 Investment Brief")
            st.markdown(report_html, unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

    else:
        st.error("Historical NAV data unavailable.")

else:
    st.info("👈 Enter a fund name (e.g., 'HDFC Flexi' or 'SBI Bluechip') to begin deep analysis.")

# --- Institutional Fiduciary Disclosure & Disclaimer (Footer) ---
st.divider()
st.caption(
    "⚠️ **Disclaimer & Fiduciary Notice:** All analytics, quantitative metrics, and AI-generated synthesis provided by **ConvexLab** are exclusively for **educational and informational purposes**. "
    "These outputs represent historical data approximations and statistical modeling based on third-party public information. "
    "**Important:** This application **does not constitute investment advice**, financial planning, or a professional recommendation to buy, sell, or hold any security. "
    "Mutual fund investments are subject to market risks; please read all scheme-related documents carefully and consult with a qualified financial advisor before making any investment decisions. "
    "**Convexica** and its developers assume no liability for potential inaccuracies or financial loss derived from the use of this dashboard."
)

# Cache-Bust: 2026-04-03 18:35:00 (v1.1.0 Production Ready)
