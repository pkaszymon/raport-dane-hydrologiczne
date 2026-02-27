"""Streamlit UI for IMGW hydrological and climatological data exports."""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import polars as pl
import streamlit as st

from data_processing import (
    HYDRO_AGGREGATION_INTERVALS,
    HYDRO_API_CATEGORIES,
    aggregate_hydro_category,
    chunk_dataframe,
    dataframe_to_excel_bytes,
    named_sheets_to_excel_bytes,
    split_hydro_api_data,
)
from ui_api_tab import render_api_tab
from ui_file_tab import render_file_tab

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_data_preview_panel(
    data: pl.DataFrame,
    row_limit: int = 100,
    height: int = 400,
    title_preview: str = "📋 Podgląd danych",
    title_stats: str = "📈 Statystyki",
    columns_to_analyze: Optional[list[str]] = None,
    date_column: Optional[str] = "Data",
    sort_by_column: Optional[str] = None,
    descending: bool = True
) -> None:
    """
    Create a responsive two-column panel with data preview and statistics.
    
    Parameters:
    -----------
    data : pl.DataFrame
        The Polars DataFrame to display.
    row_limit : int
        Maximum number of rows to display in the preview table.
    height : int
        Height of the data table in pixels.
    title_preview : str
        Title for the preview/table section.
    title_stats : str
        Title for the statistics section.
    columns_to_analyze : list[str], optional
        Specific columns to analyze for statistics. If None, analyzes all string columns.
    date_column : str, optional
        Name of the column containing date values (e.g., "Data").
    sort_by_column : str, optional
        Column name to sort by.
    descending : bool
        Whether to sort in descending order.
    """
    
    df = data
    
    # Validate data
    if df.is_empty():
        st.warning("⚠️ Brak danych do wyświetlenia")
        return
    
    # Sort data if requested
    if sort_by_column and sort_by_column in df.columns:
        df = df.sort(sort_by_column, descending=descending)
    
    # Create two-column layout
    col_preview, col_stats = st.columns([3, 1])
    
    # LEFT COLUMN: Data Preview Table
    with col_preview:
        st.subheader(title_preview)
        st.dataframe(
            df.head(row_limit),
            use_container_width=True,
            height=height
        )
    
    # RIGHT COLUMN: Statistics
    with col_stats:
        st.subheader(title_stats)
        
        # Determine which columns to analyze
        if columns_to_analyze is None:
            # Auto-detect string columns (excluding date columns)
            columns_to_analyze = [
                col for col in df.columns 
                if col != date_column and (
                    df[col].dtype in [pl.Utf8, pl.Categorical]
                )
            ]
        
        # Display basic statistics
        _display_basic_statistics(df, columns_to_analyze)
        
        # Display time range if date column exists
        if date_column and date_column in df.columns:
            st.divider()
            _display_time_statistics(df, date_column)
    
    # Additional Info Row
    st.caption(f"Wyświetlonych rekordów: {min(row_limit, len(df))} z {len(df):,}")


def _display_basic_statistics(df: pl.DataFrame, columns_to_analyze: list[str]) -> None:
    """Display basic statistics for the given columns."""
    
    # Create two-column layout for metrics
    col1, col2 = st.columns(2)
    
    # Display only essential metrics
    with col1:
        st.metric("📊 Rekordy", f"{len(df):,}")
    
    with col2:
        st.metric("💾 Rozmiar", f"{df.estimated_size('mb'):.1f} MB")
    
    # Show unique count for specific important columns
    priority_groups = {
        'stations': ['nazwa stacji', 'nazwa wodowskazu', 'wodowskaz', 'stacja', 'nazwa_stacji'],
        'rivers': ['rzeka'],
        'provinces': ['wojewodztwo', 'województwo']
    }
    
    shown = set()
    metrics = []
    
    for group_name, priority_columns in priority_groups.items():
        for col in columns_to_analyze:
            lower_col = col.lower()
            if lower_col in priority_columns and col not in shown:
                if df[col].dtype == pl.Utf8 or df[col].dtype == pl.Categorical:
                    unique_count = df.select(pl.col(col).n_unique()).item()
                    col_label = _get_column_label(col)
                    metrics.append((col_label, unique_count))
                    shown.add(col)
                    break  # Show only the first match per group
    
    # Display additional metrics in two columns
    for idx, (label, value) in enumerate(metrics):
        if idx % 2 == 0:
            with col1:
                st.metric(label, value)
        else:
            with col2:
                st.metric(label, value)


def _display_time_statistics(df: pl.DataFrame, date_column: str) -> None:
    """Display time range statistics if date column is available."""
    
    st.write("**⏱️ Zakres czasowy:**")
    
    try:
        # Get min and max dates
        date_values = df.select(pl.col(date_column)).to_series().to_list()
        
        # Filter out None values
        date_values = [d for d in date_values if d is not None]
        
        if not date_values:
            st.caption("Brak danych")
            return
        
        min_date = min(date_values)
        max_date = max(date_values)
        
        # Display dates based on type
        if isinstance(min_date, date):
            st.caption(f"🔹 {min_date.strftime('%Y-%m-%d')}")
            st.caption(f"🔸 {max_date.strftime('%Y-%m-%d')}")
        else:
            # String dates - show shortened version
            st.caption(f"🔹 {str(min_date)[:10]}")
            st.caption(f"🔸 {str(max_date)[:10]}")
    
    except Exception:
        st.caption("Brak danych")


def _get_column_label(column_name: str) -> str:
    """Convert column name to human-readable Polish label."""
    
    labels_map = {
        'nazwa stacji': '📍 Stacje',
        'nazwa wodowskazu': '🌊 Wodowskazy',
        'wodowskaz': '🌊 Wodowskazy',
        'stacja': '📍 Stacje',
        'stacja synoptyczna': '☁️ Stacje synoptyczne',
        'rzeka': '🌊 Rzeki',
        'status': '✓ Statusy',
        'nazwa_stacji': '📍 Stacje',
        'stacja_id': '🔢 ID stacji',
        'wojewodztwo': '🗺️ Województwa',
    }
    
    lower_name = column_name.lower()
    
    if lower_name in labels_map:
        return labels_map[lower_name]
    
    # Fallback: capitalize and add generic label
    return f"📊 {column_name.capitalize()}"


def _display_hydro_api_results(df: pl.DataFrame, meta: dict) -> None:
    """Display hydro API data split into category tables with aggregation and Excel export."""
    logger.info(
        "Displaying hydro API results: rows=%d, columns=%d",
        len(df),
        len(df.columns),
    )
    st.success("Dane przygotowane.")

    # Aggregation interval selector
    st.subheader("Agregacja danych")
    interval_label = st.selectbox(
        "Przedział agregacji",
        options=list(HYDRO_AGGREGATION_INTERVALS.keys()),
        key="hydro_api_interval",
    )
    interval = HYDRO_AGGREGATION_INTERVALS[interval_label]

    # Split into category tables
    categories = split_hydro_api_data(df)

    if not categories:
        st.warning("Brak danych do wyświetlenia po podziale na kategorie.")
        return

    # Determine value/date columns for each category label
    category_meta: dict[str, tuple[str, str]] = {
        label: (value_col, date_col)
        for value_col, date_col, label in HYDRO_API_CATEGORIES
        if label in categories
    }

    # Aggregate if requested
    processed: dict[str, pl.DataFrame] = {}
    for label, cat_df in categories.items():
        value_col, date_col = category_meta[label]
        if interval is not None:
            cat_df = aggregate_hydro_category(cat_df, date_col, value_col, interval)
        processed[label] = cat_df

    # Preview each category
    st.subheader("Podgląd kategorii")
    for label, cat_df in processed.items():
        with st.expander(f"📊 {label} ({len(cat_df):,} rekordów)", expanded=False):
            _, date_col = category_meta[label]
            date_col_actual = date_col if date_col in cat_df.columns else None
            create_data_preview_panel(
                data=cat_df,
                row_limit=200,
                height=300,
                title_preview=label,
                date_column=date_col_actual,
                sort_by_column=date_col_actual,
                descending=False,
            )

    # Excel export
    st.subheader("Eksport do Excel")
    source_key = meta.get("source_key", "hydro_api")
    freq_label = interval_label.replace(" ", "_").replace("(", "").replace(")", "")
    filename = f"imgw_{source_key}_{freq_label}.xlsx"

    excel_bytes = named_sheets_to_excel_bytes(processed)

    st.download_button(
        "Pobierz Excel (wszystkie kategorie)",
        data=excel_bytes,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"download_{source_key}_hydro_categories",
    )


def _display_results(df: pl.DataFrame, meta: dict) -> None:
    """Display fetched data with a preview panel and Excel export."""
    if meta.get("source_key") == "hydro_api":
        _display_hydro_api_results(df, meta)
        return

    logger.info(
        "Displaying results: source=%s, tab=%s, rows=%d, columns=%d",
        meta.get("source_key"),
        meta.get("tab_id"),
        len(df),
        len(df.columns),
    )
    st.success("Dane przygotowane.")

    date_col: Optional[str] = None
    if "Data" in df.columns:
        date_col = "Data"
    elif "dtime" in df.columns:
        date_col = "dtime"

    create_data_preview_panel(
        data=df,
        row_limit=200,
        height=400,
        date_column=date_col,
        sort_by_column=date_col,
        descending=False,
    )

    st.subheader("Eksport do Excel")
    tab_id = meta.get("tab_id", "")
    max_rows = st.number_input(
        "Maksymalna liczba wierszy na arkusz",
        min_value=50000,
        max_value=500000,
        value=200000,
        step=50000,
        key=f"max_rows_{meta.get('source_key', 'export')}_{tab_id}",
    )
    chunks = chunk_dataframe(df, max_rows)
    excel_bytes = dataframe_to_excel_bytes(chunks)

    frequency = meta.get("frequency")
    source_key = meta.get("source_key", "export")
    freq_label = frequency.replace(" ", "_") if frequency else "api"
    filename = f"imgw_{source_key}_{freq_label}.xlsx"

    st.download_button(
        "Pobierz Excel",
        data=excel_bytes,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"download_{source_key}_{tab_id}",
    )


st.set_page_config(page_title="IMGW: raport danych", layout="wide")

st.title("Raport danych IMGW")
st.markdown(
    "Aplikacja pobiera dane hydrologiczne (archiwalne i operacyjne) oraz klimat "
    "i eksportuje je do Excel po nazwie stacji."
)

tab_api, tab_archival,  = st.tabs(["🔗 Dane operacyjne (API)", "📁 Dane archiwalne"])
with tab_api:
    df_api, meta_api = render_api_tab()
    if df_api is not None:
        _display_results(df_api, meta_api)
        
with tab_archival:
    df_file, meta_file = render_file_tab()
    if df_file is not None:
        _display_results(df_file, meta_file)


