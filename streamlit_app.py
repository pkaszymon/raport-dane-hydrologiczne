"""Streamlit UI for IMGW hydrological and climatological data exports."""

from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, Optional

import polars as pl
import streamlit as st
import xlsxwriter

from imgw_client import (
    IMGW_BASE_URL,
    add_date_column,
    apply_legend_columns,
    decode_text,
    download_bytes,
    extract_zip_entries,
    fetch_hydro_data,
    fetch_meteo_data,
    fetch_synop_data,
    filter_by_station,
    list_directory,
    parse_info_legend,
    read_table_from_bytes,
)


@dataclass(frozen=True)
class DataSource:
    label: str
    base_url: str
    station_candidates: tuple[str, ...]
    is_api: bool = False  # Indicates if this source uses API instead of file downloads


DATA_SOURCES = {
    "hydro_archival": DataSource(
        label="Dane hydrologiczne archiwalne",
        base_url=IMGW_BASE_URL,
        station_candidates=("Nazwa stacji", "Nazwa wodowskazu", "Wodowskaz"),
        is_api=False,
    ),
    "hydro_api": DataSource(
        label="Dane hydrologiczne operacyjne (API)",
        base_url="",  # API doesn't need base URL
        station_candidates=("stacja", "nazwa_stacji", "rzeka"),
        is_api=True,
    ),
    "synop_api": DataSource(
        label="Dane synoptyczne (API)",
        base_url="",  # API doesn't need base URL
        station_candidates=("stacja", "nazwa_stacji"),
        is_api=True,
    ),
    "meteo_api": DataSource(
        label="Dane meteorologiczne (API)",
        base_url="",  # API doesn't need base URL
        station_candidates=("stacja", "nazwa_stacji"),
        is_api=True,
    ),
    "climate": DataSource(
        label="Dane klimatyczne archiwalne",
        base_url=f"{IMGW_BASE_URL}dane_meteorologiczne/",
        station_candidates=("Nazwa stacji", "Stacja", "Stacja synoptyczna"),
        is_api=False,
    ),
}


def format_directory(entries: Iterable) -> list[str]:
    return [f"{'[DIR]' if entry.is_dir else '[PLIK]'} {entry.name}" for entry in entries]


def parse_directory_selection(label: str) -> str:
    return label.replace("[DIR]", "").replace("[PLIK]", "").strip()


def chunk_dataframe(df: pl.DataFrame, max_rows: int) -> list[pl.DataFrame]:
    if len(df) <= max_rows:
        return [df]
    return [df.slice(offset, max_rows) for offset in range(0, len(df), max_rows)]


def dataframe_to_excel_bytes(chunks: list[pl.DataFrame], sheet_prefix: str = "Dane") -> bytes:
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    for index, chunk in enumerate(chunks, start=1):
        sheet_name = sheet_prefix if len(chunks) == 1 else f"{sheet_prefix}{index}"
        worksheet = workbook.add_worksheet(sheet_name[:31])
        worksheet.write_row(0, 0, chunk.columns)
        for row_index, row in enumerate(chunk.iter_rows(), start=1):
            worksheet.write_row(row_index, 0, row)
    workbook.close()
    output.seek(0)
    return output.read()


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


st.set_page_config(page_title="IMGW: raport danych", layout="wide")

st.title("Raport danych IMGW")
st.markdown(
    "Aplikacja pobiera dane hydrologiczne (archiwalne i operacyjne) oraz klimat "
    "i eksportuje je do Excel po nazwie stacji."
)

st.header("Ustawienia danych")

source_key = st.selectbox(
    "Rodzaj danych",
    options=list(DATA_SOURCES.keys()),
    format_func=lambda key: DATA_SOURCES[key].label,
)

source = DATA_SOURCES[source_key]

# Different UI for API sources vs file-based sources
if source.is_api:
    st.info("Źródło danych: API IMGW (aktualne dane operacyjne)")
    
    col1, col2 = st.columns(2)
    with col1:
        station_name = st.text_input(
            "Nazwa stacji (opcjonalnie)", 
            help="Wpisz nazwę stacji bez polskich znaków (np. 'warszawa', 'krakow'). Pozostaw puste dla wszystkich stacji."
        )
    with col2:
        station_id = st.number_input(
            "ID stacji (opcjonalnie)",
            min_value=0,
            value=0,
            help="Podaj ID stacji jeśli jest znane. 0 = pobierz wszystkie stacje."
        )
    
    if station_id == 0:
        station_id = None
    if not station_name:
        station_name = None
    # API always returns current data, no frequency or date filter needed
    frequency = None
    use_date_filter = False
    date_range = None
    data_url = None
    info_url = None
else:
    st.info("Źródło danych: Pliki archiwalne")
    
    col1, col2 = st.columns(2)
    with col1:
        frequency = st.selectbox(
            "Częstotliwość",
            options=("dobowe", "miesięczne", "surowe 10-min"),
        )
        station_name = st.text_input("Nazwa stacji", help="Wpisz nazwę wodowskazu lub stacji synoptycznej.")
    
    with col2:
        use_date_filter = st.checkbox("Filtruj po dacie", value=False)
        date_range = None
        if use_date_filter:
            default_end = date.today()
            default_start = default_end - timedelta(days=30)
            date_range = st.date_input("Zakres dat", value=(default_start, default_end))
    
    data_url = st.text_input("URL danych", value=source.base_url)
    info_url = st.text_input("URL pliku info (legenda)")

st.divider()

legend_columns: list[str] = st.session_state.get("legend_columns", [])

# Legend and directory browsing only for file-based sources
if not source.is_api:
    if info_url:
        if st.button("Pobierz legendę"):
            try:
                legend_text = decode_text(download_bytes(info_url))
                legend_columns = parse_info_legend(legend_text)
                st.session_state["legend_columns"] = legend_columns
                if legend_columns:
                    st.success("Pobrano legendę i wykryto kolumny.")
                    st.text("\n".join(legend_columns))
                else:
                    st.warning("Nie udało się wyodrębnić kolumn z pliku info.")
            except RuntimeError as exc:
                st.error(str(exc))

    st.subheader("Podgląd katalogu (opcjonalnie)")
    if st.button("Pokaż zawartość katalogu"):
        try:
            entries = list_directory(data_url)
            if entries:
                st.write(format_directory(entries))
            else:
                st.info("Brak widocznych wpisów w katalogu.")
        except RuntimeError as exc:
            st.error(str(exc))


st.subheader("Pobieranie danych")
if st.button("Pobierz dane"):
    df = None
    
    # API-based data sources
    if source.is_api:
        with st.spinner("Pobieranie danych z API IMGW..."):
            try:
                # Normalize station name (remove Polish diacritics)
                normalized_station = None
                if station_name:
                    replacements = str.maketrans({
                        "ą": "a", "ć": "c", "ę": "e", "ł": "l", "ń": "n",
                        "ó": "o", "ś": "s", "ż": "z", "ź": "z",
                        "Ą": "A", "Ć": "C", "Ę": "E", "Ł": "L", "Ń": "N",
                        "Ó": "O", "Ś": "S", "Ż": "Z", "Ź": "Z",
                    })
                    normalized_station = station_name.lower().translate(replacements).replace(" ", "")
                
                if source_key == "hydro_api":
                    df = fetch_hydro_data(station_id=station_id, station_name=normalized_station)
                elif source_key == "synop_api":
                    df = fetch_synop_data(station_id=station_id, station_name=normalized_station)
                elif source_key == "meteo_api":
                    df = fetch_meteo_data(station_id=station_id, station_name=normalized_station)
                
                if df is None or len(df) == 0:
                    st.warning("Brak danych z API. Sprawdź parametry zapytania.")
                    st.stop()
                    
            except RuntimeError as exc:
                st.error(f"Błąd podczas pobierania danych z API: {exc}")
                st.stop()
    
    # File-based data sources
    else:
        if not data_url:
            st.error("Podaj URL danych.")
            st.stop()
        
        with st.spinner("Pobieranie pliku..."):
            try:
                raw_bytes = download_bytes(data_url)
            except RuntimeError as exc:
                st.error(str(exc))
                st.stop()

        data_candidates = {"plik": raw_bytes}
        if raw_bytes[:4] == b"PK\x03\x04":
            with st.spinner("Rozpakowywanie archiwum..."):
                data_candidates = extract_zip_entries(raw_bytes)

        selected_name = st.selectbox("Wybierz plik", options=list(data_candidates.keys()))
        if not selected_name:
            st.stop()

        with st.spinner("Przetwarzanie danych..."):
            df = read_table_from_bytes(data_candidates[selected_name])
            if legend_columns:
                df = apply_legend_columns(df, legend_columns)

            df = filter_by_station(df, station_name, source.station_candidates)
            df = add_date_column(df)

            if date_range and isinstance(date_range, tuple) and len(date_range) == 2:
                start_date, end_date = date_range
                if "Data" in df.columns:
                    df = df.filter(pl.col("Data").is_between(start_date, end_date))

    # Display results
    if df is not None:
        st.success("Dane przygotowane.")
        
        # Determine date column (prefer "Data" for archival, "dtime" for API)
        date_col = None
        if "Data" in df.columns:
            date_col = "Data"
        elif "dtime" in df.columns:
            date_col = "dtime"
        
        # Display data preview with statistics
        create_data_preview_panel(
            data=df,
            row_limit=200,
            height=400,
            date_column=date_col,
            sort_by_column=date_col,
            descending=False
        )

        st.subheader("Eksport do Excel")
        max_rows = st.number_input(
            "Maksymalna liczba wierszy na arkusz",
            min_value=50000,
            max_value=500000,
            value=200000,
            step=50000,
        )
        chunks = chunk_dataframe(df, max_rows)
        excel_bytes = dataframe_to_excel_bytes(chunks)
        
        # Generate filename
        freq_label = frequency.replace(" ", "_") if frequency else "api"
        filename = f"imgw_{source_key}_{freq_label}.xlsx"
        
        st.download_button(
            "Pobierz Excel",
            data=excel_bytes,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
