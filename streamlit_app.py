"""Streamlit UI for IMGW hydrological and climatological data exports."""

from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

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
    list_directory,
    parse_info_legend,
    read_table_from_bytes,
)


@dataclass(frozen=True)
class DataSource:
    label: str
    base_url: str
    station_candidates: tuple[str, ...]


DATA_SOURCES = {
    "hydro_archival": DataSource(
        label="Dane hydrologiczne archiwalne",
        base_url=IMGW_BASE_URL,
        station_candidates=("Nazwa stacji", "Nazwa wodowskazu", "Wodowskaz"),
    ),
    "hydro_operational": DataSource(
        label="Dane hydrologiczne operacyjne",
        base_url="https://danepubliczne.imgw.pl/pl/datastore?product=Hydro",
        station_candidates=("Nazwa stacji", "Nazwa wodowskazu", "Wodowskaz"),
    ),
    "climate": DataSource(
        label="Dane klimatyczne",
        base_url=f"{IMGW_BASE_URL}dane_meteorologiczne/",
        station_candidates=("Nazwa stacji", "Stacja", "Stacja synoptyczna"),
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


st.set_page_config(page_title="IMGW: raport danych", layout="wide")

st.title("Raport danych IMGW")
st.markdown(
    "Aplikacja pobiera dane hydrologiczne (archiwalne i operacyjne) oraz klimat "
    "i eksportuje je do Excel po nazwie stacji."
)

with st.sidebar:
    st.header("Ustawienia danych")
    source_key = st.selectbox(
        "Rodzaj danych",
        options=list(DATA_SOURCES.keys()),
        format_func=lambda key: DATA_SOURCES[key].label,
    )
    frequency = st.selectbox(
        "Częstotliwość",
        options=("dobowe", "miesięczne", "surowe 10-min"),
    )
    use_date_filter = st.checkbox("Filtruj po dacie", value=False)
    date_range = None
    if use_date_filter:
        default_end = date.today()
        default_start = default_end - timedelta(days=30)
        date_range = st.date_input("Zakres dat", value=(default_start, default_end))
    data_url = st.text_input("URL danych", value=DATA_SOURCES[source_key].base_url)
    info_url = st.text_input("URL pliku info (legenda)")

source = DATA_SOURCES[source_key]

legend_columns: list[str] = st.session_state.get("legend_columns", [])
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
    if not data_url:
        st.error("Podaj URL danych.")
    else:
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

            df = add_date_column(df)

            if date_range and isinstance(date_range, tuple) and len(date_range) == 2:
                start_date, end_date = date_range
                if "Data" in df.columns:
                    df = df.filter(pl.col("Data").is_between(start_date, end_date))

        st.success("Dane przygotowane.")
        
        # Display statistics about the downloaded dataset
        st.subheader("Statystyki pobranych danych")
        st.metric("Liczba wierszy", len(df))
        
        # Find station column and show unique stations
        station_col = None
        for candidate in source.station_candidates:
            if candidate in df.columns:
                station_col = candidate
                break
        
        if station_col:
            unique_stations = df[station_col].unique().sort().to_list()
            st.metric("Liczba unikalnych stacji", len(unique_stations))
            with st.expander("Lista unikalnych stacji"):
                for station in unique_stations:
                    st.text(station)
        
        st.dataframe(df.head(200))

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
        st.download_button(
            "Pobierz Excel",
            data=excel_bytes,
            file_name=f"imgw_{source_key}_{frequency.replace(' ', '_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
