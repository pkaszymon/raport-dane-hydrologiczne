"""Streamlit UI for archival file-based IMGW data tab."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import polars as pl
import streamlit as st

from data_processing import ARCHIVAL_SOURCES, format_directory
from imgw_client import (
    add_date_column,
    apply_legend_columns,
    decode_text,
    download_bytes,
    extract_zip_entries,
    filter_by_station,
    list_directory,
    parse_info_legend,
    read_table_from_bytes,
)


def render_file_tab() -> tuple[Optional[pl.DataFrame], dict]:
    """Render the archival file-based data tab UI.

    Returns:
        A tuple of ``(DataFrame, metadata)`` when data has been fetched,
        or ``(None, {})`` while waiting for user input.  The *metadata* dict
        contains ``source_key`` and ``frequency``.
    """
    source_key = st.selectbox(
        "Rodzaj danych",
        options=list(ARCHIVAL_SOURCES.keys()),
        format_func=lambda key: ARCHIVAL_SOURCES[key].label,
        key="file_source_key",
    )
    source = ARCHIVAL_SOURCES[source_key]

    st.info("Źródło danych: Pliki archiwalne")

    col1, col2 = st.columns(2)
    with col1:
        frequency = st.selectbox(
            "Częstotliwość",
            options=("dobowe", "miesięczne", "surowe 10-min"),
            key="file_frequency",
        )
        station_name = st.text_input(
            "Nazwa stacji",
            help="Wpisz nazwę wodowskazu lub stacji synoptycznej.",
            key="file_station_name",
        )
    with col2:
        use_date_filter = st.checkbox("Filtruj po dacie", value=False, key="file_use_date_filter")
        date_range = None
        if use_date_filter:
            default_end = date.today()
            default_start = default_end - timedelta(days=30)
            date_range = st.date_input(
                "Zakres dat",
                value=(default_start, default_end),
                key="file_date_range",
            )

    data_url = st.text_input("URL danych", value=source.base_url, key="file_data_url")
    info_url = st.text_input("URL pliku info (legenda)", key="file_info_url")

    st.divider()

    legend_columns: list[str] = st.session_state.get("legend_columns", [])

    if info_url:
        if st.button("Pobierz legendę", key="file_btn_legend"):
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
    if st.button("Pokaż zawartość katalogu", key="file_btn_dir"):
        try:
            entries = list_directory(data_url)
            if entries:
                st.write(format_directory(entries))
            else:
                st.info("Brak widocznych wpisów w katalogu.")
        except RuntimeError as exc:
            st.error(str(exc))

    st.subheader("Pobieranie danych")
    if st.button("Pobierz dane", key="file_btn_fetch"):
        if not data_url:
            st.error("Podaj URL danych.")
            return None, {}

        with st.spinner("Pobieranie pliku..."):
            try:
                raw_bytes = download_bytes(data_url)
            except RuntimeError as exc:
                st.error(str(exc))
                return None, {}

        data_candidates = {"plik": raw_bytes}
        if raw_bytes[:4] == b"PK\x03\x04":
            with st.spinner("Rozpakowywanie archiwum..."):
                data_candidates = extract_zip_entries(raw_bytes)

        selected_name = st.selectbox(
            "Wybierz plik",
            options=list(data_candidates.keys()),
            key="file_selected_name",
        )
        if not selected_name:
            return None, {}

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

        return df, {"source_key": source_key, "frequency": frequency, "tab_id": "file"}

    return None, {}
