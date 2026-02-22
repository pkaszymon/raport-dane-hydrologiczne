"""Streamlit UI for API-based IMGW data tab."""

from __future__ import annotations

import logging
from typing import Optional

import polars as pl
import streamlit as st

from data_processing import API_SOURCES, normalize_station_name
from imgw_client import fetch_hydro_data, fetch_meteo_data, fetch_synop_data

logger = logging.getLogger(__name__)


def render_api_tab() -> tuple[Optional[pl.DataFrame], dict]:
    """Render the API data tab UI.

    Returns:
        A tuple of ``(DataFrame, metadata)`` when data has been fetched,
        or ``(None, {})`` while waiting for user input.  The *metadata* dict
        contains ``source_key`` and ``frequency`` (always ``None`` for API).
    """
    source_key = st.selectbox(
        "Rodzaj danych",
        options=list(API_SOURCES.keys()),
        format_func=lambda key: API_SOURCES[key].label,
        key="api_source_key",
    )

    st.info("Źródło danych: API IMGW (aktualne dane operacyjne)")

    col1, col2 = st.columns(2)
    with col1:
        station_name = st.text_input(
            "Nazwa stacji (opcjonalnie)",
            help="Wpisz nazwę stacji bez polskich znaków (np. 'warszawa', 'krakow'). Pozostaw puste dla wszystkich stacji.",
            key="api_station_name",
        )
    with col2:
        station_id = st.number_input(
            "ID stacji (opcjonalnie)",
            min_value=0,
            value=0,
            help="Podaj ID stacji jeśli jest znane. 0 = pobierz wszystkie stacje.",
            key="api_station_id",
        )

    resolved_station_id: int | None = int(station_id) if station_id != 0 else None
    resolved_station_name: str | None = normalize_station_name(station_name) if station_name else None

    st.subheader("Pobieranie danych")
    if st.button("Pobierz dane", key="api_btn_fetch"):
        logger.info(
            "Fetching API data: source=%s, station_id=%r, station_name=%r",
            source_key,
            resolved_station_id,
            resolved_station_name,
        )
        with st.spinner("Pobieranie danych z API IMGW..."):
            try:
                if source_key == "hydro_api":
                    df = fetch_hydro_data(
                        station_id=resolved_station_id,
                        station_name=resolved_station_name,
                    )
                elif source_key == "synop_api":
                    df = fetch_synop_data(
                        station_id=resolved_station_id,
                        station_name=resolved_station_name,
                    )
                elif source_key == "meteo_api":
                    df = fetch_meteo_data(
                        station_id=resolved_station_id,
                        station_name=resolved_station_name,
                    )
                else:
                    logger.error("Unknown API source key: %s", source_key)
                    st.error(f"Nieznane źródło API: {source_key}")
                    return None, {}

                if df is None or len(df) == 0:
                    logger.warning("API returned no data for source=%s", source_key)
                    st.warning("Brak danych z API. Sprawdź parametry zapytania.")
                    return None, {}

            except RuntimeError as exc:
                logger.error("API fetch failed for source=%s: %s", source_key, exc)
                st.error(f"Błąd podczas pobierania danych z API: {exc}")
                return None, {}

        logger.info("API data ready: %d rows × %d columns", len(df), len(df.columns))
        return df, {"source_key": source_key, "frequency": None, "tab_id": "api"}

    return None, {}
