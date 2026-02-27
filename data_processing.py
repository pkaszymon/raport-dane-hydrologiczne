"""Business logic for IMGW data processing and export."""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import Iterable

import polars as pl
import xlsxwriter

from imgw_client import IMGW_BASE_URL

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataSource:
    label: str
    base_url: str
    station_candidates: tuple[str, ...]


ARCHIVAL_SOURCES: dict[str, DataSource] = {
    "hydro_archival": DataSource(
        label="Dane hydrologiczne archiwalne",
        base_url=IMGW_BASE_URL,
        station_candidates=("Nazwa stacji", "Nazwa wodowskazu", "Wodowskaz"),
    ),
    "climate": DataSource(
        label="Dane klimatyczne archiwalne",
        base_url=f"{IMGW_BASE_URL}dane_meteorologiczne/",
        station_candidates=("Nazwa stacji", "Stacja", "Stacja synoptyczna"),
    ),
}

API_SOURCES: dict[str, DataSource] = {
    "hydro_api": DataSource(
        label="Dane hydrologiczne operacyjne (API)",
        base_url="",
        station_candidates=("stacja", "nazwa_stacji", "rzeka"),
    ),
    "synop_api": DataSource(
        label="Dane synoptyczne (API)",
        base_url="",
        station_candidates=("stacja", "nazwa_stacji"),
    ),
    "meteo_api": DataSource(
        label="Dane meteorologiczne (API)",
        base_url="",
        station_candidates=("stacja", "nazwa_stacji"),
    ),
}

DATA_SOURCES: dict[str, DataSource] = {**ARCHIVAL_SOURCES, **API_SOURCES}


def format_directory(entries: Iterable) -> list[str]:
    """Format directory entries into human-readable labels."""
    return [f"{'[DIR]' if entry.is_dir else '[PLIK]'} {entry.name}" for entry in entries]


def parse_directory_selection(label: str) -> str:
    """Strip directory/file prefix from a formatted entry label."""
    return label.replace("[DIR]", "").replace("[PLIK]", "").strip()


def chunk_dataframe(df: pl.DataFrame, max_rows: int) -> list[pl.DataFrame]:
    """Split a DataFrame into chunks of at most *max_rows* rows."""
    if len(df) <= max_rows:
        logger.debug("DataFrame fits in one chunk (%d rows)", len(df))
        return [df]
    chunks = [df.slice(offset, max_rows) for offset in range(0, len(df), max_rows)]
    logger.debug("Split DataFrame (%d rows) into %d chunks of max %d rows", len(df), len(chunks), max_rows)
    return chunks


_EXCEL_MAX_SHEET_NAME_LEN = 31  # Excel limits sheet names to 31 characters


def dataframe_to_excel_bytes(chunks: list[pl.DataFrame], sheet_prefix: str = "Dane") -> bytes:
    """Serialize a list of DataFrame chunks to an Excel workbook in memory."""
    logger.info("Exporting %d sheet(s) to Excel", len(chunks))
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    for index, chunk in enumerate(chunks, start=1):
        sheet_name = sheet_prefix if len(chunks) == 1 else f"{sheet_prefix}{index}"
        worksheet = workbook.add_worksheet(sheet_name[:_EXCEL_MAX_SHEET_NAME_LEN])
        worksheet.write_row(0, 0, chunk.columns)
        for row_index, row in enumerate(chunk.iter_rows(), start=1):
            worksheet.write_row(row_index, 0, row)
        logger.debug("Sheet '%s': wrote %d rows", sheet_name, len(chunk))
    workbook.close()
    output.seek(0)
    data = output.read()
    logger.debug("Excel workbook size: %d bytes", len(data))
    return data


def normalize_station_name(station_name: str) -> str:
    """Lower-case *station_name* and strip Polish diacritics for API queries."""
    replacements = str.maketrans({
        "ą": "a", "ć": "c", "ę": "e", "ł": "l", "ń": "n",
        "ó": "o", "ś": "s", "ż": "z", "ź": "z",
        "Ą": "A", "Ć": "C", "Ę": "E", "Ł": "L", "Ń": "N",
        "Ó": "O", "Ś": "S", "Ż": "Z", "Ź": "Z",
    })
    return station_name.lower().translate(replacements).replace(" ", "")


# Hydro API category definitions: (value_column, date_column, display_label)
HYDRO_API_CATEGORIES: list[tuple[str, str, str]] = [
    ("stan_wody", "stan_wody_data_pomiaru", "Stan wody"),
    ("temperatura_wody", "temperatura_wody_data_pomiaru", "Temperatura wody"),
    ("przeplyw", "przeplyw_data_pomiaru", "Przeplyw"),
    ("zjawisko_lodowe", "zjawisko_lodowe_data_pomiaru", "Zjawisko lodowe"),
    ("zjawisko_zarastania", "zjawisko_zarastania_data_pomiaru", "Zjawisko zarastania"),
]

# Station metadata columns to include in every category table
HYDRO_STATION_COLS = ["id_stacji", "stacja", "rzeka", "wojewodztwo"]

# Aggregation interval options: display label -> Polars duration string (None = no aggregation)
HYDRO_AGGREGATION_INTERVALS: dict[str, str | None] = {
    "Brak (surowe dane)": None,
    "Godzinowy": "1h",
    "Dzienny": "1d",
    "Tygodniowy": "1w",
    "Miesięczny": "1mo",
}


def split_hydro_api_data(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Split a hydro API DataFrame into per-category tables.

    Each returned DataFrame contains station metadata columns plus the
    measurement value and its timestamp.  Rows where the measurement value is
    null are removed.  Categories that have no non-null rows are omitted.

    Args:
        df: Raw hydro API DataFrame (one row per station snapshot).

    Returns:
        Ordered dict mapping display label to DataFrame for each non-empty
        category.
    """
    result: dict[str, pl.DataFrame] = {}
    station_cols = [c for c in HYDRO_STATION_COLS if c in df.columns]

    for value_col, date_col, label in HYDRO_API_CATEGORIES:
        if value_col not in df.columns:
            logger.debug("Hydro category '%s': value column '%s' not in DataFrame", label, value_col)
            continue

        keep_cols = station_cols + [c for c in [value_col, date_col] if c in df.columns]
        category_df = df.select(keep_cols).filter(pl.col(value_col).is_not_null())

        if len(category_df) > 0:
            result[label] = category_df
            logger.debug("Hydro category '%s': %d rows", label, len(category_df))
        else:
            logger.debug("Hydro category '%s': no non-null rows, skipping", label)

    return result


def aggregate_hydro_category(
    df: pl.DataFrame,
    date_col: str,
    value_col: str,
    interval: str,
) -> pl.DataFrame:
    """Aggregate a hydro category DataFrame by a time interval.

    The date column is truncated to the chosen interval and numeric values are
    averaged across all measurements within the same station + interval bucket.

    Args:
        df: Category DataFrame (output of :func:`split_hydro_api_data`).
        date_col: Name of the datetime string column.
        value_col: Name of the numeric value column.
        interval: Polars duration string, e.g. ``"1d"``, ``"1h"``.

    Returns:
        Aggregated DataFrame sorted by station and date.
    """
    if date_col not in df.columns or value_col not in df.columns:
        return df

    station_cols = [c for c in HYDRO_STATION_COLS if c in df.columns]

    df = df.with_columns(
        pl.col(date_col)
        .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
        .alias(date_col),
        pl.col(value_col).cast(pl.Float64, strict=False).alias(value_col),
    )

    df = df.with_columns(
        pl.col(date_col).dt.truncate(interval).alias(date_col)
    )

    sort_cols = station_cols + [date_col]
    df_agg = (
        df.group_by(sort_cols)
        .agg(pl.col(value_col).mean().alias(value_col))
        .sort(sort_cols)
    )

    logger.debug(
        "Aggregated '%s' by interval '%s': %d → %d rows",
        value_col,
        interval,
        len(df),
        len(df_agg),
    )
    return df_agg


def named_sheets_to_excel_bytes(sheets: dict[str, pl.DataFrame]) -> bytes:
    """Serialize multiple DataFrames to a single Excel workbook with named sheets.

    Args:
        sheets: Ordered dict mapping sheet name to DataFrame.

    Returns:
        Excel workbook as bytes.
    """
    logger.info("Exporting %d named sheet(s) to Excel", len(sheets))
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    for sheet_name, df in sheets.items():
        safe_name = sheet_name[:_EXCEL_MAX_SHEET_NAME_LEN]
        worksheet = workbook.add_worksheet(safe_name)
        worksheet.write_row(0, 0, df.columns)
        for row_index, row in enumerate(df.iter_rows(), start=1):
            worksheet.write_row(row_index, 0, row)
        logger.debug("Sheet '%s': wrote %d rows", safe_name, len(df))
    workbook.close()
    output.seek(0)
    data = output.read()
    logger.debug("Excel workbook size: %d bytes", len(data))
    return data
