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


def dataframe_to_excel_bytes(chunks: list[pl.DataFrame], sheet_prefix: str = "Dane") -> bytes:
    """Serialize a list of DataFrame chunks to an Excel workbook in memory."""
    logger.info("Exporting %d sheet(s) to Excel", len(chunks))
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    for index, chunk in enumerate(chunks, start=1):
        sheet_name = sheet_prefix if len(chunks) == 1 else f"{sheet_prefix}{index}"
        worksheet = workbook.add_worksheet(sheet_name[:31])
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
