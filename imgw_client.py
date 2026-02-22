"""IMGW client utilities for downloading and parsing public datasets."""

from __future__ import annotations

import io
import re
import time
import zipfile
from dataclasses import dataclass
from typing import Iterable

import polars as pl
import requests

IMGW_BASE_URL = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/"
IMGW_API_BASE_URL = "https://danepubliczne.imgw.pl/api/data"
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_BACKOFF_MULTIPLIER = 2
ALLOWED_HOST = "danepubliczne.imgw.pl"


@dataclass(frozen=True)
class DirectoryEntry:
    name: str
    href: str
    is_dir: bool


def _validate_imgw_url(url: str) -> None:
    """Raise ValueError when *url* does not target the allowed IMGW host."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"URL scheme must be http or https, got: {parsed.scheme!r}")
    if parsed.netloc != ALLOWED_HOST:
        raise ValueError(f"URL host must be {ALLOWED_HOST!r}, got: {parsed.netloc!r}")


def download_bytes(url: str) -> bytes:
    """Download content from URL with retry and backoff."""
    _validate_imgw_url(url)
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last_error = exc
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_MULTIPLIER**attempt)
    raise RuntimeError(f"Failed to download {url}: {last_error}")


def list_directory(url: str) -> list[DirectoryEntry]:
    """Parse Apache-style directory listing into entries."""
    html = download_bytes(url).decode("utf-8", errors="ignore")
    entries: list[DirectoryEntry] = []
    for match in re.finditer(r"<a href=\"([^\"]+)\">([^<]+)</a>", html):
        href = match.group(1)
        name = match.group(2)
        if name in {"../", ".."}:
            continue
        is_dir = href.endswith("/")
        entries.append(DirectoryEntry(name=name.rstrip("/"), href=href, is_dir=is_dir))
    return entries


def extract_zip_entries(data: bytes) -> dict[str, bytes]:
    """Extract zip content into a filename -> bytes mapping."""
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        return {name: zf.read(name) for name in zf.namelist() if not name.endswith("/")}


def decode_text(data: bytes) -> str:
    """Decode text using common encodings for IMGW data files."""
    for encoding in ("utf-8", "cp1250", "latin1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("latin1", errors="replace")


def detect_delimiter(sample_line: str) -> str | None:
    """Detect CSV delimiter from a sample line."""
    for delimiter in (";", ",", "\t", "|"):
        if delimiter in sample_line:
            return delimiter
    return None


def read_table_from_bytes(data: bytes) -> pl.DataFrame:
    """Read a table from bytes using a best-effort delimiter detection."""
    text = decode_text(data)
    sample_line = next((line for line in text.splitlines() if line.strip()), "")
    delimiter = detect_delimiter(sample_line)
    data_stream = io.BytesIO(data)
    if delimiter:
        return pl.read_csv(
            data_stream,
            separator=delimiter,
            infer_schema_length=1000,
            truncate_ragged_lines=True,
            ignore_errors=True,
        )
    return pl.read_csv(
        data_stream,
        separator=" ",
        infer_schema_length=1000,
        truncate_ragged_lines=True,
        ignore_errors=True,
    )


def parse_info_legend(text: str) -> list[str]:
    """Extract column names from an IMGW info/legend file."""
    columns: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        cleaned = re.sub(r"\s+", " ", line)
        match = re.match(r"^([A-Za-z\u00C0-\u017F].*?)(?:\s+\d+(?:/\d+)?)?$", cleaned)
        if match:
            name = match.group(1).strip("- ")
            if name:
                columns.append(name)
    return columns


def apply_legend_columns(df: pl.DataFrame, legend_columns: list[str]) -> pl.DataFrame:
    """Apply legend column names if counts match."""
    if legend_columns and len(legend_columns) == len(df.columns):
        return df.rename({old: new for old, new in zip(df.columns, legend_columns)})
    return df


def normalize_name(name: str) -> str:
    """Normalize Polish column names for matching."""
    replacements = str.maketrans(
        {
            "ą": "a",
            "ć": "c",
            "ę": "e",
            "ł": "l",
            "ń": "n",
            "ó": "o",
            "ś": "s",
            "ż": "z",
            "ź": "z",
        }
    )
    return name.lower().translate(replacements).replace(" ", "")


def find_column(df: pl.DataFrame, candidates: Iterable[str]) -> str | None:
    """Find a column in the DataFrame matching any candidate label."""
    normalized = {normalize_name(col): col for col in df.columns}
    for candidate in candidates:
        key = normalize_name(candidate)
        if key in normalized:
            return normalized[key]
    return None


def add_date_column(df: pl.DataFrame) -> pl.DataFrame:
    """Add a `Data` column when year/month/day fields are present."""
    year_col = find_column(df, ["Rok", "Rok hydrologiczny"])
    month_col = find_column(df, ["Miesiac", "Miesiąc", "Miesiac kalendarzowy", "Miesiąc kalendarzowy"])
    day_col = find_column(df, ["Dzien", "Dzień"])
    if not year_col or not month_col:
        return df
    day_value = pl.col(day_col) if day_col else pl.lit(1)
    return df.with_columns(
        pl.date(
            pl.col(year_col).cast(pl.Int32),
            pl.col(month_col).cast(pl.Int32),
            day_value.cast(pl.Int32),
        ).alias("Data")
    )


def filter_by_station(df: pl.DataFrame, station_name: str, candidates: Iterable[str]) -> pl.DataFrame:
    """Filter rows by station name using a case-insensitive contains."""
    if not station_name:
        return df
    station_col = find_column(df, candidates)
    if not station_col:
        return df
    return df.filter(
        pl.col(station_col).cast(pl.Utf8).str.contains(station_name, literal=True, case_sensitive=False)
    )


def fetch_api_data(endpoint: str, format_type: str = "json", station_id: int | None = None, station_name: str | None = None) -> bytes:
    """
    Fetch data from IMGW API endpoints.
    
    Args:
        endpoint: API endpoint (e.g., 'synop', 'hydro', 'meteo')
        format_type: Output format ('json', 'xml', 'csv', 'html')
        station_id: Optional station ID for filtering
        station_name: Optional station name for filtering (without Polish diacritics)
    
    Returns:
        Response content as bytes
    """
    url = f"{IMGW_API_BASE_URL}/{endpoint}"
    
    if station_id is not None:
        url = f"{url}/id/{station_id}"
    elif station_name is not None:
        url = f"{url}/station/{station_name}"
    
    if format_type != "json":
        url = f"{url}/format/{format_type}"
    
    return download_bytes(url)


def parse_api_json_to_dataframe(json_bytes: bytes) -> pl.DataFrame:
    """
    Parse JSON response from IMGW API to Polars DataFrame.
    
    Args:
        json_bytes: JSON response from API as bytes
    
    Returns:
        Polars DataFrame with the data
    """
    import json
    
    text = decode_text(json_bytes)
    data = json.loads(text)
    
    if not data:
        return pl.DataFrame()
    
    # API returns a list of dictionaries
    if isinstance(data, list):
        return pl.DataFrame(data)
    
    # If single object, wrap in list
    if isinstance(data, dict):
        return pl.DataFrame([data])
    
    return pl.DataFrame()


def fetch_synop_data(station_id: int | None = None, station_name: str | None = None) -> pl.DataFrame:
    """
    Fetch current synoptic station data from IMGW API.
    
    Args:
        station_id: Optional station ID for filtering
        station_name: Optional station name for filtering (without Polish diacritics)
    
    Returns:
        DataFrame with synoptic data
    """
    json_bytes = fetch_api_data("synop", station_id=station_id, station_name=station_name)
    return parse_api_json_to_dataframe(json_bytes)


def fetch_hydro_data(station_id: int | None = None, station_name: str | None = None) -> pl.DataFrame:
    """
    Fetch current hydrological station data from IMGW API.
    
    Args:
        station_id: Optional station ID for filtering
        station_name: Optional station name for filtering (without Polish diacritics)
    
    Returns:
        DataFrame with hydrological data
    """
    json_bytes = fetch_api_data("hydro", station_id=station_id, station_name=station_name)
    return parse_api_json_to_dataframe(json_bytes)


def fetch_meteo_data(station_id: int | None = None, station_name: str | None = None) -> pl.DataFrame:
    """
    Fetch current meteorological station data from IMGW API.
    
    Args:
        station_id: Optional station ID for filtering
        station_name: Optional station name for filtering (without Polish diacritics)
    
    Returns:
        DataFrame with meteorological data
    """
    json_bytes = fetch_api_data("meteo", station_id=station_id, station_name=station_name)
    return parse_api_json_to_dataframe(json_bytes)

