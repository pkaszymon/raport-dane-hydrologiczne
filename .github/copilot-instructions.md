# Copilot Repository Instructions

## Project Overview

This is a Streamlit web application focused on hydrological (I and II) and climatological (III) data from IMGW public datasets. The application provides an interface for downloading station data and exporting it to Excel with advanced filtering and aggregation capabilities.

### Technologies
- **Python 3.13+**: Core programming language
- **Streamlit 1.51.0+**: Web application framework
- **Polars 1.35.2+**: Fast DataFrame library for data processing
- **Requests 2.32.5+**: HTTP library for API calls
- **openpyxl 3.1.5+** & **xlsxwriter 3.2.9+**: Excel file handling

### Key Features
- Data fetching from IMGW public datasets:
  - **Archival data**: Historical data from file-based sources (hydro, climate)
  - **Operational data**: Current data from IMGW API (hydro, synoptic, meteorological)
- Filtering by date range and station names (water gauge for hydro, synoptic station for climate)
- Hydrological output as daily, monthly, and raw 10-minute data with chunked export
- Column names mapped from the legend included in dataset info files (archival data)
- Progress tracking and data caching
- Excel export with multiple output options
- Polish language UI
- Support for both file downloads and real-time API data fetching

## Build & Run

### Installation
```bash
# Install dependencies
pip install -r requirements.txt
```

### Running the Application
```bash
# Start the Streamlit app
streamlit run streamlit_app.py

# The app will be available at http://localhost:8501
```

### Development with DevContainer
This project includes a devcontainer configuration for GitHub Codespaces:
- Uses Python 3.13 on Debian Bullseye (DevContainer; code requires 3.9+ minimum)
- Automatically installs requirements on container creation
- Launches Streamlit server on port 8501 after attach

## Key Directories & File Structure

```
.
├── .github/               # GitHub configuration files
│   ├── CODEOWNERS        # Code ownership configuration
│   └── copilot-instructions.md  # This file
├── .devcontainer/        # DevContainer configuration for Codespaces
├── streamlit_app.py      # Main Streamlit application entry point
├── imgw_client.py        # IMGW API and data processing client module
├── requirements.txt      # Python package dependencies
├── README.md            # Project documentation
├── LICENSE              # Apache License 2.0
└── .gitignore           # Git ignore patterns
```

### Important Files
- **`streamlit_app.py`**: Main application UI and orchestration logic
  - Contains Streamlit interface setup
  - Handles user input, data fetching coordination, and Excel export
  - Implements caching and progress visualization
  
- **`imgw_client.py`**: IMGW API client and data processing utilities
  - Functions for fetching data from IMGW REST API (synop, hydro, meteo)
  - Functions for downloading and parsing archival file-based data
  - Retry logic with exponential backoff
  - Data parsing utilities (CSV, ZIP archives)
  - Legend parsing and column mapping
  - Configuration constants (timeouts, retry settings, etc.)

## Coding Standards

### General Guidelines
- Follow PEP 8 Python style guidelines
- Use type hints for function parameters and return values
- Write docstrings for modules, classes, and functions
- Keep functions focused and single-purpose
- Use descriptive variable names

### Code Organization
- Organize imports in the standard order: standard library, third-party, local
- Group related functionality together
- Use constants for configuration values (defined at module level)
- Separate business logic from UI code

### Documentation
- Include module-level docstrings explaining purpose
- Document function parameters, return values, and exceptions
- Add inline comments for complex logic only
- Keep README.md updated with feature changes

### Error Handling
- Use try-except blocks for expected errors (network, API issues)
- Implement retry logic with exponential backoff for transient failures
- Log errors with appropriate severity levels
- Provide user-friendly error messages in the UI

### Data Processing
- Use Polars for efficient DataFrame operations
- Prefer lazy evaluation where possible
- Handle timezone conversions explicitly (UTC)
- Validate data shapes and types before processing
- Map output column names from the dataset legend stored in info files
- Preserve special missing-data codes in hydro monthly extremes:
  - `9999` for water level, `99999.999` for flow, `99.9` for water temperature
- Preserve the hydro monthly extreme indicator values: `1` minimum, `2` mean, `3` maximum
- Preserve climate status fields for each measurement

## Testing Approach

Currently, this project does not have automated tests. When adding tests in the future:
- Place test files in a `tests/` directory
- Name test files as `test_*.py`
- Use pytest as the testing framework
- Test API functions with mocked responses
- Test data transformation logic with sample datasets
- Validate Excel export functionality

## Acceptance Criteria for Contributions

When making changes to this repository:

1. **Functionality**: Changes must maintain existing functionality unless intentionally removing/changing features
2. **Data Integrity**: Ensure data fetching and transformation logic preserves accuracy
3. **Error Handling**: Add appropriate error handling for new code paths
4. **Documentation**: Update README.md and code comments for significant changes
5. **Dependencies**: Only add new dependencies if absolutely necessary; document why in PR
6. **UI/UX**: Keep the Polish language interface consistent
7. **Performance**: Consider impact on API rate limits and memory usage for large datasets

## Boundaries & Constraints

### Do Not Modify
- **`.streamlit/secrets.toml`**: Never commit this file (it's in .gitignore for security)
- **`.github/CODEOWNERS`**: Owned by @streamlit/community-cloud
- **License file**: Apache License 2.0 should remain unchanged

### Data Sources and Constraints
- **IMGW API endpoints** (operational/current data):
  - Synoptic data: https://danepubliczne.imgw.pl/api/data/synop
  - Hydrological data: https://danepubliczne.imgw.pl/api/data/hydro
  - Meteorological data: https://danepubliczne.imgw.pl/api/data/meteo
  - Supports JSON, XML, CSV, HTML formats
  - Filtering by station ID or station name (without Polish diacritics)
- **IMGW file-based sources** (archival data):
  - Public data base: https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/
  - Contains historical hydro and climate data in ZIP/CSV format
- Respect server rate limits and use retry logic with backoff
- Chunk raw 10-minute hydrological data for export to avoid oversized Excel files
- Default timeout is 60 seconds per request

### Security Considerations
- Never commit API keys, credentials, or secrets
- Use environment variables or Streamlit secrets for sensitive data
- Validate and sanitize user inputs before processing
- Be cautious with file system operations

### Performance Guidelines
- Cache API responses to minimize redundant calls
- Process data in batches for large date ranges
- Use Polars for efficient DataFrame operations
- Monitor memory usage with large datasets

## Data Configuration

Key configuration values in `imgw_client.py`:
- `IMGW_BASE_URL`: Base endpoint for IMGW public file data (https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/)
- `IMGW_API_BASE_URL`: Base endpoint for IMGW API (https://danepubliczne.imgw.pl/api/data)
- `REQUEST_TIMEOUT`: 60 seconds
- `MAX_RETRIES`: 3 attempts
- `RETRY_BACKOFF_MULTIPLIER`: 2 (exponential backoff)

### Available API Endpoints
- `fetch_synop_data()`: Get current synoptic station data
- `fetch_hydro_data()`: Get current hydrological station data
- `fetch_meteo_data()`: Get current meteorological station data
- All functions support filtering by station_id or station_name

## Polish Language Context

The UI is in Polish. Key terminology:
- "Pobierz dane" = Fetch Data
- "Wodowskaz" = Water gauge
- "Stacja synoptyczna" = Synoptic station
- "Data" = Date
- "Eksportuj do Excel" = Export to Excel

When modifying UI text, maintain Polish language consistency and ensure proper character encoding (UTF-8).