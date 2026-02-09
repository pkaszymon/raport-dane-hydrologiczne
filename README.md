# Raport dane hydrologiczne

Aplikacja Streamlit do pobierania i eksportu danych z IMGW (Instytut Meteorologii i Gospodarki Wodnej) do formatu Excel.

## Funkcje

Aplikacja umożliwia pobieranie dwóch typów danych:

### 1. Dane operacyjne (aktualne) - API IMGW
- **Dane hydrologiczne** - aktualne dane ze stacji hydrologicznych (wodowskazów)
- **Dane synoptyczne** - aktualne dane ze stacji synoptycznych
- **Dane meteorologiczne** - aktualne dane meteorologiczne

Dane operacyjne są pobierane bezpośrednio z API IMGW i zawierają najnowsze pomiary.

### 2. Dane archiwalne - pliki IMGW
- **Dane hydrologiczne archiwalne** - historyczne dane hydrologiczne (dobowe, miesięczne, surowe 10-min)
- **Dane klimatyczne archiwalne** - historyczne dane klimatyczne

Dane archiwalne są pobierane z publicznych zasobów plikowych IMGW.

## Wykorzystywane źródła danych

### API IMGW (dane operacyjne)
- Dane synoptyczne: `https://danepubliczne.imgw.pl/api/data/synop`
- Dane hydrologiczne: `https://danepubliczne.imgw.pl/api/data/hydro`
- Dane meteorologiczne: `https://danepubliczne.imgw.pl/api/data/meteo`

### Zasoby plikowe (dane archiwalne)
- Dane pomiarowo-obserwacyjne: `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/`

## Instalacja

```bash
pip install -r requirements.txt
```

## Uruchomienie

```bash
streamlit run streamlit_app.py
```

Aplikacja będzie dostępna pod adresem `http://localhost:8501`

## Użycie

### Pobieranie danych operacyjnych (API)

1. W menu bocznym wybierz rodzaj danych:
   - "Dane hydrologiczne operacyjne (API)"
   - "Dane synoptyczne (API)"
   - "Dane meteorologiczne (API)"

2. Opcjonalnie podaj nazwę stacji (bez polskich znaków) lub ID stacji
   - Pozostaw puste, aby pobrać dane ze wszystkich stacji

3. Kliknij "Pobierz dane"

4. Eksportuj dane do Excel

### Pobieranie danych archiwalnych (pliki)

1. W menu bocznym wybierz rodzaj danych:
   - "Dane hydrologiczne archiwalne"
   - "Dane klimatyczne archiwalne"

2. Wybierz częstotliwość: dobowe, miesięczne lub surowe 10-min

3. Podaj nazwę stacji (opcjonalnie)

4. Podaj URL do pliku danych i pliku info (legenda)

5. Opcjonalnie filtruj po zakresie dat

6. Kliknij "Pobierz dane"

7. Wybierz plik z archiwum (jeśli to ZIP)

8. Eksportuj dane do Excel

## Funkcje eksportu

- Automatyczne dzielenie dużych zbiorów danych na wiele arkuszy Excel
- Konfigurowalny limit wierszy na arkusz (50 000 - 500 000)
- Mapowanie nazw kolumn z legendy (dla danych archiwalnych)
- Filtrowanie po nazwie stacji i zakresie dat

## Wymagania

- Python 3.9+
- Streamlit 1.51.0+
- Polars 1.35.2+
- Requests 2.32.5+
- openpyxl 3.1.5+
- xlsxwriter 3.2.9+

## Licencja

Apache License 2.0
