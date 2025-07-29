# CEIDG Hurtownia

Projekt `ceidg-hurtownia` to potok danych (data pipeline) służący do pobierania, wzbogacania i przechowywania danych o firmach z publicznego API CEIDG. Aplikacja wykorzystuje architekturę producent-konsument do efektywnego zarządzania limitami zapytań API.

Link do dokumentu : https://www.icloud.com/pages/015wyA_rYr_RgFbPI3gv-L_Uw#Hurtownia_CEIDG

## Instalacja

1.  Sklonuj repozytorium:
    ```bash
    git clone git@github.com:michalbajdek/ceidg-hurtownia.git
    cd ceidg-hurtownia
    ```

2.  (Zalecane) Stwórz i aktywuj wirtualne środowisko:
    ```bash
    python -m venv venv
    source venv/bin/activate  # Na Windows: venv\Scripts\activate
    ```

3.  Zainstaluj wymagane pakiety:
    ```bash
    pip install -r requirements.txt
    ```

## Konfiguracja

1.  Skopiuj plik z wzorem konfiguracji:
    ```bash
    cp config.ini.example config.ini
    ```

2.  Otwórz plik `config.ini` i uzupełnij go swoimi danymi dostępowymi do bazy danych MySQL oraz tokenem JWT.

## Użycie

#### Krok 1: (Jednorazowo) Import danych z CSV

Jeśli posiadasz dane w pliku CSV (np. `example.csv`), umieść go w głównym folderze projektu i uruchom:
```bash
python src/utils/csv_to_mysql.py
```

#### Krok 2: Uruchomienie ciągłych procesów

Otwórz **dwa osobne okna terminala**.

-   W **pierwszym** terminalu uruchom **importera**:
    ```bash
    python src/importers/ceidg.py
    ```

-   W **drugim** terminalu uruchom **enrichera**:
    ```bash
    python src/enrichers/ceidg.py
    ```