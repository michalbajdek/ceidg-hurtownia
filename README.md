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

## Krok 2: Przygotowanie Bazy Danych

Zanim uruchomisz skrypty, musisz stworzyć odpowiednią strukturę tabel w swojej bazie danych.

1.  Zaloguj się do swojej bazy MySQL.
2.  Wykonaj poniższy skrypt SQL, aby stworzyć tabele `companies` i `importer_state`.

```sql
-- Tworzy tabelę na firmy, jeśli nie istnieje.
CREATE TABLE IF NOT EXISTS `companies` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `ceidg_id` VARCHAR(255) NOT NULL,
  `nazwa` TEXT,
  `status_dzialalnosci` VARCHAR(100),
  `data_rozpoczecia` DATE,
  `nip` VARCHAR(20),
  `regon` VARCHAR(20),
  `wzbogacono_dnia` DATETIME DEFAULT NULL,
  `imie_wlasciciela` VARCHAR(255) DEFAULT NULL,
  `nazwisko_wlasciciela` VARCHAR(255) DEFAULT NULL,
  `data_zawieszenia` DATE DEFAULT NULL,
  `data_zakonczenia` DATE DEFAULT NULL,
  `data_wykreslenia` DATE DEFAULT NULL,
  `data_wznowienia` DATE DEFAULT NULL,
  `numer_statusu` INT DEFAULT NULL,
  `telefon` VARCHAR(255) DEFAULT NULL,
  `email` VARCHAR(255) DEFAULT NULL,
  `www` VARCHAR(255) DEFAULT NULL,
  `adres_doreczen_elektronicznych` VARCHAR(255) DEFAULT NULL,
  `inna_forma_kontaktu` TEXT DEFAULT NULL,
  `wspolnosc_majatkowa` VARCHAR(50) DEFAULT NULL,
  `pkd_glowny` VARCHAR(10) DEFAULT NULL,
  `adres_dzialalnosci` JSON DEFAULT NULL,
  `adres_korespondencyjny` JSON DEFAULT NULL,
  `adresy_dodatkowe` JSON DEFAULT NULL,
  `pkd_wszystkie` JSON DEFAULT NULL,
  `spolki_cywilne` JSON DEFAULT NULL,
  `obywatelstwa` JSON DEFAULT NULL,
  `upadlosc_postepowanie` JSON DEFAULT NULL,
  `zakazy` JSON DEFAULT NULL,
  `zarzadca_sukcesyjny` JSON DEFAULT NULL,
  UNIQUE KEY `ceidg_id_unique` (`ceidg_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tworzy tabelę do przechowywania stanu importera
CREATE TABLE IF NOT EXISTS `importer_state` (
  `setting_key` VARCHAR(255) PRIMARY KEY,
  `setting_value` TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Ustawia początkowy adres URL dla importera.
INSERT IGNORE INTO `importer_state` (setting_key, setting_value)
VALUES ('next_page_url', '[https://dane.biznes.gov.pl/api/ceidg/v2/firmy](https://dane.biznes.gov.pl/api/ceidg/v2/firmy)');
```

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