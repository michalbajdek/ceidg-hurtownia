import csv
import logging
import sys
import mysql.connector
from tqdm import tqdm
# Zmieniony import - odwołujemy się do folderu nadrzędnego (src)
from ..config import db_config

# Nazwa pliku CSV do zaimportowania
CSV_FILE_NAME = 'example.csv'

# Reszta kodu jest identyczna jak w poprzedniej odpowiedzi...
logging.basicConfig(level=logging.INFO, format='%(asctime)s - CSV_IMPORTER - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])

def main():
    logging.info(f"--- Rozpoczynam import danych z pliku {CSV_FILE_NAME} ---")
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        logging.info("Pomyślnie połączono z bazą danych MySQL.")
    except mysql.connector.Error as err:
        logging.error(f"Błąd połączenia z bazą MySQL: {err}")
        return
    try:
        with open(CSV_FILE_NAME, mode='r', encoding='utf-8') as csvfile:
            total_lines = sum(1 for row in csvfile) - 1
            csvfile.seek(0)
            csv_reader = csv.DictReader(csvfile)
            insert_query = """INSERT INTO companies (ceidg_id, nazwa, status_dzialalnosci, data_rozpoczecia, nip, regon) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE nazwa=VALUES(nazwa), status_dzialalnosci=VALUES(status_dzialalnosci)"""
            logging.info(f"Znaleziono {total_lines} rekordów. Zaczynam...")
            for row in tqdm(csv_reader, total=total_lines, desc="Importowanie CSV"):
                data_rozp = row['data_rozpoczecia'] if row['data_rozpoczecia'] else None
                values = (row['ceidg_id'], row['nazwa'], row['status_dzialalnosci'], data_rozp, row['nip'], row['regon'])
                cursor.execute(insert_query, values)
            conn.commit()
            logging.info(f"Import zakończony! Przetworzono {cursor.rowcount} rekordów.")
    except FileNotFoundError:
        logging.error(f"BŁĄD: Nie znaleziono pliku '{CSV_FILE_NAME}'.")
    except Exception as e:
        logging.error(f"Wystąpił nieoczekiwany błąd: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            logging.info("Połączenie z bazą danych zostało zamknięte.")

if __name__ == "__main__":
    main()