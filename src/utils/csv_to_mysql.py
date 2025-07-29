import csv
import logging
import sys
from tqdm import tqdm

from ..db_connector import get_db_connection

# Nazwa pliku CSV do zaimportowania
CSV_FILE_NAME = 'example.csv'

# ROZMIAR PARTII: Co tyle rekordów będziemy zapisywać zmiany w bazie.
BATCH_SIZE = 2000

logging.basicConfig(level=logging.INFO, format='%(asctime)s - CSV_IMPORTER - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])

def main():
    logging.info(f"--- Rozpoczynam import danych z pliku {CSV_FILE_NAME} ---")
    
    conn = get_db_connection()
    if not conn:
        logging.error("Nie udało się połączyć z bazą danych. Anulowano.")
        return
        
    cursor = conn.cursor()
    record_counter = 0
    
    try:
        with open(CSV_FILE_NAME, mode='r', encoding='utf-8') as csvfile:
            total_lines = sum(1 for row in csvfile) - 1
            csvfile.seek(0)
            csv_reader = csv.DictReader(csvfile)
            insert_query = """INSERT INTO companies (ceidg_id, nazwa, status_dzialalnosci, data_rozpoczecia, nip, regon) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE nazwa=VALUES(nazwa), status_dzialalnosci=VALUES(status_dzialalnosci)"""
            
            logging.info(f"Znaleziono {total_lines} rekordów do zaimportowania. Zaczynam...")

            for row in tqdm(csv_reader, total=total_lines, desc="Importowanie CSV"):
                data_rozp = row['data_rozpoczecia'] if row['data_rozpoczecia'] else None
                values = (row['ceidg_id'], row['nazwa'], row['status_dzialalnosci'], data_rozp, row['nip'], row['regon'])
                cursor.execute(insert_query, values)
                record_counter += 1
                
                # --- NOWA LOGIKA: ZATWIERDZANIE PARTIAMI ---
                if record_counter % BATCH_SIZE == 0:
                    conn.commit()
                    logging.info(f" -> Zatwierdzono partię {BATCH_SIZE} rekordów. Łącznie: {record_counter}")
            
            # --- Zawsze zatwierdź ostatnią, niepełną partię na końcu ---
            conn.commit()
            logging.info(f" -> Zatwierdzono ostatnią partię. Całkowita liczba przetworzonych rekordów: {record_counter}")
            logging.info("Import zakończony pomyślnie!")

    except FileNotFoundError:
        logging.error(f"BŁĄD: Nie znaleziono pliku '{CSV_FILE_NAME}'.")
    except Exception as e:
        logging.error(f"Wystąpił nieoczekiwany błąd: {e}")
        conn.rollback() # W razie błędu, wycofaj bieżącą partię
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            logging.info("Połączenie z bazą danych zostało zamknięte.")

if __name__ == "__main__":
    main()