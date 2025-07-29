import logging
import os
import sys
import time
import requests

from ..db_connector import get_db_connection
from ..config import JWT_TOKEN

LOG_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'importer.log')
LOCK_FILE = os.path.join(os.path.dirname(__file__), 'importer.lock')
SLEEP_INTERVAL = 4

logging.basicConfig(level=logging.INFO, format='%(asctime)s - IMPORTER - %(levelname)s - %(message)s', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)])

def main():
    logging.info("--- Uruchomiono importera ---")
    conn = get_db_connection()
    if not conn: return
    
    cursor = conn.cursor()
    
    while True:
        try:
            cursor.execute("SELECT setting_value FROM importer_state WHERE setting_key = 'next_page_url'")
            row = cursor.fetchone()
            next_url = row[0] if row and row[0] else None
            
            if not next_url:
                logging.info("Wszystkie strony zostały przetworzone. Kończę pracę.")
                break
                
            logging.info(f"Pobieram URL: {next_url[:150]}...")
            headers = {'Authorization': f'Bearer {JWT_TOKEN}'}
            response = requests.get(next_url, headers=headers, timeout=30)
            
            if response.status_code == 429:
                logging.warning("Kod 429 - przekroczono limit. Czekam 180 sekund.")
                time.sleep(180)
                continue
            response.raise_for_status()
            
            data = response.json()
            firmy = data.get('firmy', [])
            logging.info(f"Znaleziono {len(firmy)} firm.")
            
            if firmy:
                insert_query = "INSERT INTO companies (ceidg_id, nazwa, status_dzialalnosci, data_rozpoczecia, nip, regon) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE nazwa=VALUES(nazwa), status_dzialalnosci=VALUES(status_dzialalnosci)"
                rows_to_insert = [(f.get('id'), f.get('nazwa'), f.get('status'), f.get('dataRozpoczecia') or None, f.get('wlasciciel', {}).get('nip'), f.get('wlasciciel', {}).get('regon')) for f in firmy]
                cursor.executemany(insert_query, rows_to_insert)
                conn.commit()
                logging.info(f"Przetworzono {cursor.rowcount} rekordów.")
                
            new_next_url = data.get('links', {}).get('next')
            cursor.execute("UPDATE importer_state SET setting_value = %s WHERE setting_key = 'next_page_url'", (new_next_url,))
            conn.commit()
            
            if not new_next_url:
                logging.info("To była ostatnia strona.")
                continue
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Błąd zapytania do API: {e}. Czekam minutę.")
            time.sleep(60)
            continue
        except Exception as e:
            logging.error(f"Nieoczekiwany błąd: {e}", exc_info=True)
            conn.close()
            logging.info("Próbuję połączyć ponownie za 10s...")
            time.sleep(10)
            conn = get_db_connection()
            if conn: cursor = conn.cursor()
            else: break
            
        logging.info(f"Czekam {SLEEP_INTERVAL} sekund...")
        time.sleep(SLEEP_INTERVAL)
        
    conn.close()

if __name__ == "__main__":
    if os.path.exists(LOCK_FILE):
        logging.error("Inna instancja importera już działa. Zakańczam.")
        exit()
    try:
        with open(LOCK_FILE, 'w') as f: f.write(str(os.getpid()))
        main()
    finally:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logging.info("Plik blokady importera usunięty.")