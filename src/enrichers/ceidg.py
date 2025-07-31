import json
import logging
import os
import sys
import time
from datetime import datetime
import requests
from ..db_connector import get_db_connection
from ..config import load_config

# Wczytujemy konfigurację i pobieramy token
config = load_config()
JWT_TOKEN = config['api'].get('jwt_token')

LOG_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'enricher.log')
LOCK_FILE = os.path.join(os.path.dirname(__file__), 'enricher.lock')
SLEEP_INTERVAL = 3.6
RECORDS_TO_FETCH_PER_BATCH = 10
API_DETAIL_URL_TEMPLATE = 'https://dane.biznes.gov.pl/api/ceidg/v2/firma/{ceidg_id}'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - ENRICHER - %(levelname)s - %(message)s', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)])

def to_json_or_null(data):
    if not data: return None
    # Psycopg2 może obsługiwać słowniki bezpośrednio przy kolumnach JSONB,
    # ale przekazanie stringa JSON jest bezpieczniejsze i bardziej uniwersalne.
    return json.dumps(data)

def main():
    logging.info("--- Uruchomiono skrypt wzbogacający dla PostgreSQL ---")
    conn = get_db_connection()
    if not conn: return
    
    cursor = conn.cursor() # Domyślny kursor (zwraca krotki)
    
    while True:
        try:
            # Zmieniony kursor wymaga odwołania po indeksie
            cursor.execute("SELECT ceidg_id FROM companies WHERE wzbogacono_dnia IS NULL LIMIT %s", (RECORDS_TO_FETCH_PER_BATCH,))
            records_to_process = cursor.fetchall()
            
            if not records_to_process:
                logging.info("Brak nowych rekordów do wzbogacenia. Czekam 1 minutę.")
                time.sleep(60)
                continue
                
            logging.info(f"Pobrano {len(records_to_process)} rekordów do wzbogacenia.")
            for record in records_to_process:
                ceidg_id = record[0] # Odwołanie po indeksie
                logging.info(f"Wzbogacam rekord o ID: {ceidg_id}")
                api_url = API_DETAIL_URL_TEMPLATE.format(ceidg_id=ceidg_id)
                headers = {'Authorization': f'Bearer {JWT_TOKEN}'}
                
                try:
                    response = requests.get(api_url, headers=headers, timeout=20)
                    if response.status_code == 404:
                        logging.warning(f"Rekord o ID {ceidg_id} nie znaleziony w API (404). Oznaczam.")
                        cursor.execute("UPDATE companies SET wzbogacono_dnia = %s WHERE ceidg_id = %s", (datetime.now(), ceidg_id))
                        conn.commit()
                        continue
                    if response.status_code == 429:
                        logging.warning("Kod 429 - przekroczono limit. Czekam 180 sekund.")
                        time.sleep(180)
                        break
                        
                    response.raise_for_status()
                    details = response.json().get('firma', [{}])[0]
                    wlasciciel = details.get('wlasciciel', {})
                    
                    update_query = """
                        UPDATE companies SET
                        imie_wlasciciela=%s, nazwisko_wlasciciela=%s, data_zawieszenia=%s, data_zakonczenia=%s, 
                        data_wykreslenia=%s, data_wznowienia=%s, numer_statusu=%s, telefon=%s, email=%s, www=%s, 
                        adres_doreczen_elektronicznych=%s, inna_forma_kontaktu=%s, wspolnosc_majatkowa=%s, 
                        pkd_glowny=%s, adres_dzialalnosci=%s, adres_korespondencyjny=%s, adresy_dodatkowe=%s, 
                        pkd_wszystkie=%s, spolki_cywilne=%s, obywatelstwa=%s, upadlosc_postepowanie=%s, 
                        zakazy=%s, zarzadca_sukcesyjny=%s, wzbogacono_dnia=%s 
                        WHERE ceidg_id = %s
                    """
                    update_data = (
                        wlasciciel.get('imie'), wlasciciel.get('nazwisko'), details.get('dataZawieszenia'), details.get('dataZakonczenia'),
                        details.get('dataWykreslenia'), details.get('dataWznowienia'), details.get('numerStatusu'), details.get('telefon'),
                        details.get('email'), details.get('www'), details.get('adresDoreczenElektronicznych'), details.get('innaFormaKonaktu'),
                        str(details.get('wspolnoscMajatkowa')), details.get('pkdGlowny'), to_json_or_null(details.get('adresDzialalnosci')),
                        to_json_or_null(details.get('adresKorespondencyjny')), to_json_or_null(details.get('adresyDzialanosciDodatkowe')),
                        to_json_or_null(details.get('pkd')), to_json_or_null(details.get('spolki')), to_json_or_null(details.get('obywatelstwa')),
                        to_json_or_null(details.get('upadlosc')), to_json_or_null(details.get('zakazy')), 
                        to_json_or_null(details.get('zarzadcaSukcesyjny')), datetime.now(), ceidg_id
                    )
                    
                    cursor.execute(update_query, update_data)
                    conn.commit()
                    logging.info(f"Pomyślnie zaktualizowano (KOMPLET) rekord {ceidg_id}.")
                except requests.exceptions.RequestException as e:
                    logging.error(f"Błąd zapytania do API dla ID {ceidg_id}: {e}")
                time.sleep(SLEEP_INTERVAL)
                
        except Exception as e:
            logging.error(f"Nieoczekiwany błąd: {e}", exc_info=True)
            if conn: conn.close()
            logging.info("Próbuję połączyć ponownie za 10s...")
            time.sleep(10)
            conn = get_db_connection()
            if conn: cursor = conn.cursor()
            else: break
            
    if conn: conn.close()

if __name__ == "__main__":
    if os.path.exists(LOCK_FILE):
        logging.error("Inna instancja skryptu wzbogacającego już działa. Zakańczam.")
        exit()
    try:
        with open(LOCK_FILE, 'w') as f: f.write(str(os.getpid()))
        main()
    finally:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logging.info("Plik blokady enrichera usunięty.")
