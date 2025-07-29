import json
import logging
import os
import sys
import time
from datetime import datetime
import requests
import mysql.connector
# Zmieniony import
from ..config import db_config, JWT_TOKEN

LOG_FILE = os.path.join(os.path.dirname(__file__), 'enricher.log')
# Reszta kodu jest identyczna jak w poprzedniej odpowiedzi...
LOCK_FILE = os.path.join(os.path.dirname(__file__), 'enricher.lock')
SLEEP_INTERVAL = 3.6
RECORDS_TO_FETCH_PER_BATCH = 10
API_DETAIL_URL_TEMPLATE = 'https://dane.biznes.gov.pl/api/ceidg/v2/firma/{ceidg_id}'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - ENRICHER - %(levelname)s - %(message)s', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)])

def get_db_connection():
    try:
        return mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        logging.error(f"Błąd połączenia z bazą MySQL: {err}")
        return None

def to_json_or_null(data):
    if not data: return None
    return json.dumps(data, ensure_ascii=False)

def main():
    logging.info("--- Uruchomiono KOMPLETNY skrypt wzbogacający ---")
    conn = get_db_connection()
    if not conn: return
    cursor = conn.cursor(dictionary=True)
    while True:
        try:
            cursor.execute("SELECT ceidg_id FROM companies WHERE wzbogacono_dnia IS NULL LIMIT %s", (RECORDS_TO_FETCH_PER_BATCH,))
            records_to_process = cursor.fetchall()
            if not records_to_process:
                logging.info("Brak nowych rekordów do wzbogacenia. Czekam 1 minutę.")
                time.sleep(60)
                continue
            logging.info(f"Pobrano {len(records_to_process)} rekordów do wzbogacenia.")
            for record in records_to_process:
                ceidg_id = record['ceidg_id']
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
                    update_data = {'ceidg_id': ceidg_id, 'wzbogacono_dnia': datetime.now(), 'imie_wlasciciela': wlasciciel.get('imie'), 'nazwisko_wlasciciela': wlasciciel.get('nazwisko'), 'data_zawieszenia': details.get('dataZawieszenia'), 'data_zakonczenia': details.get('dataZakonczenia'), 'data_wykreslenia': details.get('dataWykreslenia'), 'data_wznowienia': details.get('dataWznowienia'), 'numer_statusu': details.get('numerStatusu'), 'telefon': details.get('telefon'), 'email': details.get('email'), 'www': details.get('www'), 'adres_doreczen_elektronicznych': details.get('adresDoreczenElektronicznych'), 'inna_forma_kontaktu': details.get('innaFormaKonaktu'), 'wspolnosc_majatkowa': str(details.get('wspolnoscMajatkowa')), 'pkd_glowny': details.get('pkdGlowny'), 'adres_dzialalnosci': to_json_or_null(details.get('adresDzialalnosci')), 'adres_korespondencyjny': to_json_or_null(details.get('adresKorespondencyjny')), 'adresy_dodatkowe': to_json_or_null(details.get('adresyDzialanosciDodatkowe')), 'pkd_wszystkie': to_json_or_null(details.get('pkd')), 'spolki_cywilne': to_json_or_null(details.get('spolki')), 'obywatelstwa': to_json_or_null(details.get('obywatelstwa')), 'upadlosc_postepowanie': to_json_or_null(details.get('upadlosc')), 'zakazy': to_json_or_null(details.get('zakazy')), 'zarzadca_sukcesyjny': to_json_or_null(details.get('zarzadcaSukcesyjny'))}
                    update_query = "UPDATE companies SET imie_wlasciciela=%(imie_wlasciciela)s, nazwisko_wlasciciela=%(nazwisko_wlasciciela)s, data_zawieszenia=%(data_zawieszenia)s, data_zakonczenia=%(data_zakonczenia)s, data_wykreslenia=%(data_wykreslenia)s, data_wznowienia=%(data_wznowienia)s, numer_statusu=%(numer_statusu)s, telefon=%(telefon)s, email=%(email)s, www=%(www)s, adres_doreczen_elektronicznych=%(adres_doreczen_elektronicznych)s, inna_forma_kontaktu=%(inna_forma_kontaktu)s, wspolnosc_majatkowa=%(wspolnosc_majatkowa)s, pkd_glowny=%(pkd_glowny)s, adres_dzialalnosci=%(adres_dzialalnosci)s, adres_korespondencyjny=%(adres_korespondencyjny)s, adresy_dodatkowe=%(adresy_dodatkowe)s, pkd_wszystkie=%(pkd_wszystkie)s, spolki_cywilne=%(spolki_cywilne)s, obywatelstwa=%(obywatelstwa)s, upadlosc_postepowanie=%(upadlosc_postepowanie)s, zakazy=%(zakazy)s, zarzadca_sukcesyjny=%(zarzadca_sukcesyjny)s, wzbogacono_dnia=%(wzbogacono_dnia)s WHERE ceidg_id = %(ceidg_id)s"
                    cursor.execute(update_query, update_data)
                    conn.commit()
                    logging.info(f"Pomyślnie zaktualizowano (KOMPLET) rekord {ceidg_id}.")
                except requests.exceptions.RequestException as e:
                    logging.error(f"Błąd zapytania do API dla ID {ceidg_id}: {e}")
                time.sleep(SLEEP_INTERVAL)
        except mysql.connector.Error as err:
            logging.error(f"Błąd bazy danych: {err}. Próbuję połączyć ponownie.")
            time.sleep(60)
            conn.close()
            conn = get_db_connection()
            if conn: cursor = conn.cursor(dictionary=True)
            else: break
        except Exception as e:
            logging.error(f"Nieoczekiwany błąd: {e}", exc_info=True)
            time.sleep(10)
    if conn and conn.is_connected(): conn.close()

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
            logging.info("Plik blokady usunięty.")