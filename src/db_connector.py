import mysql.connector
import logging
import os

# Importujemy konfigurację z naszego modułu
from .config import db_config

def get_db_connection():
    """
    Nawiązuje bezpieczne połączenie z bazą danych MySQL,
    uwzględniając certyfikat SSL/TLS.
    """
    try:
        # Tworzymy kopię konfiguracji, aby ją zmodyfikować
        conn_params = dict(db_config)
        
        # Konwertujemy port na liczbę całkowitą
        if 'port' in conn_params:
            conn_params['port'] = int(conn_params['port'])
            
        # Sprawdzamy, czy podano ścieżkę do certyfikatu SSL
        if 'ssl_ca_path' in conn_params and conn_params['ssl_ca_path']:
            # Tworzymy pełną ścieżkę do certyfikatu
            ca_path = os.path.join(os.path.dirname(__file__), '..', conn_params['ssl_ca_path'])
            
            if os.path.exists(ca_path):
                # Zmieniamy klucz na ten wymagany przez konektor
                conn_params['ssl_ca'] = ca_path
                # Usuwamy nasz niestandardowy klucz
                del conn_params['ssl_ca_path']
            else:
                logging.warning(f"Nie znaleziono pliku certyfikatu SSL w: {ca_path}")
        
        conn = mysql.connector.connect(**conn_params)
        return conn
        
    except mysql.connector.Error as err:
        logging.error(f"Błąd połączenia z bazą MySQL: {err}")
        return None