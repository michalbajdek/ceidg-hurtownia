import configparser
import os

# Utworzenie obiektu parsera
config = configparser.ConfigParser()

# Ścieżka do pliku konfiguracyjnego (jeden poziom wyżej niż folder src)
config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')

# Odczytanie pliku
config.read(config_path)

# Eksportowanie wartości jako zmienne
db_config = config['database']
api_config = config['api']

JWT_TOKEN = api_config.get('jwt_token')