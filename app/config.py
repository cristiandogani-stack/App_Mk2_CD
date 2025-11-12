import os
from dotenv import load_dotenv

# Carica automaticamente variabili da .env se presente
load_dotenv()

BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-cambia-questa-chiave')

    # ------------------------------------------------------------------
    # Database configuration
    #
    # The original application stored its SQLite database under the
    # ``instance`` folder (``instance/app.db``) rather than alongside the
    # application code.  When the reservation and production box
    # functionality was introduced the default URI was inadvertently
    # switched to ``app.db`` in the project root, resulting in a fresh
    # empty database being used.  This caused lookups for existing
    # products (e.g. DQS100) to fail and the reservation API to return
    # ``Product not found`` errors, which surfaced in the UI as
    # "Errore nella creazione della prenotazione".  To preserve
    # backwards‑compatibility and ensure the existing data remains
    # available, point the SQLAlchemy URI back to the ``instance``
    # database by default.  You can override this via the
    # ``DATABASE_URL`` environment variable if a different database is
    # desired.
    db_filename = os.path.join('instance', 'app.db')
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or 'sqlite:///' + os.path.join(BASE_DIR, db_filename)

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    REMEMBER_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_SAMESITE = 'Lax'
