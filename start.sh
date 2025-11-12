#!/usr/bin/env bash
#
# start.sh – avvia rapidamente l'app GestionApp.
#
# Questo script crea un ambiente virtuale nella directory corrente (se non presente),
# installa le dipendenze, inizializza il database e avvia il server Flask.

set -e

# Crea e attiva l'ambiente virtuale
if [ ! -d ".venv" ]; then
  echo "[+] Creo ambiente virtuale .venv"
  python3 -m venv .venv
fi
echo "[+] Attivo ambiente virtuale"
source .venv/bin/activate

# Installa dipendenze se mancano
if [ ! -f ".venv/installed.lock" ]; then
  echo "[+] Installo pacchetti da requirements.txt (potrebbe richiedere la connessione a Internet)"
  pip install --upgrade pip
  pip install -r requirements.txt
  touch .venv/installed.lock
fi

# Copia file .env se non esiste
if [ ! -f ".env" ]; then
  echo "[+] Nessun .env trovato. Copio .env.example in .env"
  cp .env.example .env
fi

# Inizializza database e crea un account admin temporaneo
echo "[+] Eseguo bootstrap per creare il database e l'utente admin"
python bootstrap.py

# Avvia l'applicazione
echo "[+] Avvio server Flask in modalità sviluppo (Ctrl+C per interrompere)"
python run.py