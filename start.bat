@echo off
REM
REM start.bat – avvia rapidamente l'app GestionApp su Windows.
REM Crea un ambiente virtuale, installa le dipendenze, inizializza il database e avvia il server.

IF NOT EXIST ".venv" (
    echo [+] Creo ambiente virtuale .venv
    python -m venv .venv
)

echo [+] Attivo ambiente virtuale
call .venv\Scripts\activate.bat

IF NOT EXIST ".venv\installed.lock" (
    echo [+] Installo pacchetti da requirements.txt (serve connessione a Internet)
    pip install --upgrade pip
    pip install -r requirements.txt
    echo dummy > .venv\installed.lock
)

IF NOT EXIST ".env" (
    echo [+] Nessun .env trovato. Copio .env.example in .env
    copy .env.example .env >NUL
)

echo [+] Eseguo bootstrap per creare il database e l'utente admin
python bootstrap.py

echo [+] Avvio server Flask in modalità sviluppo (Ctrl+C per interrompere)
python run.py

:: Mantieni aperta la finestra del terminale al termine del server per visualizzare
:: eventuali messaggi di errore.  Premi un tasto per chiudere la finestra.
pause