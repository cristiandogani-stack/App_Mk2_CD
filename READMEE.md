# GestionApp — Starter v1.1

Fix accesso e utilità di gestione utenti incluse.

## Avvio rapido
```bash
cd gestionapp-starter
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
python bootstrap.py          # crea DB + admin temporaneo
python run.py
```
Login su http://127.0.0.1:5000 (email e password stampate da bootstrap).

### Se non riesci ad accedere
Usa gli strumenti:
```bash
python manage.py list-users
python manage.py reset-password          # inserisci email admin e nuova password
python manage.py create-user             # per crearne uno nuovo
```
