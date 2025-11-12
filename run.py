from app import create_app
app = create_app()

if __name__ == '__main__':
    """
    Avvio dell'applicazione Flask.

    Per impostazione predefinita l'applicazione parte in debug su
    ``127.0.0.1:5000`` come nella versione originale.  È possibile
    personalizzare l'host e la porta creando un file chiamato
    ``server_config.txt`` nella stessa directory di questo script.
    Il file deve contenere l'indirizzo IP e facoltativamente la porta
    separati da due punti, ad esempio ``0.0.0.0:8000`` oppure solo
    ``0.0.0.0`` per lasciare la porta di default (5000).  Se il file
    non è presente o non è valido viene utilizzata la configurazione
    predefinita.
    """
    import os

    # Impostazioni di default
    host = '127.0.0.1'
    port = 5000
    # Percorso del file di configurazione esterno
    config_path = os.path.join(os.path.dirname(__file__), 'server_config.txt')
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                line = f.read().strip()
            if line:
                # Se c'è il carattere ``:`` separa host e porta
                if ':' in line:
                    host_part, port_part = line.split(':', 1)
                    host = host_part.strip() or host
                    # Converte la porta in intero se possibile, altrimenti mantiene il default
                    try:
                        port = int(port_part.strip())
                    except ValueError:
                        pass
                else:
                    # Solo indirizzo IP fornito
                    host = line.strip() or host
        except Exception:
            # In caso di errore di lettura ignora il file e usa i valori di default
            pass
    app.run(host=host, port=port, debug=True)
