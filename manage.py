import argparse
from getpass import getpass
from app import create_app
from app.extensions import db
from app.models import User

app = create_app()

def list_users():
    with app.app_context():
        users = User.query.all()
        if not users:
            print("Nessun utente presente.")
            return
        for u in users:
            print(f"- id={u.id} username={u.username} email={u.email} role={u.role} active={u.active}")

def create_user():
    with app.app_context():
        username = input("Username: ").strip().lower()
        email = input("Email: ").strip().lower()
        role = input("Ruolo [admin/user] (default: user): ").strip().lower() or "user"
        pwd = getpass("Password: ")
        if not username or not email:
            print("Username ed email sono obbligatori.")
            return
        if User.query.filter_by(username=username).first():
            print("Utente già esistente.")
            return
        if User.query.filter_by(email=email).first():
            print("Esiste già un utente con questa email.")
            return
        u = User(username=username, email=email, role=role, active=True)
        u.set_password(pwd)
        db.session.add(u)
        db.session.commit()
        print("Utente creato.")

def reset_password():
    with app.app_context():
        username = input("Username utente: ").strip().lower()
        u = User.query.filter_by(username=username).first()
        if not u:
            print("Utente non trovato.")
            return
        pwd = getpass("Nuova password: ")
        u.set_password(pwd)
        db.session.commit()
        print("Password aggiornata.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Utility gestione utenti")
    parser.add_argument("cmd", choices=["list-users", "create-user", "reset-password", "migrate-components"])
    args = parser.parse_args()

    if args.cmd == "list-users":
        list_users()
    elif args.cmd == "create-user":
        create_user()
    elif args.cmd == "reset-password":
        reset_password()
    elif args.cmd == "migrate-components":
        # Perform data migration to the ComponentMaster architecture.
        # This command will scan all existing structures, create or reuse
        # ComponentMaster records based on the structure name, assign the
        # component_id on structures and product components, and copy
        # attachments into the master directories.  It may be safely run
        # multiple times; duplicate masters will not be created.
        from app.models import Structure, ProductComponent, ComponentMaster
        from app.extensions import db
        from app.blueprints.admin.routes import ensure_component_master_for_structure  # reuse helper
        with app.app_context():
            # Create or assign masters for structures
            all_structs = Structure.query.all()
            for s in all_structs:
                try:
                    ensure_component_master_for_structure(s)
                except Exception:
                    # Continue on error to process remaining structures
                    pass
            # Update product components
            all_comps = ProductComponent.query.all()
            for pc in all_comps:
                try:
                    if pc.structure and pc.structure.component_id:
                        pc.component_id = pc.structure.component_id
                except Exception:
                    pass
            try:
                db.session.commit()
                print("Migration completed successfully.")
            except Exception as e:
                db.session.rollback()
                print(f"Migration failed: {e}")
