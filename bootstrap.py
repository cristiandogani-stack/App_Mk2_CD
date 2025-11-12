from app import create_app
from app.extensions import db
from app.models import User, Module
import secrets

app = create_app()

with app.app_context():
    db.create_all()

    admin = User.query.filter_by(role='admin').first()
    if not admin:
        email = 'admin@example.com'
        password = secrets.token_urlsafe(12)
        admin = User(email=email, role='admin', active=True)
        admin.set_password(password)
        db.session.add(admin)
        print('--- ADMIN CREATO ---')
        print(f'Email:    {email}')
        print(f'Password: {password}')
        print('Salva queste credenziali e cambiale dal codice/DB appena possibile.')

    defaults = [
        ('Dashboard', 'dashboard', 'dashboard.index', True, 'squares'),
        ('Admin', 'admin', 'admin.index', True, 'gear'),
        ('Magazzino', 'inventory', 'inventory.index', True, 'box'),
        ('Produzione', 'production', 'production.index', True, 'factory'),
        ('KPI', 'kpi', 'kpi.index', True, 'chart'),
        # Rename the products module to "Anagrafiche".  The slug and endpoint remain
        # unchanged to preserve existing routes and database relationships.
        ('Anagrafiche', 'products', 'products.index', True, 'box'),
    ]
    for name, slug, endpoint, enabled, icon in defaults:
        if not Module.query.filter_by(slug=slug).first():
            db.session.add(Module(name=name, slug=slug, endpoint=endpoint, enabled=enabled, icon=icon))

    db.session.commit()
    print('Database pronto.')
