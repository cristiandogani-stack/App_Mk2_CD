from flask import Flask, redirect, url_for
from .config import Config
from .extensions import db, login_manager, csrf
from .models import User, Module
from flask_wtf.csrf import generate_csrf

def create_app(config_class=Config):
    app = Flask(__name__, static_folder='static', template_folder='templates')
    app.config.from_object(config_class)

    db.init_app(app)
    login_manager.init_app(app)
    csrf.init_app(app)

    login_manager.login_view = 'auth.login'
    login_manager.login_message_category = 'info'

    from .blueprints.auth.routes import auth_bp
    from .blueprints.dashboard.routes import dashboard_bp
    from .blueprints.admin.routes import admin_bp
    from .blueprints.inventory.routes import inventory_bp
    from .blueprints.production.routes import production_bp
    from .blueprints.kpi.routes import kpi_bp
    from .blueprints.products.routes import products_bp
    from .blueprints.api import api_bp

    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(dashboard_bp, url_prefix='/dashboard')
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(inventory_bp, url_prefix='/inventory')
    app.register_blueprint(production_bp, url_prefix='/production')
    app.register_blueprint(kpi_bp, url_prefix='/kpi')
    app.register_blueprint(products_bp, url_prefix='/products')
    # Register API blueprint at the root '/api' path
    app.register_blueprint(api_bp, url_prefix='/api')
    # Exempt API routes from CSRF protection as they are invoked via
    # asynchronous fetch requests that do not include the CSRF token.
    try:
        csrf.exempt(api_bp)
    except Exception:
        pass

    with app.app_context():
        # Create all tables defined in the SQLAlchemy models.  This call
        # creates tables that do not yet exist but does not add missing
        # columns to existing tables.  Because the database schema may
        # evolve over time and migrations are not available in this
        # simplified setup, we perform additional checks to ensure
        # critical columns exist.  In particular, the ``work_centers``
        # table gained an ``hourly_cost`` column in a later version of
        # the application.  Attempting to query this column on older
        # databases will raise an OperationalError ("no such column").
        db.create_all()

        # -------------------------------------------------------------------
        # Ensure ``hourly_cost`` column exists on ``work_centers`` table.
        #
        # When upgrading from previous versions without running a formal
        # migration, the ``work_centers`` table may be missing the
        # ``hourly_cost`` column.  This runtime check inspects the table
        # schema via SQLite PRAGMA and, if necessary, issues an ALTER
        # TABLE statement to add the column.  The operation is idempotent on
        # SQLite and will only execute once when the column is missing.  On
        # other database backends, explicit migrations are preferred.
        try:
            from sqlalchemy import text
            conn = db.engine.connect()
            res = conn.execute(text('PRAGMA table_info(work_centers)')).fetchall()
            col_names = [row[1] for row in res]
            if 'hourly_cost' not in col_names:
                conn.execute(text('ALTER TABLE work_centers ADD COLUMN hourly_cost FLOAT'))

            # -----------------------------------------------------------------
            # Ensure new columns exist on structure_types and structures tables.
            #
            # When adding support for definisci details on types and nodes, new
            # columns were introduced on the ``structure_types`` and
            # ``structures`` tables.  Older database files will not have
            # these columns, resulting in OperationalError when SQLAlchemy
            # attempts to access them.  The following runtime checks use
            # SQLite PRAGMA to inspect the table schemas and add missing
            # columns via ALTER TABLE statements.  Since SQLite supports
            # adding columns in place (without foreign key constraints), this
            # approach is safe and idempotent.
            # Check structure_types table
            res_types = conn.execute(text('PRAGMA table_info(structure_types)')).fetchall()
            type_cols = [row[1] for row in res_types]
            # Map of column name to SQL type
            type_column_defs = {
                'default_work_phase_id': 'INTEGER',
                'default_processing_type': 'TEXT',
                'default_supplier_id': 'INTEGER',
                'default_work_center_id': 'INTEGER',
                'default_weight': 'FLOAT',
                'default_processing_cost': 'FLOAT',
                'default_standard_time': 'FLOAT',
                'default_lead_time_theoretical': 'FLOAT',
                'default_lead_time_real': 'FLOAT',
                'default_description': 'TEXT',
                'default_notes': 'TEXT',
                'default_price_per_unit': 'FLOAT',
                'default_minimum_order_qty': 'INTEGER'
            }
            for col_name, col_type in type_column_defs.items():
                if col_name not in type_cols:
                    conn.execute(text(f'ALTER TABLE structure_types ADD COLUMN {col_name} {col_type}'))
            # Check structures table
            res_structs = conn.execute(text('PRAGMA table_info(structures)')).fetchall()
            struct_cols = [row[1] for row in res_structs]
            struct_column_defs = {
                'work_phase_id': 'INTEGER',
                'processing_type': 'TEXT',
                'supplier_id': 'INTEGER',
                'work_center_id': 'INTEGER',
                'weight': 'FLOAT',
                'processing_cost': 'FLOAT',
                'standard_time': 'FLOAT',
                'lead_time_theoretical': 'FLOAT',
                'lead_time_real': 'FLOAT',
                'description': 'TEXT',
                'notes': 'TEXT',
                'price_per_unit': 'FLOAT',
                'minimum_order_qty': 'INTEGER',
                # New column introduced when adding ComponentMaster support.  A
                # structure references a master component via this nullable
                # foreign key.  SQLite does not enforce foreign key
                # constraints on ALTER TABLE, but the column is needed for
                # application logic.
                'component_id': 'INTEGER'
                ,
                # Additional boolean flags for structures.  These columns were
                # introduced to support sellable and guiding part flags on
                # structures.  They default to NULL/False when not present.
                'is_sellable': 'BOOLEAN',
                'guiding_part': 'BOOLEAN',
                # On‑hand inventory quantity.  Assemblies compute their
                # available stock based on children, whereas parts and
                # commercial components track how many units are
                # available directly.  Stored as FLOAT for consistency.
                'quantity_in_stock': 'FLOAT',
                # Compatible revisions column stores a comma‑separated list of
                # prior revision labels selected as compatible for a
                # structure.  Added dynamically to maintain backwards
                # compatibility with existing databases.
                'compatible_revisions': 'TEXT'
            }
            for col_name, col_type in struct_column_defs.items():
                if col_name not in struct_cols:
                    conn.execute(text(f'ALTER TABLE structures ADD COLUMN {col_name} {col_type}'))

            # -----------------------------------------------------------------
            # Ensure ``component_id`` column exists on ``product_components`` table.
            #
            # The product_components table gained a component_id column when
            # migrating to the ComponentMaster architecture.  Older
            # databases will not have this column, so we add it at runtime
            # when missing.  Since SQLite does not support adding foreign
            # keys via ALTER TABLE, the new column is created without a
            # constraint.  The ORM will still map it correctly and
            # application logic enforces referential integrity.
            res_pcs = conn.execute(text('PRAGMA table_info(product_components)')).fetchall()
            pc_cols = [row[1] for row in res_pcs]
            if 'component_id' not in pc_cols:
                conn.execute(text('ALTER TABLE product_components ADD COLUMN component_id INTEGER'))

            # -----------------------------------------------------------------
            # Ensure extended columns exist on ``products`` table.
            #
            # When adding support for flow rate, maximum pressure, dimensions
            # and characteristic curve images on products, additional
            # columns were introduced on the ``products`` table.  Older
            # database files will be missing these columns, resulting in
            # OperationalError when SQLAlchemy attempts to access them.  The
            # following block checks for their existence and adds any
            # missing columns via ALTER TABLE statements.  The operation is
            # idempotent and only runs when necessary.
            res_prod = conn.execute(text('PRAGMA table_info(products)')).fetchall()
            prod_cols = [row[1] for row in res_prod]
            prod_column_defs = {
                'flow_rate': 'FLOAT',
                'max_pressure': 'FLOAT',  # legacy single max pressure column
                'dimension_x': 'FLOAT',
                'dimension_y': 'FLOAT',
                'dimension_z': 'FLOAT',
                'curve_image_filename': 'VARCHAR(200)',
                # New columns for extended specifications
                'fluid_in_let': 'VARCHAR(50)',
                'layer_in_lett': 'VARCHAR(50)',
                'noise': 'FLOAT',
                'max_pressure_from': 'FLOAT',
                'max_pressure_to': 'FLOAT'
                ,
                # On-hand inventory quantity for products.  Added to
                # support tracking of physically built or loaded units
                # directly on the product rather than via the Structure
                # model.  Defaults to zero when absent.
                'quantity_in_stock': 'FLOAT'
            }
            for col_name, col_type in prod_column_defs.items():
                if col_name not in prod_cols:
                    conn.execute(text(f'ALTER TABLE products ADD COLUMN {col_name} {col_type}'))

            # -----------------------------------------------------------------
            # Ensure additional boolean columns exist on ``product_components`` table.
            #
            # The flags ``is_sellable`` and ``guiding_part`` were introduced to
            # designate whether a component is sellable and whether it acts
            # as a guiding part.  Older database files will not have these
            # columns, so add them via ALTER TABLE when missing.  SQLite
            # performs this operation safely and idempotently.
            res_pc = conn.execute(text('PRAGMA table_info(product_components)')).fetchall()
            pc_cols = [row[1] for row in res_pc]
            if 'is_sellable' not in pc_cols:
                conn.execute(text('ALTER TABLE product_components ADD COLUMN is_sellable BOOLEAN'))
            if 'guiding_part' not in pc_cols:
                conn.execute(text('ALTER TABLE product_components ADD COLUMN guiding_part BOOLEAN'))

            # -----------------------------------------------------------------
            # Ensure boolean columns exist on ``component_masters`` table.
            #
            # The columns ``is_sellable`` and ``guiding_part`` on the
            # component_masters table mirror the flags defined on product
            # components and structures.  They provide global defaults
            # for components.  Add the columns when absent.
            res_cm = conn.execute(text('PRAGMA table_info(component_masters)')).fetchall()
            cm_cols = [row[1] for row in res_cm]
            if 'is_sellable' not in cm_cols:
                conn.execute(text('ALTER TABLE component_masters ADD COLUMN is_sellable BOOLEAN'))
            if 'guiding_part' not in cm_cols:
                conn.execute(text('ALTER TABLE component_masters ADD COLUMN guiding_part BOOLEAN'))

            # -----------------------------------------------------------------
            # Ensure ``username`` column exists on the ``users`` table and
            # populate it for legacy records that previously stored only
            # email addresses.  The username column enables authentication
            # without relying on email addresses while maintaining
            # backwards compatibility with existing databases.
            res_users = conn.execute(text('PRAGMA table_info(users)')).fetchall()
            user_cols = [row[1] for row in res_users]
            if 'username' not in user_cols:
                conn.execute(text('ALTER TABLE users ADD COLUMN username TEXT'))
            conn.execute(text("UPDATE users SET username = LOWER(username) WHERE username IS NOT NULL AND username <> ''"))
            conn.execute(text("UPDATE users SET username = LOWER(substr(email, 1, instr(email, '@') - 1)) WHERE (username IS NULL OR username = '') AND email LIKE '%@%'"))
            conn.execute(text("UPDATE users SET username = LOWER(email) WHERE (username IS NULL OR username = '') AND email IS NOT NULL"))
            conn.execute(text("UPDATE users SET username = 'user_' || id WHERE (username IS NULL OR username = '')"))

            conn.commit()
            conn.close()
        except Exception:
            # Silently ignore errors during the schema check.  Missing
            # columns will cause errors elsewhere, at which point a
            # developer can manually intervene.  Logging could be added
            # here for visibility.
            pass

    @app.route('/')
    def root():
        return redirect(url_for('dashboard.index'))

    @app.context_processor
    def inject_csrf_token():
        return dict(csrf_token=generate_csrf)

    @app.context_processor
    def inject_enabled_modules():
        modules = Module.query.filter_by(enabled=True).order_by(Module.name.asc()).all()
        return {'enabled_modules': modules}

    return app
