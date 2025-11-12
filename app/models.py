from .extensions import db, login_manager
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

class TimestampMixin(db.Model):
    __abstract__ = True
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class User(UserMixin, TimestampMixin):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(150), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(50), default='user')
    active = db.Column(db.Boolean, default=True)

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    @property
    def is_admin(self) -> bool:
        return self.role == 'admin'

@login_manager.user_loader
def load_user(user_id):
    try:
        return User.query.get(int(user_id))
    except Exception:
        return None

class Module(TimestampMixin):
    __tablename__ = 'modules'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    slug = db.Column(db.String(100), unique=True, nullable=False)
    endpoint = db.Column(db.String(120), unique=True, nullable=False)
    enabled = db.Column(db.Boolean, default=True)
    icon = db.Column(db.String(100), default='cube')

class StructureType(TimestampMixin):
    __tablename__ = 'structure_types'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    description = db.Column(db.Text)

    # Custom fields defined for this type
    fields = db.relationship('TypeField', backref='type', cascade='all, delete-orphan')

    # Flags to identify the typology: assembly, part or commercial part.
    is_assembly = db.Column(db.Boolean, default=False)
    is_part = db.Column(db.Boolean, default=False)
    is_commercial = db.Column(db.Boolean, default=False)

    # -------------------------------------------------------------------
    # Default manufacturing/commercial attributes for this type
    #
    # When defining a new structure type the administrator may provide
    # default values for cost, processing and supplier information.  These
    # values act as a template when new structure nodes of this type are
    # created or when components are added to products.  All fields are
    # optional and will only be used when defined.  Times are stored in
    # minutes (for standard time and lead times) to match the units used
    # elsewhere in the application.  Prices and weights use floats.
    default_work_phase_id = db.Column(db.Integer, db.ForeignKey('work_phases.id'), nullable=True)
    default_processing_type = db.Column(db.String(20))  # "internal" or "external"
    default_supplier_id = db.Column(db.Integer, db.ForeignKey('suppliers.id'), nullable=True)
    default_work_center_id = db.Column(db.Integer, db.ForeignKey('work_centers.id'), nullable=True)
    default_weight = db.Column(db.Float)
    default_processing_cost = db.Column(db.Float)
    default_standard_time = db.Column(db.Float)  # stored in minutes
    default_lead_time_theoretical = db.Column(db.Float)  # stored in minutes
    default_lead_time_real = db.Column(db.Float)  # stored in minutes
    default_description = db.Column(db.Text)
    default_notes = db.Column(db.Text)
    default_price_per_unit = db.Column(db.Float)
    default_minimum_order_qty = db.Column(db.Integer)

    # -------------------------------------------------------------------
    # Plain text accessor for default notes
    #
    # ``default_notes`` may contain JSON metadata when administrators
    # store additional flags (such as ``lot_management``) alongside
    # human‑entered notes.  When rendering default notes in forms or
    # descriptions, templates should display only the user‑entered text.
    # The ``default_notes_plain`` property attempts to parse the
    # ``default_notes`` field as JSON and return the ``notes`` value
    # contained within.  If parsing fails or the stored value is not a
    # JSON object, the raw string is returned.  When no default notes
    # exist, None is returned.
    @property
    def default_notes_plain(self) -> str | None:
        raw = getattr(self, 'default_notes', None)
        if not raw:
            return None
        try:
            import json as _json
            parsed = _json.loads(raw)
            if isinstance(parsed, dict):
                return parsed.get('notes') or ''
            return raw
        except Exception:
            return raw

    # -------------------------------------------------------------------
    # Inventory management defaults
    #
    # ``default_stock_threshold`` defines the minimum on‑hand quantity for
    # this type before a replenishment should be triggered.  When the
    # available stock in the warehouse drops below this threshold the
    # system may generate a replenishment request.  ``default_replenishment_qty``
    # specifies the quantity that should be ordered or produced when
    # restocking.  Both fields are optional and can be left undefined.  When
    # undefined at the type level the node defaults or component values
    # control the behaviour.
    default_stock_threshold = db.Column(db.Float)
    default_replenishment_qty = db.Column(db.Float)

    # Relationships to dictionary tables for the default attributes.  These
    # relationships use explicit foreign key lists to avoid conflicts with
    # similarly named relationships on other models.  When no default is
    # specified the relationship will be None.
    default_work_phase = db.relationship('WorkPhase', foreign_keys=[default_work_phase_id])
    default_supplier = db.relationship('Supplier', foreign_keys=[default_supplier_id])
    default_work_center = db.relationship('WorkCenter', foreign_keys=[default_work_center_id])

class Structure(TimestampMixin):
    __tablename__ = 'structures'
    id = db.Column(db.Integer, primary_key=True)
    type_id = db.Column(db.Integer, db.ForeignKey('structure_types.id'), nullable=False)
    name = db.Column(db.String(200), nullable=False)
    parent_id = db.Column(db.Integer, db.ForeignKey('structures.id'), nullable=True)

    type = db.relationship('StructureType', backref=db.backref('nodes', lazy='dynamic'))
    parent = db.relationship('Structure', remote_side=[id], backref='children')

    # -------------------------------------------------------------------------
    # Master component reference
    #
    # Each structure node may be linked to a ComponentMaster record via
    # component_id.  When linked, the node's own descriptive fields (weight,
    # description, notes, processing parameters, etc.) are treated as
    # overrides or as legacy data only.  New nodes created through the admin
    # interface will always have a component master assigned.  Existing
    # structures created before the introduction of ComponentMaster will
    # initially have a NULL component_id until migrated.
    component_id = db.Column(db.Integer, db.ForeignKey('component_masters.id'), nullable=True)

    # Relationship to the master component.  Accessing s.component_master
    # returns the corresponding ComponentMaster instance or None.
    component_master = db.relationship('ComponentMaster')

    # Typology flags for the node (assembly, part, commercial part)
    flag_assembly = db.Column(db.Boolean, default=False)
    flag_part = db.Column(db.Boolean, default=False)
    flag_commercial = db.Column(db.Boolean, default=False)

    table_assieme = db.Column(db.Boolean, default=False)
    table_parte = db.Column(db.Boolean, default=False)
    table_commerciale = db.Column(db.Boolean, default=False)

    # -------------------------------------------------------------------
    # Default manufacturing/commercial attributes for this node
    #
    # Similar to the default attributes on StructureType, these fields
    # capture the per‑node defaults that are applied when the node is
    # associated with a product via ProductComponent.  By storing the
    # attributes directly on the Structure, administrators can fine‑tune
    # values for individual nodes while still inheriting from the type
    # defaults when unspecified.  As with other models, times are stored
    # in minutes and prices/weights are floats.
    work_phase_id = db.Column(db.Integer, db.ForeignKey('work_phases.id'), nullable=True)
    processing_type = db.Column(db.String(20))  # "internal" or "external"
    supplier_id = db.Column(db.Integer, db.ForeignKey('suppliers.id'), nullable=True)
    work_center_id = db.Column(db.Integer, db.ForeignKey('work_centers.id'), nullable=True)
    weight = db.Column(db.Float)
    processing_cost = db.Column(db.Float)
    standard_time = db.Column(db.Float)  # stored in minutes
    lead_time_theoretical = db.Column(db.Float)  # stored in minutes
    lead_time_real = db.Column(db.Float)  # stored in minutes
    description = db.Column(db.Text)
    notes = db.Column(db.Text)
    price_per_unit = db.Column(db.Float)
    minimum_order_qty = db.Column(db.Integer)

    # -------------------------------------------------------------------
    # Notes plain-text accessor
    #
    # Structure.notes may contain a JSON object when flags such as
    # ``lot_management`` are stored alongside user-entered notes.  The
    # ``notes_plain`` property attempts to parse the notes as JSON and
    # return the ``notes`` value if available.  If parsing fails or the
    # content is not JSON, the raw notes string is returned.  When no
    # notes are defined, None is returned.
    @property
    def notes_plain(self) -> str | None:
        raw = getattr(self, 'notes', None)
        if not raw:
            return None
        try:
            import json as _json
            parsed = _json.loads(raw)
            if isinstance(parsed, dict):
                return parsed.get('notes') or ''
            return raw
        except Exception:
            return raw

    # -------------------------------------------------------------------
    # On‑hand inventory quantity
    #
    # ``quantity_in_stock`` stores the current quantity of this structure
    # available in the warehouse.  Assemblies compute their available
    # quantity based on the minimum quantity of their immediate children,
    # whereas parts and commercial components simply track the number
    # loaded via the production page.  This field is optional and stored
    # as a float to accommodate fractional units if necessary.
    quantity_in_stock = db.Column(db.Float)

    # -------------------------------------------------------------------
    # Inventory management
    #
    # ``stock_threshold`` represents the minimum quantity of this structure
    # that should be kept on hand in the warehouse.  When the available
    # quantity drops below this threshold the inventory system may trigger a
    # replenishment.  ``replenishment_qty`` defines the quantity to be
    # produced or ordered when the threshold is reached.  Both fields are
    # optional and stored as floats to accommodate fractional units if
    # required.
    stock_threshold = db.Column(db.Float)
    replenishment_qty = db.Column(db.Float)

    # -------------------------------------------------------------------
    # Additional boolean flags for future functionality
    #
    # ``is_sellable`` indicates whether this structure/node represents a
    # sellable item.  ``guiding_part`` marks the node as a guiding part
    # (assegna un unico Data Matrix per il componente e i suoi figli).  Both
    # flags are optional and default to False.  These fields allow
    # administrators to tag nodes during default definition and ensure
    # consistent propagation to product components.
    is_sellable = db.Column(db.Boolean)
    guiding_part = db.Column(db.Boolean)

    # -------------------------------------------------------------------
    # Revision management
    #
    # Each structure may optionally track a revision index.  When a
    # revision value is zero or None the structure is considered to
    # have no revision and the corresponding column in lists and
    # descriptions remains empty.  Otherwise the revision is mapped to
    # a letter starting from 'A' for 1, 'B' for 2 and so on up to
    # 'Z'.  Components can be revised via the revision button in the
    # component detail page.  The revision index is persisted here so
    # that multiple revisions of the same component can coexist.
    revision = db.Column(db.Integer, default=0)

    # -------------------------------------------------------------------
    # Compatible revisions tracking
    #
    # When revising a structure the user may select previous revisions that
    # remain compatible with the new revision.  These selections are
    # persisted in this column as a comma‑separated list of revision
    # labels (e.g. "Rev.A,Rev.B").  An empty value or NULL indicates
    # there are no explicitly compatible prior versions.  The helpers
    # below provide convenient access to the selections as a list and
    # support checking membership from templates.
    compatible_revisions = db.Column(db.Text)

    @property
    def compatible_revisions_list(self) -> list[str]:
        """Return the list of revision labels marked as compatible.

        The compatible revisions string is stored comma‑separated in the
        database.  This property splits the string into a list of
        trimmed labels.  When no compatible revisions are stored it
        returns an empty list.
        """
        try:
            val = self.compatible_revisions or ''
        except Exception:
            val = ''
        if not val:
            return []
        # Split on commas and strip whitespace
        return [v.strip() for v in val.split(',') if v.strip()]

    @property
    def revision_label(self) -> str:
        """Return the human readable revision label, e.g. 'Rev.A'.

        When the revision index is defined and greater than zero this
        property converts the numeric revision into a letter from
        A–Z.  Values beyond 26 wrap around Z.  When the revision
        index is 0 or None an empty string is returned.
        """
        try:
            idx = int(self.revision or 0)
        except Exception:
            idx = 0
        if idx and idx > 0:
            # Clamp to the range 1–26 (A–Z)
            capped = idx if idx <= 26 else 26
            # zero‑based offset
            letter = chr(ord('A') + capped - 1)
            return f"Rev.{letter}"
        return ''

    @property
    def revision_letters(self) -> list[str]:
        """Return a list of revision labels from the first to the current.

        For example, if revision == 3 this returns ['Rev.A', 'Rev.B',
        'Rev.C'].  When revision is 0 or None the list is empty.
        """
        labels: list[str] = []
        try:
            count = int(self.revision or 0)
        except Exception:
            count = 0
        if count and count > 0:
            # cap at 26 revisions
            limit = count if count <= 26 else 26
            for i in range(1, limit + 1):
                letter = chr(ord('A') + i - 1)
                labels.append(f"Rev.{letter}")
        return labels

    @property
    def display_name(self) -> str:
        """Return the structure name with revision suffix when applicable.

        When the revision index is defined the name is suffixed with
        '_Rev.X' where X is the revision letter (e.g. MyPart_Rev.A).
        Otherwise the plain name is returned.  This is used in the
        component detail page header to reflect the current revision.
        """
        label = self.revision_label
        if label:
            # Extract the letter portion after 'Rev.'
            try:
                suffix = label.split('.')[-1]
            except Exception:
                suffix = ''
            return f"{self.name}_Rev.{suffix}"
        return self.name

    # Relationships to dictionary tables for the node default attributes
    # These relationships link the Structure fields to their associated
    # dictionary records.  They are defined at the end of the Structure
    # class so that the foreign key columns (work_phase_id, supplier_id,
    # work_center_id) are already declared.
    work_phase = db.relationship('WorkPhase', foreign_keys=[work_phase_id])
    supplier = db.relationship('Supplier', foreign_keys=[supplier_id])
    work_center = db.relationship('WorkCenter', foreign_keys=[work_center_id])


# -----------------------------------------------------------------------------
# InventoryLog model
#
# Records actions performed by users within the warehouse (magazzino) module.
# Each log entry stores the ID of the user who performed the action, the
# structure on which the action was performed (if any), the category of the
# structure (assembly, part or commercial), a textual description of the
# action, and the quantity involved.  TimestampMixin provides created_at and
# updated_at fields which record when the log was created.  Adding this model
# to the database requires no migrations when using SQLite; the table will be
# created automatically by db.create_all() during application startup.

class InventoryLog(TimestampMixin):
    __tablename__ = 'inventory_logs'
    id = db.Column(db.Integer, primary_key=True)
    # Foreign key to the user who performed the action.  Cannot be NULL.
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    # Optional reference to the structure on which the action was performed.
    structure_id = db.Column(db.Integer, db.ForeignKey('structures.id'), nullable=True)
    # Category of the structure at the time of the action.  Examples: 'assemblies',
    # 'parts', 'commercial'.  Kept as a short string for filtering.
    category = db.Column(db.String(50))
    # Text describing the action (e.g. "Caricati 10" or "Assemblati 3").
    action = db.Column(db.String(200))
    # Quantity associated with the action; optional because some actions may not
    # involve a quantity (e.g. deleting an item).  Stored as a float for
    # consistency with stock quantities.
    quantity = db.Column(db.Float, nullable=True)

    # Relationships back to the user and structure models.  Using backref
    # names allows SQLAlchemy to create convenient accessors on the related
    # objects (e.g. user.inventory_logs).
    user = db.relationship('User', backref='inventory_logs')
    structure = db.relationship('Structure', backref='inventory_logs')

# -----------------------------------------------------------------------------
# New models for reservation, stock tracking and production
#
# In response to evolving warehouse requirements this application introduces a
# number of new entities to support the concepts of reservations, physical
# stock items, production boxes, associated documents and scan events.  These
# tables are intentionally simple to integrate with the existing database
# without the need for migrations.  Each model defines a primary key on
# ``id`` and uses foreign keys back to the ``products`` table where
# appropriate.  Relationships are defined to allow SQLAlchemy to navigate
# between parent and child records.


class Reservation(TimestampMixin):
    """Represents a reservation of one or more units of a product.

    Operators create reservations when they plan to build or prepare
    components.  Each reservation records the product, the quantity
    requested, an optional note and a status.  A reservation may span
    multiple stock items when the requested quantity exceeds one.
    """
    __tablename__ = 'reservations'
    id = db.Column(db.Integer, primary_key=True)
    # Product being reserved.  Reservations are always tied to a product
    product_id = db.Column(db.Integer, db.ForeignKey('products.id'), nullable=False)
    qty = db.Column(db.Integer, nullable=False)
    note = db.Column(db.Text)
    status = db.Column(db.String(20), default='APERTO')  # APERTO, PARZIALE, CHIUSO, ANNULLATO

    product = db.relationship('Product', backref='reservations')


class ProductionBox(TimestampMixin):
    """Container grouping stock items prepared by a reservation.

    A ProductionBox holds one or more StockItem instances created when a
    reservation is made.  The box has its own unique code (e.g. BOX-2025-00001)
    and tracks the type of items it contains (parte, assieme or commerciale)
    along with a simple status.  Boxes remain 'APERTO' until operators
    complete the loading procedure.  Completed or archived boxes remain
    available for auditing in the archive view.
    """
    __tablename__ = 'production_boxes'
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(50), unique=True, nullable=False)
    box_type = db.Column(db.String(20), nullable=False)
    status = db.Column(db.String(20), default='APERTO')  # APERTO, IN_CARICO, COMPLETATO, ARCHIVIATO

    # Relationship: one box contains many stock items
    stock_items = db.relationship('StockItem', back_populates='production_box')


class StockItem(TimestampMixin):
    """Physical instance of a product tracked in the warehouse.

    Each stock item is created at reservation time with a unique
    datamatrix_code.  The status progresses through the lifecycle defined
    by the warehouse workflow: LIBERO → PRENOTATO → IN_PRODUZIONE → CARICATO →
    COMPLETATO → SCARTO.  Stock items may optionally reference a parent
    datamatrix_code when belonging to a guiding part.  They also link
    back to the reservation and production box that created them.
    """
    __tablename__ = 'stock_items'
    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.Integer, db.ForeignKey('products.id'), nullable=False)
    # The DataMatrix code uniquely identifies a stock item for scanning.
    # When batch (lot) management is enabled for a component, multiple
    # stock items may intentionally share the same DataMatrix within a
    # single production box.  To support this behaviour the uniqueness
    # constraint on ``datamatrix_code`` has been removed.  Individual
    # items can still be distinguished by their primary key and
    # production box membership.
    datamatrix_code = db.Column(db.String(200), nullable=False)
    parent_code = db.Column(db.String(200))
    status = db.Column(db.String(30), default='LIBERO')  # LIBERO, PRENOTATO, IN_PRODUZIONE, CARICATO, COMPLETATO, SCARTO
    location = db.Column(db.String(100))
    reservation_id = db.Column(db.Integer, db.ForeignKey('reservations.id'))
    production_box_id = db.Column(db.Integer, db.ForeignKey('production_boxes.id'))

    product = db.relationship('Product')
    reservation = db.relationship('Reservation', backref='stock_items')
    production_box = db.relationship('ProductionBox', back_populates='stock_items')


class Document(TimestampMixin):
    """Generic document associated with a stock item or production box.

    Documents capture the files required to accompany the loading of
    components.  Each document is linked either to a production box or
    to an individual stock item via the owner_type/owner_id fields.  A
    status flag tracks whether the document has been uploaded or
    approved.  The url stores the location of the uploaded file within
    the static folder.
    """
    __tablename__ = 'documents'
    id = db.Column(db.Integer, primary_key=True)
    owner_type = db.Column(db.String(10), nullable=False)  # 'BOX' or 'STOCK'
    owner_id = db.Column(db.Integer, nullable=False)
    doc_type = db.Column(db.String(50), nullable=False)
    url = db.Column(db.String(200), nullable=False)
    status = db.Column(db.String(20), default='RICHIESTO')  # RICHIESTO, CARICATO, APPROVATO, RESPINTO


class ScanEvent(TimestampMixin):
    """Audit trail capturing all scanning activity on datamatrix codes.

    Whenever a code is scanned or a state transition occurs, a ScanEvent
    is recorded.  The meta column stores additional context such as the
    user performing the action, the workstation and any associated
    box.  The ts timestamp is inherited from TimestampMixin.
    """
    __tablename__ = 'scan_events'
    id = db.Column(db.Integer, primary_key=True)
    datamatrix_code = db.Column(db.String(200), nullable=False)
    action = db.Column(db.String(50), nullable=False)
    meta = db.Column(db.Text)  # JSON stored as text for simplicity


    def __repr__(self) -> str:
        return f"<InventoryLog id={self.id} user_id={self.user_id} structure_id={self.structure_id} action={self.action}>"

    # InventoryLog does not define relationships to work phases, suppliers or work centres.
    # These relationships are specific to the Structure model and were originally placed
    # after the Structure class definition.  They have been removed from this class
    # to avoid referencing undefined names (work_phase_id, supplier_id, work_center_id).


# -----------------------------------------------------------------------------
# Product build tracking
#
# To support assembly of finished products from their component structures, the
# application records build events in a dedicated set of tables.  When an
# operator clicks the "Costruisci" button on a product card the back‑end
# decrements the on‑hand quantity of each child component, increments the
# stock for the finished product and creates a ProductBuild record.  Each
# build is associated with one or more ProductBuildItems capturing the
# component products consumed and their required quantities.  These tables
# enable an archive view that lists all builds along with the exploded
# structure used at the time of assembly and provides quick access to
# supporting documentation for each component.

class ProductBuild(TimestampMixin):
    """Represents the assembly of one or more units of a finished product.

    A ProductBuild stores the top‑level product assembled, the quantity
    produced (normally one per build action) and the timestamp of
    assembly.  Components consumed during the build are recorded via
    ProductBuildItem entries.
    """
    __tablename__ = 'product_builds'
    id = db.Column(db.Integer, primary_key=True)
    # ID of the finished product or assembly built.
    product_id = db.Column(db.Integer, db.ForeignKey('products.id'), nullable=False)
    # Quantity produced in this build (usually an integer).
    qty = db.Column(db.Integer, default=1)
    # Optional: user who performed the build.  When null the user is unknown.
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    # Optional: production box associated with this build when built from a production box.
    production_box_id = db.Column(db.Integer, db.ForeignKey('production_boxes.id'), nullable=True)

    # Timestamp of assembly is provided by TimestampMixin.created_at
    product = db.relationship('Product')
    # Relationship back to the building user (if recorded)
    user = db.relationship('User')
    # Relationship to a production box (if recorded)
    production_box = db.relationship('ProductionBox')
    items = db.relationship('ProductBuildItem', backref='build', cascade='all, delete-orphan')


class ProductBuildItem(TimestampMixin):
    """Link between a build and a component consumed during assembly.

    Each ProductBuildItem references the child product consumed and the
    quantity required to build one unit of the parent product.  Recording
    this information at build time allows the archive view to display the
    exploded structure used for the assembly and to attach documentation
    links at the component level.  The foreign key to Product points to
    the product definition rather than to a Structure because products
    aggregate structures via ProductComponent and BOM.
    """
    __tablename__ = 'product_build_items'
    id = db.Column(db.Integer, primary_key=True)
    build_id = db.Column(db.Integer, db.ForeignKey('product_builds.id'), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey('products.id'), nullable=False)
    quantity_required = db.Column(db.Float, default=1.0)

    product = db.relationship('Product')


# -------------------------------------------------------------
# Product and Bill of Materials
#
# A product is composed of one or more structures (assemblies/parts).  The
# ProductComponent association table links a product to the structures that
# constitute its bill of materials.  Each component can optionally have a
# quantity (defaults to 1).

class Product(TimestampMixin):
    __tablename__ = 'products'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    
    revision = db.Column(db.Integer, default=1)
    revisionabile = db.Column(db.Boolean, default=False)

    # Optional filename of an uploaded image associated with this product.  When
    # present, templates can render it via the ``uploads`` directory under
    # ``app/static``.  If absent, a placeholder image is shown.  Note that
    # file upload handling is performed in the product blueprint.
    image_filename = db.Column(db.String(200))

    # -------------------------------------------------------------------
    # Extended product attributes
    #
    # To support additional product-level specifications requested by users,
    # the Product model exposes optional fields for flow rate, maximum
    # pressure, physical dimensions (x/y/z) and an additional image to
    # represent characteristic performance curves.  These fields are all
    # nullable so that existing records are not required to define them.
    # When defined, templates can render the values and the curve image
    # alongside the primary product image.  The curve image filename is
    # stored separately from ``image_filename`` so that uploading a new
    # curve does not overwrite the primary product image.

    # Numeric flow rate of the product (e.g. litres per minute).  Stored
    # as a float to allow fractional values.  A null value indicates no
    # specification has been set.
    flow_rate = db.Column(db.Float)

    # Maximum pressure that the product can withstand (e.g. bar or PSI).
    # Also stored as a float for consistency.  Leave null when not
    # applicable.
    max_pressure = db.Column(db.Float)

    # Dimensions of the product along the X, Y and Z axes.  Each
    # dimension is stored as a float to capture decimal values (e.g.
    # millimetres).  All fields are optional and default to null.
    dimension_x = db.Column(db.Float)
    dimension_y = db.Column(db.Float)
    dimension_z = db.Column(db.Float)

    # -------------------------------------------------------------------
    # Inventory quantity for products
    #
    # Products now track their own on-hand quantity to support the
    # reservation workflow.  When a production box is fully loaded,
    # its contained stock items increment the corresponding product's
    # quantity_in_stock.  Assemblies built via the "Costruisci"
    # action similarly update this value.  Defaults to zero.
    quantity_in_stock = db.Column(db.Float, default=0)

    # Optional filename of an uploaded curve image associated with this
    # product.  When present, templates can render this image adjacent
    # to the main product image.  The image is stored in the same
    # ``uploads`` directory as the primary product image.  If absent,
    # no curve is displayed.
    curve_image_filename = db.Column(db.String(200))

    # Additional specifications for hydrodynamic and acoustic properties.
    #
    # ``fluid_in_let`` and ``layer_in_lett`` capture the BSPP sizes for
    # the fluid and layer inlets respectively.  These are stored as
    # short strings (e.g. "1 1/5\" BSPP").
    fluid_in_let = db.Column(db.String(50))
    layer_in_lett = db.Column(db.String(50))

    # ``noise`` represents the nominal noise level of the product in
    # dB(A).  A ``None`` value indicates that the noise has not been
    # specified.
    noise = db.Column(db.Float)

    # ``max_pressure_from`` and ``max_pressure_to`` represent the range
    # of maximum pressure in bar.  When both are defined they define a
    # continuous interval [from, to] expressed in bar.  Either value may
    # be ``None`` if only a single limit is specified.
    max_pressure_from = db.Column(db.Float)
    max_pressure_to = db.Column(db.Float)

    components = db.relationship('ProductComponent', backref='product', cascade='all, delete-orphan')

    bom_children = db.relationship('BOMLine',
                                   foreign_keys='BOMLine.padre_id',
                                   backref='padre',
                                   cascade='all, delete-orphan')
    bom_parents = db.relationship('BOMLine',
                                  foreign_keys='BOMLine.figlio_id',
                                  backref='figlio',
                                  cascade='all, delete-orphan')

    custom_values = db.relationship('CustomValue',
                                    primaryjoin="Product.id==CustomValue.product_id",
                                    backref='product',
                                    cascade='all, delete-orphan')

class ProductComponent(TimestampMixin):
    __tablename__ = 'product_components'
    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.Integer, db.ForeignKey('products.id'), nullable=False)
    structure_id = db.Column(db.Integer, db.ForeignKey('structures.id'), nullable=False)
    quantity = db.Column(db.Integer, default=1)

    # -------------------------------------------------------------------------
    # Detailed manufacturing/commercial metadata
    #
    # The following optional fields allow storing per‑component information
    # depending on whether the associated structure node represents a part,
    # assembly or commercial part.  For assemblies only the quantity and
    # structure link are used, whereas parts and commercial parts may define
    # additional attributes.  All fields are nullable to avoid forcing values
    # when they are not applicable.

    # Part and commercial: reference to the work phase (dizionario Fasi di lavorazione)
    work_phase_id = db.Column(db.Integer, db.ForeignKey('work_phases.id'), nullable=True)
    # Part and commercial: whether the process is internal or external
    processing_type = db.Column(db.String(20))  # "internal" or "external"
    # Part and commercial: reference to a supplier (dizionario Fornitori)
    supplier_id = db.Column(db.Integer, db.ForeignKey('suppliers.id'), nullable=True)
    # Part and commercial: reference to a work center (dizionario Centri di lavoro)
    work_center_id = db.Column(db.Integer, db.ForeignKey('work_centers.id'), nullable=True)
    # Part: weight of the single part expressed in kilograms
    weight = db.Column(db.Float)
    # Part: processing cost per piece in EUR
    processing_cost = db.Column(db.Float)
    # Part: standard time required to complete the processing (minutes).  Only
    # meaningful when ``processing_type`` is "internal".
    standard_time = db.Column(db.Float)
    # Part and commercial: theoretical lead time expressed in minutes
    lead_time_theoretical = db.Column(db.Float)
    # Part and commercial: real lead time expressed in minutes
    lead_time_real = db.Column(db.Float)
    # Part and commercial: free‑text notes
    notes = db.Column(db.Text)
    # Commercial part: price per unit in EUR
    price_per_unit = db.Column(db.Float)
    # Commercial part: minimum order lot (integer).  Not relevant for parts.
    minimum_order_qty = db.Column(db.Integer)

    # -------------------------------------------------------------------------
    # Inventory management per product component
    #
    # ``stock_threshold`` and ``replenishment_qty`` mirror the inventory
    # management fields on Structure and ComponentMaster.  They allow
    # specifying per‑product overrides for the stock threshold and the
    # replenishment quantity.  When undefined, the system falls back to
    # values defined on the associated structure node or component master.
    stock_threshold = db.Column(db.Float)
    replenishment_qty = db.Column(db.Float)

    # -------------------------------------------------------------------------
    # Additional boolean flags per product component
    #
    # ``is_sellable`` designates whether this component is sold as a
    # standalone item.  ``guiding_part`` marks the component as a guiding
    # part, signifying that a single Data Matrix should be assigned to this
    # component and its children.  These flags default to False and can be
    # overridden at the product level without affecting other products.
    is_sellable = db.Column(db.Boolean)
    guiding_part = db.Column(db.Boolean)

    # -------------------------------------------------------------------------
    # Component master reference
    #
    # To enforce absolute uniqueness of component codes across the entire
    # application we introduce a ComponentMaster model.  Each structure node
    # points at a master component and every product component references the
    # same master for the same code.  The master holds the canonical
    # description, processing parameters, notes, costs, etc.  When a
    # ProductComponent has an associated master its own descriptive fields
    # (weight, description, notes, etc.) should generally be ignored in
    # favour of the data stored on the master.  Existing fields remain for
    # backwards compatibility and to support per‑product overrides when
    # necessary.

    # Foreign key reference into the component_masters table.  Nullable to
    # support legacy rows that have not yet been migrated.
    component_id = db.Column(db.Integer, db.ForeignKey('component_masters.id'), nullable=True)

    # Relationship to the master component.  Accessing comp.component_master
    # returns the corresponding ComponentMaster instance or None.
    component_master = db.relationship('ComponentMaster')

    # -------------------------------------------------------------------------
    # Additional fields
    #
    # description: free‑text description imported from BOM files.  For
    # commercial components this field captures the description instead of
    # storing it in the notes attribute.  Assemblies and parts may also
    # populate this field depending on import logic.
    description = db.Column(db.Text)
    # image_filename: optional filename of an uploaded image associated with
    # this component.  When present, templates can render it via the
    # ``uploads`` directory under ``app/static``.  If absent, a placeholder
    # image is shown.  File upload handling is performed in the component
    # detail blueprint.
    image_filename = db.Column(db.String(200))

    # Relationships back to dictionary tables
    work_phase = db.relationship('WorkPhase')
    supplier = db.relationship('Supplier')
    work_center = db.relationship('WorkCenter')

    structure = db.relationship('Structure')

    # field values associated with this component (e.g. attributes defined per type)
    values = db.relationship('ComponentFieldValue', backref='component', cascade='all, delete-orphan')

    # ---------------------------------------------------------------------
    # Expose a plain-text representation of the notes field.
    #
    # ProductComponent.notes may contain JSON-encoded metadata in addition
    # to user-entered notes (e.g. cycles or the lot_management flag).  When
    # rendering descriptions or populating form fields, templates should
    # present only the human-entered notes and hide any JSON structure.  The
    # ``notes_plain`` property attempts to parse the notes as JSON and return
    # the ``notes`` value if available.  If parsing fails or the content is
    # not JSON, the original string is returned.  This property returns
    # ``None`` when no notes are defined.
    @property
    def notes_plain(self) -> str | None:
        """Return a plain-text version of the notes, stripping any JSON."""
        raw = getattr(self, 'notes', None)
        if not raw:
            return None
        try:
            import json as _json
            parsed = _json.loads(raw)
            if isinstance(parsed, dict):
                return parsed.get('notes') or ''
            return raw
        except Exception:
            return raw


# Fields definable per structure type
class TypeField(TimestampMixin):
    __tablename__ = 'type_fields'
    id = db.Column(db.Integer, primary_key=True)
    type_id = db.Column(db.Integer, db.ForeignKey('structure_types.id'), nullable=False)
    name = db.Column(db.String(100), nullable=False)

    # relationship back to StructureType: defined via backref in StructureType.fields


# Value of a field for a given product component
class ComponentFieldValue(TimestampMixin):
    __tablename__ = 'component_field_values'
    id = db.Column(db.Integer, primary_key=True)
    component_id = db.Column(db.Integer, db.ForeignKey('product_components.id'), nullable=False)
    field_id = db.Column(db.Integer, db.ForeignKey('type_fields.id'), nullable=False)
    value = db.Column(db.String(200))

    field = db.relationship('TypeField')


# -----------------------------------------------------------------------------
# New BOM and Custom Field models
#
# To support hierarchical bill of materials directly between products (as opposed
# to relying solely on ``Structure`` nodes), the BOMLine model introduces a
# self‑referencing association between two products.  Each BOM line links a
# parent product (padre) to a child product (figlio) along with a quantity.
#
# Dynamic metadata can be attached to products and individual BOM lines using
# ``CustomField`` definitions and corresponding ``CustomValue`` instances.

class BOMLine(TimestampMixin):
    __tablename__ = 'bom_lines'
    id = db.Column(db.Integer, primary_key=True)
    padre_id = db.Column(db.Integer, db.ForeignKey('products.id'), nullable=False)
    figlio_id = db.Column(db.Integer, db.ForeignKey('products.id'), nullable=False)
    quantita = db.Column(db.Float, default=1.0)

    # custom values attached to this BOM line are provided via CustomValue.bom_line


class CustomField(TimestampMixin):
    __tablename__ = 'custom_fields'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    field_type = db.Column(db.String(50), default='text')
    entity = db.Column(db.String(50), nullable=False)  # product or bom_line
    group = db.Column(db.String(100))

    values = db.relationship('CustomValue', backref='field', cascade='all, delete-orphan')


class CustomValue(TimestampMixin):
    __tablename__ = 'custom_values'
    id = db.Column(db.Integer, primary_key=True)
    field_id = db.Column(db.Integer, db.ForeignKey('custom_fields.id'), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey('products.id'), nullable=True)
    bomline_id = db.Column(db.Integer, db.ForeignKey('bom_lines.id'), nullable=True)
    value = db.Column(db.String(255))

    # Relationship back to the owning BOM line.  The relationship to the owning
    # product is provided by the backref defined on Product.custom_values above.
    bom_line = db.relationship('BOMLine', backref='custom_values', foreign_keys=[bomline_id])


# Dictionary models for suppliers, work centers, work phases and material costs
class Supplier(TimestampMixin):
    __tablename__ = 'suppliers'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), unique=True, nullable=False)

class WorkCenter(TimestampMixin):
    __tablename__ = 'work_centers'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), unique=True, nullable=False)
    # Optional hourly cost expressed in EUR.  When provided this value
    # represents the base cost per hour associated with the work centre.  It
    # allows costing calculations for parts processed at a given centre.  A
    # ``None`` value indicates that no cost has been specified.
    hourly_cost = db.Column(db.Float)

class WorkPhase(TimestampMixin):
    __tablename__ = 'work_phases'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), unique=True, nullable=False)

class MaterialCost(TimestampMixin):
    __tablename__ = 'material_costs'
    id = db.Column(db.Integer, primary_key=True)
    material = db.Column(db.String(255), nullable=False)
    cost_eur = db.Column(db.Float)


# -----------------------------------------------------------------------------
# Component Master
#
# The ComponentMaster model provides a single canonical record for each
# component code used across all structure types and product BOMs.  Prior
# to introducing this model the application stored component attributes on
# Structure and ProductComponent records, leading to duplicate data and
# inconsistencies when the same code appeared in multiple contexts.  A
# ComponentMaster record uniquely identifies a component by its ``code`` and
# stores shared attributes such as weight, description, notes, processing
# parameters and costing information.  Structure and ProductComponent now
# reference ComponentMaster via ``component_id`` and should rely on the
# master for these attributes rather than duplicating them.  During
# migration, existing attributes on structures and product components are
# copied into the corresponding master records.  New records created through
# the UI automatically attach to a master (creating one if necessary).

class ComponentMaster(TimestampMixin):
    __tablename__ = 'component_masters'
    id = db.Column(db.Integer, primary_key=True)
    # Human‑readable unique code identifying the component (e.g. P001-025-001B)
    code = db.Column(db.String(200), unique=True, nullable=False)
    # Canonical shared attributes
    weight = db.Column(db.Float)
    description = db.Column(db.Text)
    notes = db.Column(db.Text)
    processing_type = db.Column(db.String(20))  # "internal" or "external"
    work_phase_id = db.Column(db.Integer, db.ForeignKey('work_phases.id'), nullable=True)
    supplier_id = db.Column(db.Integer, db.ForeignKey('suppliers.id'), nullable=True)
    work_center_id = db.Column(db.Integer, db.ForeignKey('work_centers.id'), nullable=True)
    processing_cost = db.Column(db.Float)
    standard_time = db.Column(db.Float)  # minutes
    lead_time_theoretical = db.Column(db.Float)  # minutes
    lead_time_real = db.Column(db.Float)  # minutes
    price_per_unit = db.Column(db.Float)
    minimum_order_qty = db.Column(db.Integer)

    # -------------------------------------------------------------------
    # Additional boolean flags for global component metadata
    #
    # ``is_sellable`` indicates whether the master component is considered
    # sellable.  ``guiding_part`` marks the component as a guiding part,
    # meaning a single Data Matrix applies to the component and its
    # descendants.  Both flags are nullable and default to False.  When
    # undefined, product components and structures may override the value.
    is_sellable = db.Column(db.Boolean)
    guiding_part = db.Column(db.Boolean)
    # JSON field storing encoded cycles (arbitrary JSON blob)
    cycles_json = db.Column(db.Text)
    # Optional type-of-processing descriptor (free text)
    type_of_processing = db.Column(db.String(100))

    # Relationships back to dictionary tables
    work_phase = db.relationship('WorkPhase')
    supplier = db.relationship('Supplier')
    work_center = db.relationship('WorkCenter')

    # -------------------------------------------------------------------
    # Inventory management defaults at the master level
    #
    # ``stock_threshold`` represents the minimum quantity that should be
    # maintained across all products referencing this component.  When a
    # structure or product component does not define its own threshold,
    # this value acts as the canonical fallback.  ``replenishment_qty``
    # specifies the quantity to replenish when stock drops below the
    # threshold.  Both fields are optional and default to None.
    stock_threshold = db.Column(db.Float)
    replenishment_qty = db.Column(db.Float)

    # -------------------------------------------------------------------
    # Notes plain-text accessor
    #
    # Similar to ProductComponent.notes_plain, the notes field on a
    # ComponentMaster may contain JSON metadata in addition to the
    # user-provided notes.  The ``notes_plain`` property returns the
    # human-readable notes portion when present, otherwise the raw
    # notes string.  When notes is empty, None is returned.
    @property
    def notes_plain(self) -> str | None:
        raw = getattr(self, 'notes', None)
        if not raw:
            return None
        try:
            import json as _json
            parsed = _json.loads(raw)
            if isinstance(parsed, dict):
                return parsed.get('notes') or ''
            return raw
        except Exception:
            return raw
