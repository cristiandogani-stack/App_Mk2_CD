"""
Routes for the production module.

This blueprint allows operators to register new stock for parts and
commercial components.  Assemblies are excluded here – they are built
via the inventory page.  When a part's quantity is increased, any
parent assemblies are recalculated automatically so that the warehouse
dashboard reflects the number of complete units that can be assembled.
"""

import os

from flask import Blueprint, render_template, request, redirect, url_for, flash, g
from flask_login import login_required, current_user

from ...extensions import db
from ...models import Structure, InventoryLog, ProductionBox


production_bp = Blueprint('production', __name__, template_folder='../../templates')


@production_bp.before_request
def _highlight_production_tab() -> None:
    """Ensure the navigation highlights the production module.

    When operators open a production box detail page (served through this
    blueprint but ultimately rendered by the inventory view) the layout
    would previously fall back to the inventory module because the
    underlying template relied solely on the request endpoint.  Setting a
    ``g.active_module`` flag allows the base template to keep the
    "Produzione" tab highlighted throughout the workflow.
    """

    g.active_module = 'production'


def _update_parent_assemblies(part: Structure) -> None:
    """Previously propagated stock changes up the assembly hierarchy.

    Assemblies now track only physically built units in their
    ``quantity_in_stock``.  Increasing the stock of a part or
    commercial component should not automatically recalculate the
    ``quantity_in_stock`` of parent assemblies based on the minimum of
    child quantities.  The number of buildable assemblies is computed
    dynamically in the warehouse views using `complete_qty`.  This
    helper is retained for backwards compatibility but no longer
    modifies any database state.
    """
    # No action is required: assembly stock is no longer updated from child stock.
    return


@production_bp.route('/')
@login_required
def index():
    """Render the production dashboard.

    The production dashboard now lists all production boxes so that
    operators can easily see pending reservations and continue the
    loading process.  Each card shows the box code, associated
    product, quantity of items and current status.  Clicking the
    "Apri" button navigates to the box detail view where the
    "Carica" action can be performed.  Completed boxes are still
    displayed but sorted below open boxes for convenience.
    """
    # Order boxes by status and creation time: show APERTO and IN_CARICO first
    try:
        # Use explicit ordering to group boxes by status.  SQLite sorts text
        # lexicographically, so we map statuses to numbers.  If the database
        # backend does not support case expressions this block may need
        # adjustment.
        from sqlalchemy import case
        status_order = case(
            (
                ProductionBox.status == 'APERTO', 0
            ),
            (
                ProductionBox.status == 'IN_CARICO', 1
            ),
            (
                ProductionBox.status == 'COMPLETATO', 2
            ),
            (
                ProductionBox.status == 'ARCHIVIATO', 3
            ),
            else_=4
        )
        boxes = (ProductionBox.query
                 .filter(ProductionBox.status.in_(['APERTO','IN_CARICO']))
                 .order_by(status_order, ProductionBox.created_at.desc())
                 .all())
    except Exception:
        # Fallback to a simple list when ordering fails (e.g. missing column)
        boxes = ProductionBox.query.filter(ProductionBox.status.in_(['APERTO','IN_CARICO'])).all()
    # For each box determine its component based on the DataMatrix code rather
    # than the product to properly display assembly names.  Parse the
    # datamatrix (DMV1|P=<component>|S=...|T=...) to extract the component
    # code (P=).  Use this name to look up the corresponding Structure and
    # assign the image accordingly.
    try:
        from ...models import Structure
        from flask import current_app
        # Build a prefix→filename map for the uploads directory once before
        # iterating over the boxes.  This avoids scanning the directory
        # repeatedly for each box when determining the appropriate image.
        upload_dir = os.path.join(current_app.static_folder, 'uploads')
        prefix_map: dict[str, str] = {}
        if os.path.isdir(upload_dir):
            try:
                for fname in os.listdir(upload_dir):
                    parts = fname.split('_', 2)
                    if len(parts) >= 2:
                        prefix = parts[0] + '_' + parts[1] + '_'
                        if prefix not in prefix_map:
                            prefix_map[prefix] = fname
            except Exception:
                prefix_map = {}
        for b in boxes:
            root_struct = None
            root_image = None
            try:
                # Ensure the box has at least one stock item
                if b.stock_items:
                    dm = b.stock_items[0].datamatrix_code or ''
                    parts = dm.split('|')
                    component_name = None
                    for part in parts:
                        if part.startswith('P='):
                            component_name = part.split('=', 1)[1]
                            break
                    if component_name:
                        root_struct = Structure.query.filter_by(name=component_name).first()
                        if root_struct:
                            # Attempt to locate an image file.  Use the fallback
                            # search logic based on structure id, component master
                            # and structure type.
                            # Use the prefix_map built outside the loop to select an image.
                            prefix_struct = f"sn_{root_struct.id}_"
                            root_image = prefix_map.get(prefix_struct)
                            if not root_image:
                                master = getattr(root_struct, 'component_master', None)
                                if master:
                                    prefix_master = f"cm_{master.id}_"
                                    root_image = prefix_map.get(prefix_master)
                            if not root_image:
                                stype = getattr(root_struct, 'type', None)
                                if stype:
                                    prefix_type = f"st_{stype.id}_"
                                    root_image = prefix_map.get(prefix_type)
            except Exception:
                root_struct = None
                root_image = None
            # Attach the root structure and image filename to the box for use in templates
            setattr(b, 'root_structure', root_struct)
            setattr(b, 'root_image_filename', root_image)
    except Exception:
        # On failure set properties to None for all boxes
        for b in boxes:
            setattr(b, 'root_structure', None)
            setattr(b, 'root_image_filename', None)

    # ------------------------------------------------------------------
    # Group boxes by their declared type so the UI can present dedicated
    # sections for assemblies, mechanical parts, commercial parts and
    # finished products.  Provide human-friendly labels, icons and short
    # descriptions for each category.  Any unexpected box types fall back
    # to an "Altri" bucket to avoid hiding data.
    category_definitions = [
        {
            'key': 'ASSIEME',
            'label': 'Assiemi',
            'icon': '🧩',
            'description': 'Assemblaggi in attesa di completamento.',
        },
        {
            'key': 'PARTE',
            'label': 'Parti',
            'icon': '⚙️',
            'description': 'Componenti meccanici prenotati per il carico.',
        },
        {
            'key': 'COMMERCIALE',
            'label': 'Parti a commercio',
            'icon': '🛒',
            'description': 'Articoli commerciali pronti per la produzione.',
        },
        {
            'key': 'PRODOTTO',
            'label': 'Prodotti',
            'icon': '📦',
            'description': 'Prodotti finiti da completare o collaudare.',
        },
    ]
    grouped_boxes: dict[str, list[ProductionBox]] = {c['key']: [] for c in category_definitions}
    uncategorised: list[ProductionBox] = []
    for box in boxes:
        box_key = getattr(box, 'box_type', None)
        if box_key in grouped_boxes:
            grouped_boxes[box_key].append(box)
        else:
            uncategorised.append(box)
    if uncategorised:
        category_definitions.append(
            {
                'key': 'ALTRO',
                'label': 'Altri',
                'icon': '🗃️',
                'description': 'Box con tipologie non classificate.',
            }
        )
        grouped_boxes['ALTRO'] = uncategorised

    # Determine which tab should be active.  Accept a ``tab`` query parameter
    # and validate it against the known category keys.  When a ``box`` query
    # parameter is provided, use the corresponding box type as a fallback so
    # that deep links like ``/production?box=123`` automatically reveal the
    # correct section.
    requested_tab = request.args.get('tab', type=str)
    requested_box_id = request.args.get('box', type=int)
    valid_keys = {c['key'] for c in category_definitions}
    initial_tab = None
    if requested_tab:
        candidate = (requested_tab or '').strip().upper()
        if candidate in valid_keys:
            initial_tab = candidate
    focused_box = None
    if requested_box_id:
        for b in boxes:
            try:
                if b.id == requested_box_id:
                    focused_box = b
                    break
            except Exception:
                continue
    if focused_box and not initial_tab:
        candidate = getattr(focused_box, 'box_type', None)
        if candidate in valid_keys:
            initial_tab = candidate

    return render_template(
        'production/index.html',
        boxes=boxes,
        categories=category_definitions,
        grouped_boxes=grouped_boxes,
        active_module='production',
        initial_tab=initial_tab,
        focused_box_id=requested_box_id,
    )


@production_bp.route('/add/<int:part_id>', methods=['POST'])
@login_required
def add_stock(part_id: int):
    """Increase the on‑hand quantity of a part or commercial component.

    Accepts a quantity from the form, updates the structure's
    ``quantity_in_stock``, and propagates the change up to parent
    assemblies.  Displays success or warning messages accordingly.
    """
    part = Structure.query.get_or_404(part_id)
    qty_str = request.form.get('quantity', '0')
    try:
        qty = float(qty_str)
        if qty <= 0:
            flash('La quantità deve essere maggiore di zero.', 'warning')
            return redirect(url_for('production.index'))
    except ValueError:
        flash('Inserisci una quantità valida.', 'warning')
        return redirect(url_for('production.index'))
    # Determine the new global quantity for this component.  Components
    # identified by the same master component (component_id) or by the same
    # name represent the same physical part in the warehouse.  When stock
    # is added to one of these components, update all matching structures
    # to reflect the new total on‑hand quantity rather than maintaining
    # separate absolute quantities.  Compute the new quantity by adding
    # the requested amount to the current quantity of the target part.  Then
    # assign this value to every structure sharing the same component
    # identifier or name.
    # When adding stock to a component, multiple Structure rows may represent
    # the same physical item (via shared component_id or name).  If their
    # quantities differ, simply adding the requested qty to the quantity of
    # the selected part may lower the stock of other matches when their
    # quantity is greater.  To avoid this problem, compute the highest
    # quantity among all matching structures, then add the requested qty and
    # apply the result to every match.
    # Build the list of all structures that represent the same physical component.
    # When component_id is set, include both component_id matches and name matches
    # to cover legacy records lacking a component_id.  Use a dictionary keyed on
    # structure id to deduplicate results from both queries.  If component_id is
    # not set, fall back to name-based matching only.
    matches_dict: dict[int, Structure] = {}
    try:
        # Always include name-based matches
        try:
            for s in Structure.query.filter(Structure.name == part.name).all():
                matches_dict[s.id] = s
        except Exception:
            pass
        # Include component_id matches when defined
        if part.component_id:
            try:
                for s in Structure.query.filter(Structure.component_id == part.component_id).all():
                    matches_dict[s.id] = s
            except Exception:
                pass
    except Exception:
        matches_dict = {}
    matches = list(matches_dict.values())
    # Collect current quantities across all matching structures
    quantities: list[float] = []
    for s in matches:
        try:
            quantities.append(float(s.quantity_in_stock or 0))
        except Exception:
            pass
    # Include the target part's quantity if no matches found
    if not quantities:
        try:
            quantities.append(float(part.quantity_in_stock or 0))
        except Exception:
            quantities.append(0)
    current_qty: float = max(quantities) if quantities else 0.0
    new_qty: float = current_qty + qty
    # Apply the new quantity to the target part and all matches
    part.quantity_in_stock = new_qty
    for s in matches:
        s.quantity_in_stock = new_qty
    # Commit the change so that parent assemblies can compute updated stock
    db.session.commit()
    # Record a log entry for the stock addition.  Determine the category of
    # the part based on its flags (assembly parts are not included here).
    try:
        if getattr(part, 'flag_part', False):
            category = 'parts'
        elif getattr(part, 'flag_commercial', False):
            category = 'commercial'
        else:
            category = 'unknown'
        log = InventoryLog(
            user_id=current_user.id,
            structure_id=part.id,
            category=category,
            action=f'Caricati {qty:.0f}',
            quantity=qty
        )
        db.session.add(log)
        db.session.commit()
    except Exception:
        # If logging fails, ignore the error to avoid blocking the main flow
        pass
    _update_parent_assemblies(part)
    flash(f'Aggiunte {qty:.0f} unità di {part.name} al magazzino.', 'success')
    # After adding stock, redirect back to the page that submitted the form.  Use
    # the HTTP referrer when available so that loading stock from the warehouse
    # pages keeps the user on the same page.  To improve usability when
    # reloading warehouse listings, append an anchor corresponding to the part
    # identifier (e.g. ``#part-42``).  This ensures that the browser scrolls
    # back to the row that triggered the update rather than resetting the view.
    # The ``Referer`` header does not include URL fragments, so we always
    # append a new fragment to the recorded URL.  If no referrer is available
    # (e.g. direct POST requests), fall back to the production dashboard without
    # an anchor.
    referrer = request.referrer
    if referrer:
        # Strip any existing fragment from the referrer before appending our own.
        base_ref = referrer.split('#', 1)[0]
        # Build a fragment pointing to the updated part.  All warehouse
        # templates use the ``id="part-<id>"`` convention for row anchors.
        anchor = f"#part-{part.id}"
        return redirect(f"{base_ref}{anchor}")
    # When no referrer is present, redirect to the production dashboard.
    return redirect(url_for('production.index'))


# -----------------------------------------------------------------------------
# Additional views for the production module
#
# The production dashboard lists all open production boxes.  Each card links
# to a detailed view of the selected box.  Historically the detailed view
# lived under the inventory blueprint (``inventory.production_box_view``),
# which caused the navigation bar to highlight "Magazzino" (warehouse) even
# when the user arrived there from the production section.  To ensure the
# correct tab is highlighted, provide a thin wrapper around the existing
# inventory view but expose it under the production blueprint.  This wrapper
# delegates the heavy lifting to the inventory implementation while letting
# Flask record the current endpoint as ``production.production_box_view``.  The
# base template uses the blueprint name from ``request.endpoint`` to mark the
# active module, so exposing the view via the production blueprint resolves
# the mis-highlighted tab.

@production_bp.route('/production_box/<int:box_id>')
@login_required
def production_box_view(box_id: int):
    """Render the production box detail page within the production module.

    This wrapper calls the underlying inventory production box view to
    assemble the page context and template.  By routing through the
    production blueprint the request endpoint becomes ``production.production_box_view``,
    ensuring that the navigation bar highlights the production tab instead
    of the inventory tab.  The inventory implementation is reused to avoid
    duplicating business logic.  If the import fails or the function raises
    an exception the error will propagate to Flask's error handlers.

    :param box_id: Primary key of the ProductionBox to display.
    :return: Response returned by the inventory view function.
    """
    # Import lazily to avoid circular dependencies during blueprint
    # registration.  Use the full import path relative to this module.
    from ...blueprints.inventory.routes import production_box_view as _inv_production_box_view
    return _inv_production_box_view(box_id)