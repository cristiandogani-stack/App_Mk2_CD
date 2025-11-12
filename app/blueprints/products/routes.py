from flask import Blueprint, render_template, request, redirect, url_for, flash, abort, current_app
from flask_login import login_required
from flask import jsonify
from flask import send_from_directory, send_file
from ...extensions import db, csrf
from ...models import (
    Product,
    ProductComponent,
    Structure,
    StructureType,
    Supplier,
    WorkCenter,
    WorkPhase,
    Reservation,
    StockItem,
)

# Import checklist helpers
from ...checklist import load_checklist, toggle_flag

# Import helper to create or attach a ComponentMaster for a structure.
from ..admin.routes import ensure_component_master_for_structure

from sqlalchemy import or_, func
import shutil  # For copying uploaded documents into multiple destinations
import re      # Added for normalising filenames and base names
import json
import os
from werkzeug.utils import secure_filename

# Blueprint for product management
products_bp = Blueprint('products', __name__, template_folder='../../templates')

# -----------------------------------------------------------------------------
# Helper functions
#
# The notes fields on both ComponentMaster and ProductComponent may contain
# JSON-encoded data to store additional metadata such as the ``lot_management``
# flag alongside user‑supplied notes.  When displaying notes back to the
# user, however, only the plain text portion should be shown.  The helper
# below attempts to parse a JSON string and extract the ``notes`` key.  If
# parsing fails or the input is not JSON, the original string is returned.
def extract_plain_notes(note_str: str | None) -> str | None:
    """Return the plain notes string from a JSON‑encoded notes field.

    When notes are stored as a JSON object containing at least a ``notes``
    key (and possibly other metadata like ``lot_management``), this helper
    returns the value of the ``notes`` key.  If the input is not a JSON
    object or parsing fails, the original value is returned unchanged.

    :param note_str: the stored notes string (possibly JSON)
    :return: the plain notes string or the original value when parsing fails
    """
    if not note_str or not isinstance(note_str, str):
        return note_str
    try:
        parsed = json.loads(note_str)
        if isinstance(parsed, dict):
            # Return the 'notes' key when present; default to empty string
            return parsed.get('notes', '')
    except Exception:
        # Not JSON or failed parsing; return the original string
        return note_str
    # In all other cases return the input string
    return note_str


@products_bp.route('/toggle_checklist', methods=['POST'])
@login_required
@csrf.exempt  # Allow AJAX without CSRF token
def toggle_checklist_route():
    """Toggle the inclusion of a document in the build/load checklist.

    This endpoint is invoked via JavaScript when a user checks or
    unchecks the "Check list" box next to a document in the product
    anagrafiche views.  The request payload must contain JSON
    attributes ``structure_id`` (the ID of the Structure) and
    ``doc_path`` (the relative static path of the document).  A
    boolean ``flag`` indicates whether the document should be added to
    (True) or removed from (False) the checklist.  When the flag is
    updated successfully a JSON response with ``success: true`` is
    returned.  Malformed requests yield a 400 response.
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        structure_id = data.get('structure_id')
        doc_path = data.get('doc_path')
        flag = data.get('flag')
        # Validate inputs: structure_id must be an integer convertible value,
        # doc_path must be a non-empty string, flag must be boolean.
        if structure_id is None or doc_path is None or flag is None:
            return jsonify({'success': False}), 400
        try:
            sid = int(structure_id)
        except (TypeError, ValueError):
            return jsonify({'success': False}), 400
        if not isinstance(doc_path, str) or not doc_path.strip():
            return jsonify({'success': False}), 400
        # Coerce flag to boolean
        flag_value = bool(flag)
        # Persist the flag change
        toggle_flag(sid, doc_path.strip(), flag_value)
        return jsonify({'success': True})
    except Exception:
        # Unexpected errors are handled gracefully
        return jsonify({'success': False}), 400

@products_bp.route('/search_suggestions')
@login_required
def search_suggestions():
    """Return JSON search suggestions for product components.

    By default the returned list contains up to 10 components whose
    structure name or description matches the query ``q`` regardless of
    the product they belong to.  When a ``product_id`` query parameter
    is provided the search is limited to components belonging to that
    product.  Each suggestion includes a label combining the component
    and product names along with identifiers (product_id, component_id
    and category) that the frontend uses to build a redirect URL.
    """
    query = request.args.get('q', '').strip()
    # Optional product filter; if not convertible to int the value is ignored.
    product_filter = request.args.get('product_id')
    try:
        product_filter = int(product_filter) if product_filter is not None else None
    except (TypeError, ValueError):
        product_filter = None

    suggestions: list[dict[str, object]] = []
    if query:
        # Build patterns for case-insensitive matching.  The first pattern looks
        # for the query as typed anywhere in the target fields.  The second
        # pattern removes spaces from the query and matches against the target
        # fields with spaces removed to allow flexible spacing (e.g. "vite1"
        # matches "vite 1").
        like = f"%{query}%"
        like_no_space = f"%{query.replace(' ', '')}%"
        # Base query joining ProductComponent, Structure and Product.
        q = (
            db.session.query(ProductComponent, Structure, Product)
            .join(Structure, ProductComponent.structure_id == Structure.id)
            .join(Product, ProductComponent.product_id == Product.id)
            # Apply filters on name and descriptions of structure and component.
            .filter(
                or_(
                    Structure.name.ilike(like),
                    Structure.description.ilike(like),
                    func.replace(Structure.name, ' ', '').ilike(like_no_space),
                    func.replace(Structure.description, ' ', '').ilike(like_no_space),
                    # Also search in component descriptions and notes
                    ProductComponent.description.ilike(like),
                    ProductComponent.notes.ilike(like),
                    func.replace(func.coalesce(ProductComponent.description, ''), ' ', '').ilike(like_no_space),
                    func.replace(func.coalesce(ProductComponent.notes, ''), ' ', '').ilike(like_no_space),
                )
            )
        )
        # Apply optional product filter to restrict suggestions to a single product.
        if product_filter is not None:
            q = q.filter(ProductComponent.product_id == product_filter)
        # Order by name and limit results to reduce load on the client.
        results = q.order_by(Structure.name).limit(10).all()
        for comp, struct, prod in results:
            # Determine the category based on structure flags
            if struct.flag_assembly:
                category = 'assembly'
            elif struct.flag_part:
                category = 'part'
            elif struct.flag_commercial:
                category = 'commercial'
            else:
                # Fallback: default to part if no flag is set
                category = 'part'
            label = f"{struct.name} - {prod.name}"
            # Include image URL if available on the ProductComponent.  When
            # ``image_filename`` is None a null value is returned and the
            # frontend can display a placeholder.  Use url_for to build
            # the static path relative to the ``uploads`` directory.
            image_url: str | None = None
            try:
                if comp.image_filename:
                    image_url = url_for('static', filename='uploads/' + comp.image_filename)
            except Exception:
                image_url = None
            suggestions.append({
                'label': label,
                'product_id': prod.id,
                'component_id': comp.id,
                'category': category,
                'image': image_url,
            })
    return jsonify(suggestions)

# -----------------------------------------------------------------------------
# Revision endpoints
#
# Components (structures) can be revised via the component detail view.  When a
# user clicks the "Revisiona" button the corresponding Structure record
# increments its revision index.  The revision index maps to a letter
# (A–Z) displayed in the UI.  Revisions beyond 26 are clamped to Z.

@products_bp.route('/structure/<int:structure_id>/revise', methods=['POST'])
@login_required
def revise_structure(structure_id: int):
    """Increment the revision index for a structure and redirect back.

    This endpoint receives a POST request from the component detail page
    when the user clicks the "Revisiona" button.  The structure's
    revision field is incremented by one (capped at 26), the change is
    committed to the database and the user is redirected to the page
    specified in the ``next`` query parameter or to the component
    detail page if unspecified.
    """
    # Ensure the revision and compatible_revisions columns exist on the
    # structures table.  Some older databases may lack these columns
    # because migrations are performed dynamically.  Use PRAGMA table_info
    # and ALTER TABLE statements to add the columns when missing.  This
    # check runs quickly and does nothing if the columns already exist.
    try:
        from sqlalchemy import text
        conn = db.engine.connect()
        cols = [row[1] for row in conn.execute(text('PRAGMA table_info(structures)')).fetchall()]
        if 'revision' not in cols:
            conn.execute(text('ALTER TABLE structures ADD COLUMN revision INTEGER DEFAULT 0'))
        if 'compatible_revisions' not in cols:
            conn.execute(text('ALTER TABLE structures ADD COLUMN compatible_revisions TEXT'))
        conn.close()
    except Exception:
        # Ignore any errors; the columns may already exist or be added via admin helpers
        pass

    structure = Structure.query.get_or_404(structure_id)
    # Increment the revision index (default to 0 if None)
    try:
        current = int(structure.revision or 0)
    except Exception:
        current = 0
    new_value = current + 1
    # Clamp to 26 revisions (A–Z)
    if new_value > 26:
        new_value = 26
    structure.revision = new_value
    # -----------------------------------------------------------------
    # Save any compatible revisions selections submitted with the
    # revision form.  The revision form includes checkboxes or a multi‑select
    # named "compatible_revisions" allowing the user to designate prior
    # versions that remain compatible.  Capture the selections using
    # request.form.getlist() and persist them as a comma‑separated string.
    try:
        # Retrieve all submitted revision values.  The form uses multiple
        # checkboxes named ``compatible_revisions`` to represent each prior
        # revision.  Use getlist() to collect multiple checkbox values.  If
        # no list values are returned (some browsers submit a comma‑separated
        # string for multi‑selects), split the raw value on commas.
        # The current revision is always selected in the UI and therefore
        # included in compat_vals, but it should never be stored in the
        # compatible_revisions column.  Treat the current revision
        # specially by filtering it out.
        compat_vals = request.form.getlist('compatible_revisions') or []
        if not compat_vals:
            raw_val = request.form.get('compatible_revisions')
            if raw_val:
                compat_vals = [v.strip() for v in raw_val.split(',') if v.strip()]
        # Determine if the input was provided at all.  When the field is
        # absent (no checkboxes submitted) we leave the existing value
        # unchanged; when present but empty (no selections besides the
        # current revision) we clear the stored compatible revisions.
        if 'compatible_revisions' in request.form or 'current_revision' in request.form or compat_vals:
            # Build a list of selected prior revisions by iterating over
            # the structure's revision letters.  Skip the current revision
            # (revision_label) so it is never persisted.  Use a list to
            # preserve the order of revisions as they appear in the UI.
            selected_prev: list[str] = []
            for letter in structure.revision_letters:
                # Skip the current revision; it is always considered
                # compatible implicitly and should not be stored.
                if letter == structure.revision_label:
                    continue
                if letter in compat_vals:
                    selected_prev.append(letter)
            # Persist the list as a comma‑separated string or clear it
            # entirely when no previous revisions are selected.
            structure.compatible_revisions = ','.join(selected_prev) if selected_prev else None
    except Exception:
        # Ignore errors; compatibility selection is optional
        pass
    # Persist the change
    db.session.commit()

    # -----------------------------------------------------------------
    # After incrementing the revision and saving any compatibility data,
    # replicate the folder structure associated with this component for
    # the new revision.  Each component has a dedicated folder for its
    # attachments (either under static/tmp_components when a master exists
    # or within static/documents/<product>/<structure_path> when defined
    # per product).  When revising, create a new folder at the same
    # level with the revision suffix appended to the safe component name
    # (e.g. "chiodo" becomes "chiodo_Rev_A").  Copy only the directory
    # structure without any files.  This preserves the organisation of
    # subfolders (qualita, step_tavole, etc.) for the new revision while
    # leaving the existing files untouched.
    try:
        from werkzeug.utils import secure_filename
        import os
        # Determine the safe base name for this structure
        safe_name = secure_filename(structure.name) or 'unnamed'
        # Compute the revision letter (e.g. 'A') from the revision label
        rev_label = structure.revision_label  # e.g. 'Rev.A'
        rev_letter = None
        if isinstance(rev_label, str) and '.' in rev_label:
            try:
                rev_letter = rev_label.split('.')[-1]
            except Exception:
                rev_letter = None
        # Only proceed when a revision letter is determined
        if rev_letter:
            new_folder_name = f"{safe_name}_Rev_{rev_letter}"
            static_root = current_app.static_folder
            # 1. Replicate under tmp_components when a master exists (folder name is based on component name)
            tmp_components_root = os.path.join(static_root, 'tmp_components')
            old_tmp_dir = os.path.join(tmp_components_root, safe_name)
            new_tmp_dir = os.path.join(tmp_components_root, new_folder_name)
            if os.path.isdir(old_tmp_dir) and not os.path.exists(new_tmp_dir):
                for dirpath, dirnames, filenames in os.walk(old_tmp_dir):
                    # Compute relative path from the old directory
                    rel = os.path.relpath(dirpath, old_tmp_dir)
                    dest_dir = os.path.join(new_tmp_dir, rel)
                    try:
                        os.makedirs(dest_dir, exist_ok=True)
                    except Exception:
                        pass
            # 2. Replicate within static/documents for each product referencing this structure
            #    Determine all ProductComponents that use this structure to identify
            #    the product directories and structure paths.
            pcs = ProductComponent.query.filter_by(structure_id=structure.id).all()
            for pc in pcs:
                try:
                    prod = pc.product
                    if not prod:
                        continue
                    prod_safe = secure_filename(prod.name) or 'unnamed'
                    # Build the structure path from the root of the hierarchy down to this structure
                    path_parts: list[str] = []
                    def _traverse_path(s):
                        if s.parent:
                            _traverse_path(s.parent)
                        path_parts.append(secure_filename(s.name) or 'unnamed')
                    path_parts.clear()
                    _traverse_path(structure)
                    # Old path is the full hierarchy under documents
                    base_doc_root = os.path.join(static_root, 'documents', prod_safe)
                    old_doc_dir = os.path.join(base_doc_root, *path_parts)
                    # New path: replace the last element (safe_name) with new_folder_name
                    if path_parts:
                        new_path_parts = path_parts[:-1] + [new_folder_name]
                    else:
                        new_path_parts = [new_folder_name]
                    new_doc_dir = os.path.join(base_doc_root, *new_path_parts)
                    if os.path.isdir(old_doc_dir) and not os.path.exists(new_doc_dir):
                        for dirpath, dirnames, filenames in os.walk(old_doc_dir):
                            rel = os.path.relpath(dirpath, old_doc_dir)
                            dest_dir = os.path.join(new_doc_dir, rel)
                            try:
                                os.makedirs(dest_dir, exist_ok=True)
                            except Exception:
                                pass
                except Exception:
                    # Ignore errors when computing product paths
                    continue
    except Exception:
        # Ignore any errors during folder replication; missing directories are expected
        pass
    # Determine where to redirect back to.  Accept both 'next' and 'return_to'
    # parameters for compatibility with existing patterns in the templates.
    return_to = request.form.get('next') or request.args.get('next') or request.form.get('return_to') or request.args.get('return_to')
    # Fallback to the component detail if no return_to provided
    if not return_to:
        # Attempt to find a ProductComponent referencing this structure; if
        # found redirect to its component detail page.  Otherwise go to
        # the inventory list.  This fallback ensures the revision button
        # always redirects somewhere sensible even when called directly.
        pc = ProductComponent.query.filter_by(structure_id=structure.id).first()
        if pc:
            return redirect(url_for('products.component_detail', product_id=pc.product_id, component_id=pc.id))
        else:
            return redirect(url_for('inventory.index'))
    return redirect(return_to)

@products_bp.route('/')
@login_required
def index():
    """Show a list of all products."""
    products = Product.query.order_by(Product.created_at.desc()).all()
    # Calcola il peso totale dei componenti per ciascun prodotto.  La somma
    # considera solo i componenti che hanno un peso definito.  Per ogni
    # componente il peso totale è il peso per pezzo moltiplicato per la
    # quantità.
    for p in products:
        total_weight = 0.0
        for comp in p.components:
            # Prefer weight from the master component when available.  Fall back
            # to the per‑component override if the master has no weight defined.
            comp_weight = None
            try:
                if getattr(comp, 'component_master', None) and comp.component_master and comp.component_master.weight is not None:
                    comp_weight = comp.component_master.weight
                elif comp.weight is not None:
                    comp_weight = comp.weight
            except Exception:
                comp_weight = comp.weight if hasattr(comp, 'weight') else None
            if comp_weight is not None and comp.quantity is not None:
                try:
                    total_weight += float(comp_weight) * (comp.quantity or 0)
                except Exception:
                    # Se i valori non sono numerici, ignora il componente
                    pass
        # Attribuisci il peso totale come proprietà dinamica sull'istanza
        p.total_weight = total_weight
    return render_template('products/index.html', products=products)


# Product editing
@products_bp.route('/<int:id>/edit', methods=['GET', 'POST'])
@login_required
def edit(id: int):
    product = Product.query.get_or_404(id)
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        image_file = request.files.get('image')

        # -----------------------------------------------------------------
        # Extract extended product attributes
        #
        # Additional fields such as flow rate, maximum pressure and
        # dimensions are parsed from the form.  Numeric values are
        # converted to floats when possible.  Empty inputs result in
        # ``None`` to avoid overriding existing values with zero or
        # empty strings.  The curve image (if provided) is handled
        # separately via ``curve_file`` below.
        def _parse_float(value):
            if value is None:
                return None
            value = value.strip()
            if not value:
                return None
            try:
                return float(value.replace(',', '.'))
            except (ValueError, TypeError):
                return None

        flow_rate_val = _parse_float(request.form.get('flow_rate'))
        # For backward compatibility a single max_pressure value may be provided.  When
        # using the new two-field format (from/to) these values will be used instead.
        max_pressure_val = _parse_float(request.form.get('max_pressure'))
        dimension_x_val = _parse_float(request.form.get('dimension_x'))
        dimension_y_val = _parse_float(request.form.get('dimension_y'))
        dimension_z_val = _parse_float(request.form.get('dimension_z'))

        # Capture the optional curve image upload; this field will be
        # processed after validation and assignment of other attributes.
        curve_file = request.files.get('curve_image')

        # Additional product specifications parsed from the form.  Strings are
        # stripped of whitespace and stored as-is.  Numeric values are
        # converted to floats when provided.  Empty strings are treated as
        # ``None`` so that unspecified values do not override existing
        # database values.
        fluid_in_let_val = request.form.get('fluid_in_let')
        if fluid_in_let_val is not None:
            fluid_in_let_val = fluid_in_let_val.strip() or None
        layer_in_lett_val = request.form.get('layer_in_lett')
        if layer_in_lett_val is not None:
            layer_in_lett_val = layer_in_lett_val.strip() or None
        noise_val = _parse_float(request.form.get('noise'))
        max_pressure_from_val = _parse_float(request.form.get('max_pressure_from'))
        max_pressure_to_val = _parse_float(request.form.get('max_pressure_to'))

        def allowed_file(filename: str) -> bool:
            return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'png', 'jpg', 'jpeg', 'gif', 'bmp'}

        if not name:
            flash('Il nome del prodotto è obbligatorio.', 'danger')
        else:
            product.name = name
            product.description = description
            # If a new image was uploaded, save it and update filename
            if image_file and allowed_file(image_file.filename):
                filename = secure_filename(image_file.filename)
                filename = f"{product.id}_{filename}"
                upload_dir = os.path.join(current_app.static_folder, 'uploads')
                os.makedirs(upload_dir, exist_ok=True)
                path = os.path.join(upload_dir, filename)
                image_file.save(path)
                product.image_filename = filename

            # Apply extended attribute values only when provided
            if flow_rate_val is not None:
                product.flow_rate = flow_rate_val
            # Legacy single max pressure value.  When from/to values are
            # provided they take precedence; however, to avoid losing data
            # from previous versions we still update max_pressure when
            # explicitly set.
            if max_pressure_val is not None:
                product.max_pressure = max_pressure_val
            if dimension_x_val is not None:
                product.dimension_x = dimension_x_val
            if dimension_y_val is not None:
                product.dimension_y = dimension_y_val
            if dimension_z_val is not None:
                product.dimension_z = dimension_z_val

            # Save additional specifications
            if fluid_in_let_val is not None:
                product.fluid_in_let = fluid_in_let_val
            if layer_in_lett_val is not None:
                product.layer_in_lett = layer_in_lett_val
            if noise_val is not None:
                product.noise = noise_val
            if max_pressure_from_val is not None:
                product.max_pressure_from = max_pressure_from_val
            if max_pressure_to_val is not None:
                product.max_pressure_to = max_pressure_to_val

            # Save the curve image if provided
            if curve_file and allowed_file(curve_file.filename):
                curve_filename = secure_filename(curve_file.filename)
                curve_filename = f"{product.id}_curve_{curve_filename}"
                upload_dir = os.path.join(current_app.static_folder, 'uploads')
                os.makedirs(upload_dir, exist_ok=True)
                curve_path = os.path.join(upload_dir, curve_filename)
                curve_file.save(curve_path)
                product.curve_image_filename = curve_filename

            db.session.commit()
            flash('Prodotto aggiornato.', 'success')
            return redirect(url_for('products.detail', id=product.id))
    return render_template('products/edit.html', product=product)


# Product deletion
@products_bp.route('/<int:id>/delete', methods=['POST'])
@login_required
def delete(id: int):
    product = Product.query.get_or_404(id)
    # Before deleting the product, explicitly remove any dependent reservations
    # and stock items.  Without this step SQLAlchemy will attempt to nullify
    # the foreign keys on reservation and stock_item rows which violates
    # NOT NULL constraints (product_id cannot be NULL).  Loop through
    # reservations referenced by the product (via the backref defined on
    # Reservation.product) and delete both the reservation and its stock items.
    try:
        # Make a copy of the reservations list to avoid modification during iteration
        for reservation in list(product.reservations):
            # Delete any stock items tied to this reservation first
            for si in list(reservation.stock_items):
                db.session.delete(si)
            db.session.delete(reservation)
        # Remove any remaining stock items tied directly to the product
        other_items = StockItem.query.filter_by(product_id=product.id).all()
        for si in other_items:
            try:
                db.session.delete(si)
            except Exception:
                # If deletion fails for a particular item, continue deleting others
                db.session.rollback()
        db.session.commit()
    except Exception:
        db.session.rollback()
        # Even if reservation cleanup fails, proceed to delete product to avoid inconsistent state
    # Capture unique structure identifiers associated with this product's components before deletion.
    structure_ids = {comp.structure_id for comp in product.components if comp.structure_id is not None}
    # Delete the product and its components via cascade
    db.session.delete(product)
    db.session.commit()
    # After removing the product and its components, remove any structures (and their
    # children) that are no longer referenced by other products.  Also delete
    # structure types when no structures remain.  This ensures that deleting
    # a product cleans up its entire BOM structure tree.
    def _delete_structure_tree(node: Structure):
        """Recursively delete a structure node and all its descendants along with
        their product components.  Does not check for other references – callers
        must ensure the node is safe to remove."""
        for child in list(node.children):
            _delete_structure_tree(child)
        # Remove any product components referencing this structure
        comps = ProductComponent.query.filter_by(structure_id=node.id).all()
        for comp in comps:
            db.session.delete(comp)
        db.session.delete(node)
    for s_id in structure_ids:
        struct = Structure.query.get(s_id)
        if not struct:
            continue
        # If any other product references this structure, skip deletion
        other_refs = ProductComponent.query.filter_by(structure_id=struct.id).count()
        if other_refs > 0:
            continue
        try:
            stype = struct.type
            _delete_structure_tree(struct)
            db.session.commit()
            # After deleting the structure tree, check if its type has any remaining nodes
            if stype:
                remaining_nodes = Structure.query.filter_by(type_id=stype.id).count()
                if remaining_nodes == 0:
                    db.session.delete(stype)
                    db.session.commit()
        except Exception:
            db.session.rollback()
            # Continue cleanup for other structures but do not abort deletion
            continue
    flash('Prodotto eliminato insieme alla sua struttura.', 'success')
    return redirect(url_for('products.index'))

# Endpoint to remove a component from a product.  This deletes the
# ProductComponent entry linking a product to a structure.  Only
# authenticated users can access it.  After deletion the user is
# returned to the product detail page.  Additionally, if the underlying
# structure node is no longer referenced by any other product component
# and has no child nodes, the structure and potentially its type are
# removed from the database.  This keeps the structures hierarchy in
# sync with the products catalogue.
@products_bp.route('/<int:product_id>/component/delete/<int:component_id>', methods=['POST'])
@login_required
def delete_component(product_id: int, component_id: int):
    product = Product.query.get_or_404(product_id)
    comp = ProductComponent.query.get_or_404(component_id)
    # Ensure the component belongs to this product
    if comp.product_id != product.id:
        abort(403)
    # Preserve reference to the structure before deletion
    structure = comp.structure
    # Delete the product component
    db.session.delete(comp)
    db.session.commit()
    # After removing the component, attempt to clean up the underlying
    # structure node and its type if orphaned.  A structure can be
    # deleted only when no other product components reference it and it
    # has no child nodes.  If, after deletion of the node, the type
    # has no remaining nodes, the type itself will be removed.  To
    # avoid breaking referential integrity, we perform these checks
    # after committing the component deletion.
    if structure is not None:
        try:
            # Refresh structure state from the database.  If the record
            # was already deleted elsewhere this will return None.
            struct_obj = Structure.query.get(structure.id)
            if struct_obj:
                # Count how many components still reference this structure
                other_count = ProductComponent.query.filter_by(structure_id=struct_obj.id).count()
                # Delete only if no other references exist and it has no children
                if other_count == 0 and (not struct_obj.children or len(struct_obj.children) == 0):
                    stype = struct_obj.type
                    db.session.delete(struct_obj)
                    db.session.commit()
                    # After deleting the node, check if the type has any remaining nodes
                    if stype is not None:
                        # Use a fresh count from the database; subtract one because we just
                        # deleted struct_obj.
                        remaining = Structure.query.filter_by(type_id=stype.id).count()
                        if remaining == 0:
                            db.session.delete(stype)
                            db.session.commit()
        except Exception:
            # Silently ignore cleanup errors to avoid interrupting user workflow
            db.session.rollback()
    flash('Componente eliminato.', 'success')
    return redirect(url_for('products.detail', id=product.id))

# -----------------------------------------------------------------------------
# Document download helper
#
# When users click on a document link in the UI the file should download
# instead of being rendered in the browser.  Using the built‑in static file
# handler with a ``download`` attribute on the anchor tag sometimes causes
# files to open directly depending on the browser and file type.  To ensure
# consistent download behaviour we expose a dedicated endpoint that serves
# files located under the application's static folder using
# ``send_from_directory`` with ``as_attachment=True``.  The route is
# protected by login to prevent unauthorised access to uploaded documents.
@products_bp.route('/download/<path:filename>')
@login_required
def download_file(filename: str):
    """Serve a file from the static directory as an attachment.

    The ``filename`` parameter should be a path relative to the
    ``static`` folder.  For example ``documents/myproduct/folder/file.pdf``.
    ``send_from_directory`` sanitises the path and prevents directory
    traversal.  Setting ``as_attachment=True`` instructs the browser to
    download the file rather than display it inline.
    """
    # Only serve files from within the static folder.  ``send_from_directory``
    # will raise NotFound if the file does not exist or attempts to escape
    # the specified directory.
    # Build absolute path to the requested file.  We join the provided relative
    # path against the static folder to avoid directory traversal.  If the
    # resulting path does not point to a file, ``send_file`` will raise a
    # ``NotFound`` error which Flask converts into a 404 response.
    try:
        # ``filename`` may include nested directories; join it to the static folder.
        file_path = os.path.join(current_app.static_folder, filename)
        # Use ``os.path.abspath`` to normalise the path and ensure it remains
        # within the static folder.  This prevents directory traversal beyond
        # the static directory.
        static_root = os.path.abspath(current_app.static_folder)
        abs_path = os.path.abspath(file_path)
        # Only allow serving files that reside within the static directory.
        if not abs_path.startswith(static_root):
            abort(404)
        # Serve the file as attachment with the original filename.  The
        # ``download_name`` argument ensures the client receives the correct
        # filename with its extension rather than the entire relative path.
        return send_file(abs_path, as_attachment=True, download_name=os.path.basename(abs_path))
    except Exception:
        # Fall back to 404 if anything goes wrong
        abort(404)


# -----------------------------------------------------------------------------
# Bulk image download helper
#
# When a product contains many components the browser must request each image
# individually.  To offer an alternative "caricamento unico", this endpoint
# bundles all component images for a product into a single ZIP archive.  The
# archive is streamed directly from memory to avoid creating temporary files on
# disk.  Components without images are skipped.  If no images exist the
# resulting archive will be empty.  Access is protected by login.
@products_bp.route('/<int:product_id>/download_images')
@login_required
def download_images(product_id: int):
    """Return a zip file containing all images for the given product's components.

    The zip archive includes every ``component.image_filename`` associated with
    the product.  When a component lacks its own image, the underlying
    structure image is used if present.  Each file is stored in the archive
    under its original filename to preserve context.  If an image file is
    missing on disk it is silently skipped.  On success the archive is
    returned as an attachment with a name based on the product ID.
    """
    # Look up the product or return 404
    product = Product.query.get_or_404(product_id)
    upload_dir = os.path.join(current_app.static_folder, 'uploads')
    # Collect absolute file paths and names for all images
    images: list[tuple[str, str]] = []
    for comp in product.components:
        fname = None
        # Prefer the component image, fallback to structure image
        if getattr(comp, 'image_filename', None):
            fname = comp.image_filename
        else:
            struct = getattr(comp, 'structure', None)
            if struct is not None and getattr(struct, 'image_filename', None):
                fname = struct.image_filename
        if fname:
            abs_path = os.path.join(upload_dir, fname)
            if os.path.isfile(abs_path):
                images.append((abs_path, fname))
    from io import BytesIO
    import zipfile
    mem_zip = BytesIO()
    with zipfile.ZipFile(mem_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
        for abs_path, name in images:
            try:
                zf.write(abs_path, arcname=name)
            except Exception:
                # Skip files that cannot be read
                continue
    mem_zip.seek(0)
    archive_name = f"product_{product.id}_images.zip"
    return send_file(mem_zip, download_name=archive_name,
                     as_attachment=True, mimetype='application/zip')


# ---------------------------------------------------------------------------
# Document deletion endpoint

@products_bp.route('/delete_document', methods=['POST'])
@login_required
def delete_document():
    """
    Remove a previously uploaded document from the static folder.

    This endpoint accepts a POST request with two form fields:

    * ``filepath`` – the path of the file relative to the ``static`` folder
      (e.g. ``documents/MyProduct/qualita/manuale.pdf``).
    * ``next`` – optional URL to redirect to after deletion.  If absent,
      the HTTP referrer is used; when the referrer is unavailable the
      user is redirected to the product index page.

    The function validates that the specified file resides within the
    application's static directory before attempting to delete it.  On
    successful deletion a flash message is shown to the user.  Errors
    during deletion are ignored.  The response is always a redirect.
    """
    filepath = request.form.get('filepath', '').strip()
    # Determine where to redirect after handling deletion
    next_url = request.form.get('next') or request.referrer or url_for('products.index')
    if not filepath:
        return redirect(next_url)
    # Normalise the filepath (relative to static) for checklist removal
    norm_path = ''
    try:
        norm_path = filepath.replace('\\', '/').strip()
    except Exception:
        norm_path = filepath
    try:
        # Build absolute path to the target file
        abs_path = os.path.abspath(os.path.join(current_app.static_folder, filepath))
        static_root = os.path.abspath(current_app.static_folder)
        # Only proceed if the file resides within the static folder
        if abs_path.startswith(static_root) and os.path.isfile(abs_path):
            try:
                os.remove(abs_path)
                flash('Documento eliminato.', 'success')
            except Exception:
                # Ignore errors silently
                pass
    except Exception:
        # Ignore path resolution errors
        pass
    # Remove the document from any checklist entries so that deleted
    # documents are no longer required during production loads.
    try:
        from ...checklist import load_checklist, save_checklist
        data = load_checklist()
        updated = False
        for sid, docs in list(data.items()):
            if not isinstance(docs, list):
                continue
            # Remove any entries matching the normalised path
            if norm_path in docs:
                data[sid] = [d for d in docs if d != norm_path]
                updated = True
        if updated:
            save_checklist(data)
    except Exception:
        # Ignore errors while updating checklist
        pass
    return redirect(next_url)


# Category table placeholder for assemblies, parts and commercial parts
@products_bp.route('/<int:product_id>/category/<category>/table', methods=['GET', 'POST'])
@login_required
def category_table(product_id: int, category: str):
    """Display and edit details for components of a product based on category.

    - ``assembly``: shows a read‑only list of component parts with their quantities and derived weight.
    - ``part``: allows editing of manufacturing attributes such as work phase, type, supplier, work center, weight,
      processing cost, standard time (if internal), theoretical/real lead time and notes.
    - ``commercial``: allows editing of supplier, price per unit, minimum order quantity, theoretical/real lead time
      and notes.

    When the view receives a POST request it updates a single ``ProductComponent`` instance based on the
    ``component_id`` provided in the form and redirects back to the same page.
    """
    product = Product.query.get_or_404(product_id)
    allowed = {'assembly', 'part', 'commercial'}
    if category not in allowed:
        abort(404)
    # Handle update from submitted form
    if request.method == 'POST':
        # Handle update of a single component.  Use the global master to
        # ensure updates propagate across all occurrences of the same code.
        comp_id = request.form.get('component_id')
        if not comp_id:
            flash('Nessun componente specificato.', 'warning')
            return redirect(url_for('products.category_table', product_id=product_id, category=category))
        component = ProductComponent.query.get_or_404(comp_id)
        # Only allow editing of components belonging to this product
        if component.product_id != product.id:
            abort(403)
        struct = component.structure
        # Ensure a ComponentMaster exists for this structure so updates are global
        try:
            ensure_component_master_for_structure(struct)
        except Exception:
            pass
        master = getattr(component, 'component_master', None)
        # Update based on category
        if category == 'part':
            # Extract form values (empty string -> None)
            phase_id = request.form.get('work_phase_id') or None
            supplier_id = request.form.get('supplier_id') or None
            center_id = request.form.get('work_center_id') or None
            processing_type = request.form.get('processing_type') or None
            weight = request.form.get('weight') or None
            processing_cost = request.form.get('processing_cost') or None
            standard_time = request.form.get('standard_time') or None
            lead_time_theoretical = request.form.get('lead_time_theoretical') or None
            lead_time_real = request.form.get('lead_time_real') or None
            description = request.form.get('description') or None
            notes = request.form.get('notes') or None
            # Convert numeric fields to appropriate types
            def to_minutes(days_val: str | None) -> float | None:
                try:
                    return float(days_val) * 1440.0 if days_val not in (None, '') else None
                except Exception:
                    return None
            # Handle weight
            try:
                weight_val: float | None = round(float(weight), 3) if weight else None
            except (ValueError, TypeError):
                weight_val = None
            # Handle standard time in hours (convert to minutes)
            try:
                std_hours: float | None = float(standard_time) if standard_time else None
            except (ValueError, TypeError):
                std_hours = None
            std_minutes: float | None = std_hours * 60.0 if std_hours is not None else None
            # Lead times in minutes
            lt_theo_min: float | None = to_minutes(lead_time_theoretical)
            lt_real_min: float | None = to_minutes(lead_time_real)
            # If a master exists update it; otherwise update the component directly
            if master:
                # Update core attributes on the master
                master.description = description
                master.weight = weight_val
                master.processing_type = processing_type
                master.work_phase_id = int(phase_id) if phase_id else None
                # Safely convert supplier identifier to integer if possible
                if supplier_id:
                    try:
                        master.supplier_id = int(supplier_id)
                    except (ValueError, TypeError):
                        master.supplier_id = None
                else:
                    master.supplier_id = None
                master.work_center_id = int(center_id) if center_id else None
                master.standard_time = std_minutes
                master.lead_time_theoretical = lt_theo_min
                master.lead_time_real = lt_real_min
                master.notes = notes
                # Compute or set processing cost
                if processing_type == 'internal':
                    # Compute cost using hours and work centre cost
                    if std_hours is not None and master.work_center_id:
                        wc = WorkCenter.query.get(master.work_center_id)
                        if wc and wc.hourly_cost is not None:
                            master.processing_cost = std_hours * wc.hourly_cost
                        else:
                            master.processing_cost = None
                    else:
                        master.processing_cost = None
                elif processing_type == 'external':
                    master.processing_cost = None
                else:
                    # If provided, use explicit processing cost
                    try:
                        master.processing_cost = float(processing_cost) if processing_cost else None
                    except (ValueError, TypeError):
                        master.processing_cost = None
                # Mirror the updated master fields back onto the component for immediate display
                component.description = master.description
                component.weight = master.weight
                component.processing_type = master.processing_type
                component.work_phase_id = master.work_phase_id
                component.supplier_id = master.supplier_id
                component.work_center_id = master.work_center_id
                component.standard_time = master.standard_time
                component.lead_time_theoretical = master.lead_time_theoretical
                component.lead_time_real = master.lead_time_real
                component.processing_cost = master.processing_cost
                # Copy only the plain notes from the master to the component
                component.notes = extract_plain_notes(master.notes)
            else:
                # No master: update the component directly (legacy behavior)
                component.description = description
                component.notes = notes
                component.weight = weight_val
                component.processing_type = processing_type
                component.work_phase_id = int(phase_id) if phase_id else None
                # Safely convert supplier identifier to integer when possible.
                # A supplier may be entered manually as a code that is not numeric; in this
                # case ignore and set to None.
                if supplier_id:
                    try:
                        component.supplier_id = int(supplier_id)
                    except (ValueError, TypeError):
                        component.supplier_id = None
                else:
                    component.supplier_id = None
                component.work_center_id = int(center_id) if center_id else None
                component.standard_time = std_minutes
                component.lead_time_theoretical = lt_theo_min
                component.lead_time_real = lt_real_min
                if processing_type == 'internal':
                    if std_hours is not None and component.work_center_id:
                        wc = WorkCenter.query.get(component.work_center_id)
                        if wc and wc.hourly_cost is not None:
                            component.processing_cost = std_hours * wc.hourly_cost
                        else:
                            component.processing_cost = None
                    else:
                        component.processing_cost = None
                elif processing_type == 'external':
                    component.processing_cost = None
                else:
                    try:
                        component.processing_cost = float(processing_cost) if processing_cost else None
                    except (ValueError, TypeError):
                        component.processing_cost = None
            db.session.commit()
            flash('Componente aggiornato.', 'success')
        elif category == 'commercial':
            # Extract form values
            # Read flags for future functionality.  The presence of the checkbox
            # indicates a True value, otherwise False.  These flags should
            # propagate to the master (when present) and to the component.
            sellable_flag = True if request.form.get('is_sellable') else False
            guiding_flag = True if request.form.get('guiding_part') else False

            supplier_id = request.form.get('supplier_id') or None
            weight = request.form.get('weight') or None
            price_per_unit = request.form.get('price_per_unit') or None
            minimum_order_qty = request.form.get('minimum_order_qty') or None
            lead_time_theoretical = request.form.get('lead_time_theoretical') or None
            lead_time_real = request.form.get('lead_time_real') or None
            description = request.form.get('description') or None
            # Notes may be stored as JSON with additional attributes (e.g. lot management).
            raw_notes = request.form.get('notes') or None
            lot_mgmt_flag = True if request.form.get('lot_management') else False
            # Convert numeric fields
            try:
                weight_val = round(float(weight), 3) if weight else None
            except (ValueError, TypeError):
                weight_val = None
            try:
                price_val = float(price_per_unit) if price_per_unit else None
            except (ValueError, TypeError):
                price_val = None
            try:
                moq_val = int(minimum_order_qty) if minimum_order_qty else None
            except (ValueError, TypeError):
                moq_val = None
            def to_minutes(days_val: str | None) -> float | None:
                try:
                    return float(days_val) * 1440.0 if days_val not in (None, '') else None
                except Exception:
                    return None
            lt_theo_min = to_minutes(lead_time_theoretical)
            lt_real_min = to_minutes(lead_time_real)
            if master:
                master.description = description
                master.weight = weight_val
                # Safely convert supplier identifier to integer when possible
                if supplier_id:
                    try:
                        master.supplier_id = int(supplier_id)
                    except (ValueError, TypeError):
                        master.supplier_id = None
                else:
                    master.supplier_id = None
                master.price_per_unit = price_val
                master.minimum_order_qty = moq_val
                master.lead_time_theoretical = lt_theo_min
                master.lead_time_real = lt_real_min
                master.notes = notes
                # Mirror values back to component
                component.description = master.description
                component.weight = master.weight
                component.supplier_id = master.supplier_id
                component.price_per_unit = master.price_per_unit
                component.minimum_order_qty = master.minimum_order_qty
                component.lead_time_theoretical = master.lead_time_theoretical
                component.lead_time_real = master.lead_time_real
                # Copy only the plain notes from the master to the component
                component.notes = extract_plain_notes(master.notes)
            else:
                component.description = description
                component.weight = weight_val
                # Safely convert supplier identifier to integer when possible.
                if supplier_id:
                    try:
                        component.supplier_id = int(supplier_id)
                    except (ValueError, TypeError):
                        component.supplier_id = None
                else:
                    component.supplier_id = None
                component.price_per_unit = price_val
                component.minimum_order_qty = moq_val
                component.lead_time_theoretical = lt_theo_min
                component.lead_time_real = lt_real_min
                component.notes = notes
            db.session.commit()
            flash('Componente aggiornato.', 'success')
        # Assemblies remain read-only
        return redirect(url_for('products.category_table', product_id=product_id, category=category))

    # GET request: gather components and related dictionary lists
    # Filter components based on structure flags
    if category == 'assembly':
        components = (ProductComponent.query
                      .join(Structure)
                      .filter(ProductComponent.product_id == product.id, Structure.flag_assembly == True)
                      .all())
    elif category == 'part':
        components = (ProductComponent.query
                      .join(Structure)
                      .filter(ProductComponent.product_id == product.id, Structure.flag_part == True)
                      .all())
    else:  # commercial
        components = (ProductComponent.query
                      .join(Structure)
                      .filter(ProductComponent.product_id == product.id, Structure.flag_commercial == True)
                      .all())

    # ---------------------------------------------------------------------
    # When handling a GET request, prefill component fields from their
    # associated ComponentMaster records (if any) before rendering.
    # This ensures that inline editing in the category table displays
    # existing global values for codes like P001-025-001B.  Fields that
    # are None or blank on the ProductComponent are replaced by values
    # from the master.  No database commit occurs – the values are
    # populated only for display.
    if request.method == 'GET':
        # Determine uploads directory for image fallback.  If retrieval
        # fails the variable remains None and image fallback is skipped.
        try:
            upload_dir = os.path.join(current_app.static_folder, 'uploads')
        except Exception:
            upload_dir = None
        # Build a prefix→filename map once per request to avoid scanning the
        # uploads directory for each component.  Each key is a prefix such
        # as ``cm_<id>_`` or ``sn_<id>_`` and the value is the first
        # encountered filename with that prefix.  If the directory cannot
        # be read the map remains empty.
        prefix_map: dict[str, str] = {}
        if upload_dir and os.path.isdir(upload_dir):
            try:
                for _fname in os.listdir(upload_dir):
                    parts = _fname.split('_', 2)
                    if len(parts) >= 2:
                        prefix = parts[0] + '_' + parts[1] + '_'
                        if prefix not in prefix_map:
                            prefix_map[prefix] = _fname
            except Exception:
                prefix_map = {}
        for comp in components:
            try:
                # Helper to decide whether a string field is empty
                def _empty(val: str | None) -> bool:
                    return val is None or (isinstance(val, str) and val.strip() == '')
                master = getattr(comp, 'component_master', None)
                # -----------------------------------------------------------------
                # Prefill description and notes.  Prefer component's own fields; if
                # empty fall back to master, then to the structure itself.  Only
                # if still empty, leave as is and let the template fall back to
                # type description or name.
                if _empty(comp.description):
                    if master and not _empty(getattr(master, 'description', None)):
                        comp.description = master.description
                    elif not _empty(getattr(comp.structure, 'description', None)):
                        comp.description = comp.structure.description
                if _empty(comp.notes):
                    if master and not _empty(getattr(master, 'notes', None)):
                        # Populate notes with the plain text portion of the master notes
                        comp.notes = extract_plain_notes(master.notes)
                    elif not _empty(getattr(comp.structure, 'notes', None)):
                        # Fall back to the plain notes on the structure
                        comp.notes = extract_plain_notes(comp.structure.notes)
                # Weight
                if comp.weight is None:
                    if master and master.weight is not None:
                        comp.weight = master.weight
                    elif comp.structure.weight is not None:
                        comp.weight = comp.structure.weight
                # Work phase
                if comp.work_phase_id is None:
                    if master and master.work_phase_id is not None:
                        comp.work_phase_id = master.work_phase_id
                    elif comp.structure.work_phase_id is not None:
                        comp.work_phase_id = comp.structure.work_phase_id
                # Processing type
                if _empty(comp.processing_type):
                    if master and not _empty(master.processing_type):
                        comp.processing_type = master.processing_type
                    elif not _empty(comp.structure.processing_type):
                        comp.processing_type = comp.structure.processing_type
                # Supplier and work centre
                if comp.supplier_id is None:
                    if master and master.supplier_id is not None:
                        comp.supplier_id = master.supplier_id
                    elif comp.structure.supplier_id is not None:
                        comp.supplier_id = comp.structure.supplier_id
                if comp.work_center_id is None:
                    if master and master.work_center_id is not None:
                        comp.work_center_id = master.work_center_id
                    elif comp.structure.work_center_id is not None:
                        comp.work_center_id = comp.structure.work_center_id
                # Standard time and lead times
                if comp.standard_time is None:
                    if master and master.standard_time is not None:
                        comp.standard_time = master.standard_time
                    elif comp.structure.standard_time is not None:
                        comp.standard_time = comp.structure.standard_time
                if comp.lead_time_theoretical is None:
                    if master and master.lead_time_theoretical is not None:
                        comp.lead_time_theoretical = master.lead_time_theoretical
                    elif comp.structure.lead_time_theoretical is not None:
                        comp.lead_time_theoretical = comp.structure.lead_time_theoretical
                if comp.lead_time_real is None:
                    if master and master.lead_time_real is not None:
                        comp.lead_time_real = master.lead_time_real
                    elif comp.structure.lead_time_real is not None:
                        comp.lead_time_real = comp.structure.lead_time_real
                # Processing cost
                if comp.processing_cost is None:
                    if master and master.processing_cost is not None:
                        comp.processing_cost = master.processing_cost
                    elif comp.structure.processing_cost is not None:
                        comp.processing_cost = comp.structure.processing_cost
                # Commercial fields
                if hasattr(comp, 'price_per_unit') and comp.price_per_unit is None:
                    if master and master.price_per_unit is not None:
                        comp.price_per_unit = master.price_per_unit
                    elif comp.structure.price_per_unit is not None:
                        comp.price_per_unit = comp.structure.price_per_unit
                if hasattr(comp, 'minimum_order_qty') and comp.minimum_order_qty is None:
                    if master and master.minimum_order_qty is not None:
                        comp.minimum_order_qty = master.minimum_order_qty
                    elif comp.structure.minimum_order_qty is not None:
                        comp.minimum_order_qty = comp.structure.minimum_order_qty
                # -----------------------------------------------------------------
                # Image fallback: choose the first available among component's own image,
                # master image (cm_<master id>_) and structure image (sn_<structure id>_).
                # Do not override if an image is already set.  Use the prefix_map
                # computed above to avoid scanning the directory multiple times.
                if not getattr(comp, 'image_filename', None) and prefix_map:
                    chosen: str | None = None
                    # Master image
                    if master:
                        prefix_master = f"cm_{master.id}_"
                        chosen = prefix_map.get(prefix_master)
                    # Structure image
                    if not chosen:
                        prefix_struct = f"sn_{comp.structure.id}_"
                        chosen = prefix_map.get(prefix_struct)
                    if chosen:
                        comp.image_filename = chosen
            except Exception:
                # Continue on errors to avoid breaking the view
                continue

    # Fetch dictionary lists for selects
    suppliers = Supplier.query.order_by(Supplier.name.asc()).all()
    work_centers = WorkCenter.query.order_by(WorkCenter.name.asc()).all()
    work_phases = WorkPhase.query.order_by(WorkPhase.name.asc()).all()

    # ---------------------------------------------------------------------
    # Build a document map for each component so the table can display
    # previously uploaded documents in the inline editing view.  Documents for
    # product components are stored under static/documents/<product>/<structure
    # path>/<folder>.  The folders vary by category (assembly, part or
    # commercial).  Collect the files per folder for each component and
    # assemble a mapping keyed by component ID.  Also provide a
    # human‑friendly label map for the folders.
    from os.path import join, isdir, isfile
    from werkzeug.utils import secure_filename
    documents_map: dict[int, dict[str, list[dict]]] = {}
    # Determine which document folders apply for this category
    if category == 'part':
        doc_folders = ['qualita', '3_1_materiale', 'step_tavole', 'altro']
    elif category == 'assembly':
        doc_folders = ['qualita', 'step_tavole', 'funzionamento', 'istruzioni', 'altro']
    else:
        doc_folders = ['qualita', 'ddt_fornitore', 'step_tavole', '3_1_materiale', 'altro']
    def _safe(name: str) -> str:
        return secure_filename(name) or 'unnamed'
    prod_dir = _safe(product.name)
    # Helper to collect the structure path for a node
    def _collect_path(node, parts):
        if node.parent:
            _collect_path(node.parent, parts)
        parts.append(_safe(node.name))
    for comp in components:
        entries: dict[str, list[dict]] = {}
        struct_parts: list[str] = []
        _collect_path(comp.structure, struct_parts)
        base_path = os.path.join(current_app.static_folder, 'documents', prod_dir, *struct_parts)
        # Prefer a revision‑specific directory when the component's structure
        # has a revision label.  Use the `_Rev_<letter>` suffix on the final
        # path component to find the directory for this revision.  If the
        # directory exists, use it instead of the unversioned path to avoid
        # showing files from previous revisions.
        try:
            s = comp.structure
            rev_label = s.revision_label
            rev_letter = None
            if isinstance(rev_label, str) and '.' in rev_label:
                rev_letter = rev_label.split('.')[-1]
            if rev_letter:
                from werkzeug.utils import secure_filename
                safe_base = secure_filename(s.name) or 'unnamed'
                rev_name = f"{safe_base}_Rev_{rev_letter}"
                # Build the alternate path segments by replacing the last
                # structure segment with the revision-specific folder.  When
                # the structure has no parts, treat the revision name as the sole segment.
                alt_parts = struct_parts[:-1] + [rev_name] if struct_parts else [rev_name]
                alt_base = os.path.join(current_app.static_folder, 'documents', prod_dir, *alt_parts)
                # Prefer the revision-specific directory (even if it does not exist)
                # to avoid showing files from previous revisions.
                base_path = alt_base
                # Replace struct_parts with revision-specific segments so that
                # relative file paths reflect the correct directory.  Without
                # this assignment, rel_path values would still point to the
                # unversioned folder, causing mismatches when displaying and
                # flagging documents in the UI.
                struct_parts = alt_parts
        except Exception:
            pass
        for folder in doc_folders:
            file_list: list[dict] = []
            folder_path = os.path.join(base_path, folder)
            try:
                if os.path.isdir(folder_path):
                    for fname in os.listdir(folder_path):
                        fpath = os.path.join(folder_path, fname)
                        if os.path.isfile(fpath):
                            rel_path = os.path.join('documents', prod_dir, *struct_parts, folder, fname)
                            file_list.append({'name': fname, 'path': rel_path})
            except Exception:
                pass
            entries[folder] = file_list
        # Merge master‑level documents stored under tmp_components/<code>/<folder>.
        # Skip merging default master documents when the component belongs to a
        # revised structure (revision_label is non‑empty) so that new
        # revisions start without inherited files.
        try:
            if not comp.structure.revision_label:
                m = getattr(comp, 'component_master', None)
                if m:
                    code_dir = _safe(m.code)
                    base_master = os.path.join(current_app.static_folder, 'tmp_components', code_dir)
                    for folder in doc_folders:
                        file_list = entries.get(folder, [])
                        master_folder = os.path.join(base_master, folder)
                        if os.path.isdir(master_folder):
                            for fname in os.listdir(master_folder):
                                full_path = os.path.join(master_folder, fname)
                                if os.path.isfile(full_path) and not any(d.get('name') == fname for d in file_list):
                                    rel_default_path = os.path.join('tmp_components', code_dir, folder, fname)
                                    file_list.append({'name': fname, 'path': rel_default_path})
                        entries[folder] = file_list
        except Exception:
            pass
        documents_map[comp.id] = entries
    # Map folder identifiers to human‑readable labels
    doc_label_map: dict[str, str] = {
        'qualita': 'Modulo Cert. qualità',
        '3_1_materiale': '3.1 Materiale',
        'step_tavole': 'Step/tavola',
        'funzionamento': 'Verifica funzionamento',
        'istruzioni': 'Montaggio istruzioni',
        'ddt_fornitore': 'DDT fornitore',
        'altro': 'Altro',
    }
    # Load current checklist selections to pre-check documents in the UI.
    try:
        checklist_data = load_checklist()
    except Exception:
        checklist_data = {}
    # Build a mapping from component id to a set of flagged document paths.  Each
    # flagged entry corresponds to a specific document (folder + filename) and
    # should match any actual document stored for the component regardless of
    # where the document resides (e.g. tmp_components vs documents) or how the
    # structure path has changed (e.g. through renaming or revision).  To
    # accomplish this, extract the folder and filename from each stored
    # checklist path and then look up the corresponding file path in
    # ``documents_map``.  Only file paths that exist for the component are
    # returned, ensuring that the checkbox is checked for the correct
    # documents.
    flagged_docs_map: dict[int, set[str]] = {}
    # Additional mappings to capture flagged filenames and base names (both lower-case) per component.
    # These maps allow the template to determine if a document should be shown as checked even when
    # the stored path differs due to renaming or moving between revisions.  Using lower-case
    # filenames/base names ensures case-insensitive matching.
    flagged_filenames_map: dict[int, set[str]] = {}
    flagged_basenames_map: dict[int, set[str]] = {}
    for comp in components:
        # Determine the structure id for this component.  If absent, skip.
        try:
            sid = comp.structure.id
        except Exception:
            sid = None
        if sid is None:
            flagged_docs_map[comp.id] = set()
            continue
        # Retrieve the list of flagged paths for this structure from the
        # checklist.  Absent entries yield an empty list.
        flagged_list = checklist_data.get(str(sid), []) or []
        # Build a set of (folder, filename) tuples and a set of filenames
        # from the stored checklist paths.  Paths are normalised by
        # replacing backslashes with forward slashes and trimming
        # whitespace.  Both structures are used when matching uploaded
        # documents: flagged_names requires an exact folder and filename
        # match, while flagged_filenames matches any file with the
        # same filename regardless of folder.  This makes checklist
        # flags resilient to differences in product or revision paths.
        flagged_names: set[tuple[str, str]] = set()
        # Per-component collection of lower-case filenames and base names of flagged documents.
        flagged_filenames: set[str] = set()
        # Also derive a set of base filenames (without extension and without compiled suffix)
        # for flagged documents.  This allows the UI to treat compiled
        # versions of flagged documents as flagged as well.
        flagged_basenames: set[str] = set()
        for p in flagged_list:
            # Normalise each stored path by converting backslashes to forward slashes
            # and trimming whitespace.  Skip any non-string entries or empty strings.
            if not isinstance(p, str):
                continue
            try:
                norm = p.replace('\\', '/').strip()
            except Exception:
                norm = p
            if not norm:
                continue
            parts = norm.split('/')
            if len(parts) >= 2:
                folder = parts[-2]
                filename = parts[-1]
                # Convert folder and filename to lowercase for case-insensitive matching
                folder_lower = str(folder).lower()
                filename_lower = str(filename).lower()
                # Store the lowercase folder and filename in the flagged sets
                flagged_names.add((folder_lower, filename_lower))
                # Also add a sanitised filename where runs of spaces and underscores
                # are removed.  This allows matching between filenames such as
                # "data sheet.pdf" and "data_sheet.pdf".
                # Always include the raw filename.
                flagged_filenames.add(filename_lower)
                # Include common variants of the filename to tolerate spaces/underscores.
                try:
                    # Remove all runs of spaces and underscores to build a compact key
                    filename_sanitised = re.sub(r'[\s_]+', '', filename_lower)
                    if filename_sanitised:
                        flagged_filenames.add(filename_sanitised)
                    # Replace spaces with underscores and underscores with spaces to allow
                    # matching between names like "data sheet" and "data_sheet".
                    variant_spaces_to_underscores = filename_lower.replace(' ', '_')
                    variant_underscores_to_spaces = filename_lower.replace('_', ' ')
                    if variant_spaces_to_underscores:
                        flagged_filenames.add(variant_spaces_to_underscores)
                    if variant_underscores_to_spaces:
                        flagged_filenames.add(variant_underscores_to_spaces)
                except Exception:
                    pass
                # Compute the base name (remove extension) and strip any compiled suffix.
                # Lowercase the result so comparisons are case-insensitive.
                base_no_ext = filename_lower.rsplit('.', 1)[0]
                base_original = base_no_ext.split('_compiled_', 1)[0]
                if base_original:
                    # Add the raw base
                    flagged_basenames.add(base_original)
                    try:
                        # Also include variants to handle underscores/spaces interchangeably.
                        # Compact version with spaces/underscores removed
                        base_sanitised = re.sub(r'[\s_]+', '', base_original)
                        if base_sanitised:
                            flagged_basenames.add(base_sanitised)
                        # Replace spaces with underscores and underscores with spaces
                        variant_spaces_to_underscores = base_original.replace(' ', '_')
                        variant_underscores_to_spaces = base_original.replace('_', ' ')
                        if variant_spaces_to_underscores:
                            flagged_basenames.add(variant_spaces_to_underscores)
                        if variant_underscores_to_spaces:
                            flagged_basenames.add(variant_underscores_to_spaces)
                    except Exception:
                        pass
        # For each document in the documents_map for this component, check
        # whether its (folder, filename) tuple matches one of the flagged
        # names.  If so, include the file path in the flagged_docs_map.
        matches: set[str] = set()
        doc_entries = documents_map.get(comp.id, {})
        for folder, files in doc_entries.items():
            for file_dict in files:
                try:
                    fname = file_dict.get('name')
                except Exception:
                    fname = None
                # A match occurs when either the exact folder and filename
                # pair is present in flagged_names or the filename alone
                # appears in flagged_filenames.  Matching only on the
                # filename allows toggled documents to remain checked
                # after renaming products or revision directories.  Use
                # try/except when accessing the path to guard against
                # missing keys.
                if fname:
                    # Convert folder and filename to lowercase for case-insensitive matching
                    folder_lower = str(folder).lower()
                    fname_lower = str(fname).lower()
                    # Determine the base name of the current file (remove extension and compiled suffix)
                    fname_no_ext = fname_lower.rsplit('.', 1)[0]
                    fname_base = fname_no_ext.split('_compiled_', 1)[0]
                    # Compute sanitised versions of the filename and base name by removing
                    # both spaces and underscores.  These forms allow matching when the
                    # stored checklist entry uses spaces but the uploaded file name has
                    # underscores (or vice versa).
                    try:
                        fname_sanitised = re.sub(r'[\s_]+', '', fname_lower)
                    except Exception:
                        fname_sanitised = fname_lower
                    try:
                        fname_base_sanitised = re.sub(r'[\s_]+', '', fname_base)
                    except Exception:
                        fname_base_sanitised = fname_base
                    # A match occurs when either the exact folder and filename pair
                    # (in lowercase) is present in flagged_names, the lowercase
                    # filename or its sanitised variant appears in flagged_filenames,
                    # or the base name (raw or sanitised) matches one of the
                    # flagged_basenames.  Matching on the base name allows compiled
                    # document names to remain checked even after the product or
                    # revision path changes.  Matching on the sanitised forms
                    # accommodates differences in spacing and underscore usage.
                    if (
                        (folder_lower, fname_lower) in flagged_names
                        or fname_lower in flagged_filenames
                        or fname_sanitised in flagged_filenames
                        or fname_base in flagged_basenames
                        or fname_base_sanitised in flagged_basenames
                    ):
                        try:
                            matches.add(file_dict['path'])
                        except Exception:
                            pass
        # Additionally include the raw checklist paths themselves.  When a
        # checklist entry refers to a default document located outside the
        # component's documents_map (e.g. under tmp_components or tmp_structures),
        # it may not appear in doc_entries.  By adding the stored path to
        # matches, the template will still recognise it when comparing
        # against file.path values.  Normalise each stored path by replacing
        # backslashes with forward slashes and stripping whitespace to
        # maintain consistency with toggle_flag() semantics.
        for p in flagged_list:
            # Include each stored path directly in the matches set so that
            # checkboxes remain selected even when the corresponding file is
            # located outside of documents_map (e.g. default documents).  To
            # normalise the path consistently across platforms, replace
            # single backslashes with forward slashes and strip whitespace.
            if not isinstance(p, str):
                continue
            try:
                norm = p.replace('\\', '/').strip()
            except Exception:
                norm = p
            if norm:
                matches.add(norm)
        flagged_docs_map[comp.id] = matches
        # Save the per-component flagged filename and base name sets for template use
        flagged_filenames_map[comp.id] = flagged_filenames
        flagged_basenames_map[comp.id] = flagged_basenames
    return render_template('products/category_table.html',
                           product=product,
                           category=category,
                           components=components,
                           suppliers=suppliers,
                           work_centers=work_centers,
                           work_phases=work_phases,
                           documents_map=documents_map,
                           doc_label_map=doc_label_map,
                           flagged_docs_map=flagged_docs_map,
                           flagged_filenames_map=flagged_filenames_map,
                           flagged_basenames_map=flagged_basenames_map)

@products_bp.route('/<int:product_id>/component/<int:component_id>', methods=['GET', 'POST'])
@login_required
def component_detail(product_id: int, component_id: int):
    """Render a dedicated page for viewing and editing a single product component.

    This view provides a more user‑friendly interface than the inline editing
    offered in the category table.  Depending on the underlying structure flags
    (assembly, part or commercial) it displays different fields.  Assemblies
    are read‑only; parts and commercial components can be edited.  The parent
    product's image is shown as a visual reference.  On successful update
    the page reloads with a success message.
    """
    product = Product.query.get_or_404(product_id)
    component = ProductComponent.query.get_or_404(component_id)
    # Ensure the component belongs to this product
    if component.product_id != product.id:
        abort(403)
    # ---------------------------------------------------------------------
    # Redirect to the global definition page used in the admin section.
    #
    # The "Modifica" action on the product detail page should present the same
    # form as the "Definisci" action in the Admin → Strutture → Tipi e Nodi
    # section.  Instead of rendering the dedicated component detail template,
    # we forward the user to the admin blueprint route that manages default
    # attributes for a structure node.  This ensures that edits always
    # operate on the shared ComponentMaster associated with the component code
    # (e.g. "P001-025-001B").  After verifying ownership, compute the
    # underlying structure and redirect.  If no structure exists, fall
    # through to the original behaviour (extremely unlikely in a valid DB).
    struct = component.structure
    if struct is not None:
        # Preserve any return_to parameter provided by the product page.  When
        # present, this value identifies the URL to return to after saving
        # defaults (typically the product detail view with a highlight).
        return_to = request.args.get('return_to')
        if return_to:
            # Pass both return_to and the current product component id (pc_id) so
            # the admin default view can display the correct quantity and update
            # only this specific component when saving.  Including pc_id
            # distinguishes product-specific editing from pure default editing.
            return redirect(url_for('admin.define_structure_node_defaults', node_id=struct.id, return_to=return_to, pc_id=component.id))
        else:
            return redirect(url_for('admin.define_structure_node_defaults', node_id=struct.id, pc_id=component.id))
    # ---------------------------------------------------------------------
    # Determine category based on structure flags
    if struct.flag_part:
        category = 'part'
    elif struct.flag_commercial:
        category = 'commercial'
    else:
        category = 'assembly'
    # For assemblies compute the total weight of all part descendants (Peso parti).
    # The weight is calculated by summing the weight of each leaf part multiplied
    # by the product of quantities along the path from the assembly to the leaf.
    parts_weight: float | None = None
    if category == 'assembly':
        total = 0.0
        # Recursive helper to traverse descendants and accumulate weight
        def _accumulate_weight(structure_obj, multiplier: float):
            nonlocal total
            # For each child structure, find the corresponding product component
            for child in structure_obj.children:
                try:
                    comp_child = ProductComponent.query.filter_by(product_id=product.id, structure_id=child.id).first()
                except Exception:
                    comp_child = None
                if not comp_child:
                    continue
                # Determine the new multiplier: multiply by the quantity of this component
                qty = comp_child.quantity or 1
                new_multiplier = multiplier * qty
                # If the child is itself an assembly, recurse into its children
                if child.flag_assembly:
                    _accumulate_weight(child, new_multiplier)
                else:
                    # For parts and commercial parts, add the weighted weight if defined.
                    # Prefer the master weight if available.
                    comp_weight = None
                    try:
                        if getattr(comp_child, 'component_master', None) and comp_child.component_master and comp_child.component_master.weight is not None:
                            comp_weight = comp_child.component_master.weight
                        elif comp_child.weight is not None:
                            comp_weight = comp_child.weight
                    except Exception:
                        comp_weight = comp_child.weight if hasattr(comp_child, 'weight') else None
                    try:
                        if comp_weight is not None:
                            total += float(comp_weight) * new_multiplier
                    except Exception:
                        # Ignore non-numeric weights
                        pass
        try:
            _accumulate_weight(struct, 1.0)
            parts_weight = total
        except Exception:
            # If any error occurs leave parts_weight as None
            parts_weight = None
    # ---------------------------------------------------------------------
    # On all requests (GET or POST) determine if there is an image associated
    # with the component master and, if so, capture the filename.  This
    # allows the template to display the master image when the component
    # itself has no uploaded image.  The master image files are stored in
    # ``static/uploads`` with a prefix ``cm_<master_id>_``.
    master_image_filename = None
    try:
        m = getattr(component, 'component_master', None)
        if m:
            upload_dir = os.path.join(current_app.static_folder, 'uploads')
            if os.path.isdir(upload_dir):
                prefix = f"cm_{m.id}_"
                for f in os.listdir(upload_dir):
                    if f.startswith(prefix):
                        master_image_filename = f
                        break
    except Exception:
        master_image_filename = None
    # ---------------------------------------------------------------------
    # Prefill display fields from the component master on GET requests.  When
    # a component code already exists globally, the associated master holds
    # the canonical information (weight, description, notes, processing
    # parameters, supplier/work centre, costing, etc.).  If the current
    # component lacks a value for any of these fields (i.e. is None or
    # blank), populate it from the master.  This ensures that when viewing
    # or editing a component whose code already exists (e.g. P001-025-001B)
    # all of the previously defined information is shown by default.  No
    # values are committed to the database during this step – the
    # modifications exist only in memory for rendering purposes.
    if request.method == 'GET':
        try:
            master = getattr(component, 'component_master', None)
            if master:
                # Helper to decide if a field is considered empty on the component
                def _empty(val):
                    return val is None or (isinstance(val, str) and val.strip() == '')
                # Description
                if not _empty(component.description) and component.description:
                    pass
                elif not _empty(getattr(master, 'description', None)):
                    component.description = master.description
                # Notes
                if not _empty(component.notes):
                    pass
                elif not _empty(getattr(master, 'notes', None)):
                    # Copy only the plain notes from the master to the component
                    component.notes = extract_plain_notes(master.notes)
                # Weight
                if component.weight is None and master.weight is not None:
                    component.weight = master.weight
                # Work phase
                if component.work_phase_id is None and master.work_phase_id is not None:
                    component.work_phase_id = master.work_phase_id
                # Processing type
                if _empty(component.processing_type) and not _empty(master.processing_type):
                    component.processing_type = master.processing_type
                # Supplier
                if component.supplier_id is None and master.supplier_id is not None:
                    component.supplier_id = master.supplier_id
                # Work centre
                if component.work_center_id is None and master.work_center_id is not None:
                    component.work_center_id = master.work_center_id
                # Standard time
                if component.standard_time is None and master.standard_time is not None:
                    component.standard_time = master.standard_time
                # Lead time theoretical
                if component.lead_time_theoretical is None and master.lead_time_theoretical is not None:
                    component.lead_time_theoretical = master.lead_time_theoretical
                # Lead time real
                if component.lead_time_real is None and master.lead_time_real is not None:
                    component.lead_time_real = master.lead_time_real
                # Processing cost
                if component.processing_cost is None and master.processing_cost is not None:
                    component.processing_cost = master.processing_cost
                # Price per unit (commercial)
                if hasattr(component, 'price_per_unit'):
                    if component.price_per_unit is None and master.price_per_unit is not None:
                        component.price_per_unit = master.price_per_unit
                # Minimum order qty (commercial)
                if hasattr(component, 'minimum_order_qty'):
                    if component.minimum_order_qty is None and master.minimum_order_qty is not None:
                        component.minimum_order_qty = master.minimum_order_qty

                # Sellable and guiding part flags: prefill from master when the component's
                # flags are undefined.  Use simple boolean assignment without
                # overriding explicit False values on the component.  When the master
                # defines a flag as True, propagate it; otherwise leave as is.
                try:
                    if getattr(component, 'is_sellable', None) is None and getattr(master, 'is_sellable', None) is not None:
                        component.is_sellable = bool(master.is_sellable)
                    if getattr(component, 'guiding_part', None) is None and getattr(master, 'guiding_part', None) is not None:
                        component.guiding_part = bool(master.guiding_part)
                except Exception:
                    pass
        except Exception:
            pass
    if request.method == 'POST':
        # ---------------------------------------------------------------------
        # Ensure the revision and compatible_revisions columns exist on the
        # structures table before attempting to read or write these fields.
        # Older databases may not include these columns and will silently
        # discard changes if they are not present.  This dynamic check
        # mirrors the logic in the revise_structure endpoint and avoids
        # migrations in environments where Alembic is not used.
        try:
            from sqlalchemy import text
            conn = db.engine.connect()
            col_names = [row[1] for row in conn.execute(text('PRAGMA table_info(structures)')).fetchall()]
            # Add revision column if missing (defaults to 0 so existing rows
            # appear without a revision until explicitly revised).
            if 'revision' not in col_names:
                conn.execute(text('ALTER TABLE structures ADD COLUMN revision INTEGER DEFAULT 0'))
            # Add compatible_revisions column if missing.  This stores a
            # comma‑separated list of prior revision labels selected as
            # compatible with the current revision.  When absent, any
            # assignments to struct.compatible_revisions would be ignored.
            if 'compatible_revisions' not in col_names:
                conn.execute(text('ALTER TABLE structures ADD COLUMN compatible_revisions TEXT'))
            conn.close()
        except Exception:
            # Ignore any error; if the columns already exist or cannot be
            # added the subsequent assignment will simply update the model
            # attribute in memory.
            pass
        # Ensure this component has a master.  Creating or retrieving the master
        # before handling uploads ensures images and documents are stored under
        # the correct global directory.  The helper gracefully handles cases
        # where a master already exists.
        try:
            ensure_component_master_for_structure(struct)
        except Exception:
            pass
        master = getattr(component, 'component_master', None)

        # -------------------------------------------------------------------
        # Save compatible revisions selections submitted with the edit forms.
        #
        # When editing a component (e.g. updating quantities or attributes) the
        # template may include one or more checkboxes named ``compatible_revisions``.
        # These checkboxes allow the user to designate which prior revision labels
        # remain compatible with the current revision.  Unlike the separate
        # revision form handled by the ``revise_structure`` route, the edit form
        # posts to this endpoint.  Prior to this fix the submitted
        # compatibility selections were ignored, causing changes to be lost when
        # clicking "Salva".  Capture the selections using getlist() and
        # persist them on the underlying Structure.  When no values are
        # submitted we clear any previous selections; when the field is not
        # present at all (e.g. assemblies without revisions) the value is left
        # unchanged.
        try:
            # Capture submitted compatibility values.  Each checkbox named
            # ``compatible_revisions`` corresponds to a revision letter.  Use
            # getlist() to collect multiple checkbox values.  If no list
            # values are returned and the field exists, also split the
            # raw string on commas to accommodate multi‑select inputs that
            # send a single comma‑separated string.  The current revision
            # checkbox is always checked in the UI and included in
            # compat_vals, but it should not be stored in the
            # compatible_revisions field.
            # Gather all selected previous revisions.  Checkboxes and multi‑selects
            # use the "compatible_revisions" name.  When no previous revisions
            # are selected, the field may be absent; however the form includes
            # a hidden "current_revision" input to indicate that a
            # compatibility selection block was present.  Use getlist() and
            # fallback to splitting a raw value when necessary.
            compat_vals = request.form.getlist('compatible_revisions') or []
            if not compat_vals:
                raw_val2 = request.form.get('compatible_revisions')
                if raw_val2:
                    compat_vals = [v.strip() for v in raw_val2.split(',') if v.strip()]
            # Determine whether the field was present at all.  When the
            # field exists (even if no previous revisions are selected)
            # update the structure's compatible revisions accordingly.  If
            # the field is missing entirely (e.g. assemblies without
            # revisions), leave the existing value unchanged.
            # Determine whether compatibility selections were posted.  The
            # presence of either the "compatible_revisions" list or the
            # "current_revision" hidden input signals that the user interacted
            # with the compatibility control.  When true, update the
            # structure's compatible revisions accordingly.  Without this
            # check, absence of the field would leave existing values
            # unchanged (e.g. when editing assemblies without revisions).
            if 'compatible_revisions' in request.form or 'current_revision' in request.form or compat_vals:
                selected_prev: list[str] = []
                for letter in struct.revision_letters:
                    if letter == struct.revision_label:
                        # Skip the current revision; it is always implicitly
                        # compatible with itself and should not be persisted.
                        continue
                    if letter in compat_vals:
                        selected_prev.append(letter)
                struct.compatible_revisions = ','.join(selected_prev) if selected_prev else None
                # Persist immediately so subsequent logic sees the updated value.
                db.session.commit()
                try:
                    db.session.refresh(struct)
                except Exception:
                    # Ignore refresh failures; the next request will load fresh values.
                    pass
        except Exception:
            # On any error roll back the compatibility change but continue
            db.session.rollback()
            pass

        # -------------------------------------------------------------------
        # Update the quantity on this product component when a quantity input is provided.
        # This applies to assemblies, parts and commercial components.  The quantity
        # field is always named 'quantity' in the form.  Only non‑empty numeric
        # values will update the component.
        qty_value = request.form.get('quantity')
        if qty_value:
            try:
                qty_int = int(qty_value)
                if qty_int > 0:
                    component.quantity = qty_int
                    db.session.commit()
            except Exception:
                # Ignore invalid quantities
                pass

        # -------------------------------------------------------------------
        # Update sellable and guiding part flags for this component.  These flags
        # may appear on assembly, part and commercial component forms.  When the
        # corresponding checkbox is present in the submitted form, update
        # the boolean value; otherwise set to False.  Persist changes to
        # component immediately so subsequent logic sees the updated values.
        try:
            component.is_sellable = True if request.form.get('is_sellable') else False
            component.guiding_part = True if request.form.get('guiding_part') else False
            db.session.commit()
        except Exception:
            # Ignore if the component model lacks these attributes (legacy DB)
            db.session.rollback()
            pass
        # Process image upload for any category.  If a new image was uploaded,
        # save it and update the filename on the master (when present) or the
        # component.  Master images use the ``cm_<id>_`` prefix and are shared
        # globally; fallback images use the component id prefix.
        image_file = request.files.get('image')
        def allowed_file(filename: str) -> bool:
            return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'png', 'jpg', 'jpeg', 'gif', 'bmp'}
        if image_file and allowed_file(image_file.filename):
            raw_name = secure_filename(image_file.filename)
            upload_dir = os.path.join(current_app.static_folder, 'uploads')
            os.makedirs(upload_dir, exist_ok=True)
            if master:
                dest_name = f"cm_{master.id}_{raw_name}"
            else:
                dest_name = f"{component.id}_{raw_name}"
            try:
                image_file.save(os.path.join(upload_dir, dest_name))
            except Exception:
                pass
            # Record image filename on component for legacy compatibility
            component.image_filename = dest_name
            # Persist the updated image filename.  Without committing here,
            # modifications made by merely uploading a new image would not
            # propagate to the database.  In practice this meant the first
            # image upload succeeded (because other fields were saved at
            # the same time) but subsequent image replacements did not
            # stick if no other fields changed.  Committing immediately
            # ensures that image-only updates are saved correctly.
            try:
                db.session.commit()
            except Exception:
                # Roll back silently if the commit fails; other
                # operations later in this view may still succeed.  This
                # mirrors error handling used elsewhere in this route.
                db.session.rollback()
        # -------------------------------------------------------------------
        # Handle document uploads for parts, assemblies and commercial components.
        # The UI presents file inputs whose names encode the target category and folder
        # (e.g. part_qualita, ass_step_tavole, comm_ddt_fornitore).  Files
        # are saved under ``tmp_components/<code>/<folder>`` when a master exists;
        # otherwise they fall back to the legacy ``documents/<product>/<structure path>/<folder>``.
        doc_fields: dict[str, str] = {}
        if category == 'part':
            doc_fields = {
                'part_qualita': 'qualita',
                'part_3_1_materiale': '3_1_materiale',
                'part_step_tavole': 'step_tavole',
                'part_altro': 'altro',
            }
        elif category == 'assembly':
            doc_fields = {
                'ass_qualita': 'qualita',
                'ass_step_tavole': 'step_tavole',
                'ass_funzionamento': 'funzionamento',
                'ass_istruzioni': 'istruzioni',
                'ass_altro': 'altro',
            }
        elif category == 'commercial':
            doc_fields = {
                'comm_qualita': 'qualita',
                'comm_ddt_fornitore': 'ddt_fornitore',
                'comm_step_tavole': 'step_tavole',
                'comm_3_1_materiale': '3_1_materiale',
                'comm_altro': 'altro',
            }
        # Save documents if any file inputs were submitted
        docs_uploaded = False
        if doc_fields:
            # Helper to sanitize names for safe directory names
            def _safe(n: str) -> str:
                return secure_filename(n) or 'unnamed'
            # Determine base directory: prefer tmp_components/<code> when a
            # component master exists; otherwise fall back to the legacy
            # documents/<product>/<structure path> location.  When the
            # structure has a revision label, attempt to target a
            # revision‑specific folder (safe name + '_Rev_<letter>') if it
            # exists to prevent uploading files into the previous revision's
            # folder.
            base_path = None
            try:
                if getattr(component, 'component_master', None) and component.component_master:
                    code_name = _safe(component.component_master.code)
                    base_path = os.path.join(current_app.static_folder, 'tmp_components', code_name)
                    # If this structure has a revision, look for a revision‑specific
                    # directory under tmp_components using the safe structure name
                    try:
                        rev_label = struct.revision_label
                        rev_letter = None
                        if isinstance(rev_label, str) and '.' in rev_label:
                            rev_letter = rev_label.split('.')[-1]
                        if rev_letter:
                            safe_base = _safe(struct.name)
                            rev_name = f"{safe_base}_Rev_{rev_letter}"
                            alt_tmp = os.path.join(current_app.static_folder, 'tmp_components', rev_name)
                            if os.path.isdir(alt_tmp):
                                base_path = alt_tmp
                    except Exception:
                        pass
                else:
                    prod_dir = _safe(product.name)
                    struct_path_parts: list[str] = []
                    def _traverse_path(structure_obj):
                        if structure_obj.parent:
                            _traverse_path(structure_obj.parent)
                        struct_path_parts.append(_safe(structure_obj.name))
                    _traverse_path(struct)
                    base_path = os.path.join(current_app.static_folder, 'documents', prod_dir, *struct_path_parts)
                    # For revisions, use a revision‑specific path when available
                    try:
                        rev_label = struct.revision_label
                        rev_letter = None
                        if isinstance(rev_label, str) and '.' in rev_label:
                            rev_letter = rev_label.split('.')[-1]
                        if rev_letter:
                            safe_base = _safe(struct.name)
                            rev_name = f"{safe_base}_Rev_{rev_letter}"
                            alt_parts = struct_path_parts[:-1] + [rev_name] if struct_path_parts else [rev_name]
                            alt_docs = os.path.join(current_app.static_folder, 'documents', prod_dir, *alt_parts)
                            if os.path.isdir(alt_docs):
                                base_path = alt_docs
                    except Exception:
                        pass
            except Exception:
                prod_dir = _safe(product.name)
                struct_path_parts: list[str] = []
                def _traverse_path(structure_obj):
                    if structure_obj.parent:
                        _traverse_path(structure_obj.parent)
                    struct_path_parts.append(_safe(structure_obj.name))
                _traverse_path(struct)
                base_path = os.path.join(current_app.static_folder, 'documents', prod_dir, *struct_path_parts)
                # Attempt revision‑specific fallback
                try:
                    rev_label = struct.revision_label
                    rev_letter = None
                    if isinstance(rev_label, str) and '.' in rev_label:
                        rev_letter = rev_label.split('.')[-1]
                    if rev_letter:
                        safe_base = _safe(struct.name)
                        rev_name = f"{safe_base}_Rev_{rev_letter}"
                        alt_parts = struct_path_parts[:-1] + [rev_name] if struct_path_parts else [rev_name]
                        alt_docs = os.path.join(current_app.static_folder, 'documents', prod_dir, *alt_parts)
                        if os.path.isdir(alt_docs):
                            base_path = alt_docs
                except Exception:
                    pass
            for field_name, folder_name in doc_fields.items():
                files = request.files.getlist(field_name) or []
                for f in files:
                    if f and f.filename:
                        filename = secure_filename(f.filename)
                        target_dir = os.path.join(base_path, folder_name)
                        # Ensure the target directory exists and save the uploaded file
                        try:
                            os.makedirs(target_dir, exist_ok=True)
                        except Exception:
                            pass
                        dest_file_path = os.path.join(target_dir, filename)
                        try:
                            f.save(dest_file_path)
                            docs_uploaded = True
                        except Exception:
                            # Ignore errors silently; optional logging could be added
                            dest_file_path = None
                        # If the file was saved successfully, also copy it into the
                        # component-specific and warehouse (magazzino) directories under
                        # ``static/documents``.  This ensures that documentation
                        # uploaded via the product edit page is visible when building
                        # assemblies and stored in the warehouse hierarchy.
                        if dest_file_path:
                            # Determine the revision letter once for this file upload.  When
                            # the structure has a non‑empty revision label, the letter
                            # indicates that a revision‑specific directory should be
                            # created.  Compute it outside of the nested copy logic so
                            # it is available when copying into multiple destinations.
                            rev_letter = None
                            try:
                                rev_label_tmp = struct.revision_label
                                if isinstance(rev_label_tmp, str) and '.' in rev_label_tmp:
                                    rev_letter = rev_label_tmp.split('.')[-1]
                            except Exception:
                                rev_letter = None
                            try:
                                # Determine a safe name for the component.  When a master exists
                                # use its code, otherwise fall back to the structure name or id.
                                if getattr(component, 'component_master', None) and component.component_master:
                                    safe_comp = secure_filename(component.component_master.code) or component.component_master.code
                                else:
                                    safe_comp = secure_filename(component.name) or f"id_{component.id}"
                                # Copy the uploaded document into the base component directory
                                comp_doc_dir = os.path.join(current_app.static_folder, 'documents', safe_comp, folder_name)
                                os.makedirs(comp_doc_dir, exist_ok=True)
                                comp_dest = os.path.join(comp_doc_dir, filename)
                                try:
                                    shutil.copyfile(dest_file_path, comp_dest)
                                except Exception:
                                    pass
                                # Also prepare a revision‑specific component name when the structure has a revision
                                try:
                                    if rev_letter:
                                        safe_comp_rev = f"{safe_comp}_Rev_{rev_letter}"
                                        # Copy into the revision‑specific component directory
                                        comp_rev_dir = os.path.join(current_app.static_folder, 'documents', safe_comp_rev, folder_name)
                                        os.makedirs(comp_rev_dir, exist_ok=True)
                                        comp_rev_dest = os.path.join(comp_rev_dir, filename)
                                        try:
                                            shutil.copyfile(dest_file_path, comp_rev_dest)
                                        except Exception:
                                            pass
                                except Exception:
                                    pass
                                # Also copy into magazzino for each product referencing this structure
                                try:
                                    comps_for_struct = ProductComponent.query.filter_by(structure_id=component.id).all()
                                    for pc_ref in comps_for_struct:
                                        prod_ref = pc_ref.product
                                        if not prod_ref:
                                            continue
                                        safe_prod = secure_filename(prod_ref.name) or f"id_{prod_ref.id}"
                                        # Base magazzino directory (unversioned)
                                        mag_dir = os.path.join(current_app.static_folder,
                                                               'documents', safe_prod, safe_comp, folder_name)
                                        os.makedirs(mag_dir, exist_ok=True)
                                        mag_dest = os.path.join(mag_dir, filename)
                                        try:
                                            shutil.copyfile(dest_file_path, mag_dest)
                                        except Exception:
                                            pass
                                        # Revision‑specific magazzino directory
                                        try:
                                            if rev_letter:
                                                safe_comp_rev = f"{safe_comp}_Rev_{rev_letter}"
                                                mag_rev_dir = os.path.join(current_app.static_folder,
                                                                           'documents', safe_prod, safe_comp_rev, folder_name)
                                                os.makedirs(mag_rev_dir, exist_ok=True)
                                                mag_rev_dest = os.path.join(mag_rev_dir, filename)
                                                try:
                                                    shutil.copyfile(dest_file_path, mag_rev_dest)
                                                except Exception:
                                                    pass
                                        except Exception:
                                            pass
                                except Exception:
                                    pass
                            except Exception:
                                pass
        # If any documents were saved, display a success message.  This message
        # is shown in addition to any component update messages.
        if docs_uploaded:
            flash('Documenti caricati.', 'success')
        # Only parts and commercial components support editing of detailed fields
        if category == 'part':
            # Extract raw fields from the form
            phase_id = request.form.get('work_phase_id') or None
            supplier_id = request.form.get('supplier_id') or None
            center_id = request.form.get('work_center_id') or None
            processing_type = request.form.get('processing_type') or None
            weight_val = request.form.get('weight') or None
            processing_cost_input = request.form.get('processing_cost') or None
            standard_time_input = request.form.get('standard_time') or None
            lead_time_theoretical = request.form.get('lead_time_theoretical') or None
            lead_time_real = request.form.get('lead_time_real') or None
            description = request.form.get('description') or None
            notes_text = request.form.get('notes') or None
            cycles_json_str = request.form.get('cycles_json') or None
            # Ensure a master exists and set local reference
            m = getattr(component, 'component_master', None)
            if not m:
                try:
                    ensure_component_master_for_structure(struct)
                except Exception:
                    pass
                m = getattr(component, 'component_master', None)
            # Update the master when available
            if m:
                # Set sellable/guiding flags on master
                try:
                    m.is_sellable = True if request.form.get('is_sellable') else False
                    m.guiding_part = True if request.form.get('guiding_part') else False
                except Exception:
                    pass
                # Set basic fields
                m.description = description
                try:
                    m.weight = round(float(weight_val), 3) if weight_val else None
                except (ValueError, TypeError):
                    m.weight = None
                m.work_phase_id = int(phase_id) if phase_id else None
                m.supplier_id = int(supplier_id) if supplier_id else None
                m.work_center_id = int(center_id) if center_id else None
                m.processing_type = processing_type
                # Convert standard time (hours) to minutes
                try:
                    std_hours = float(standard_time_input) if standard_time_input else None
                except (ValueError, TypeError):
                    std_hours = None
                m.standard_time = std_hours * 60.0 if std_hours is not None else None
                # Convert lead times (days) to minutes
                def _to_minutes(dval: str | None) -> float | None:
                    try:
                        return float(dval) * 1440.0 if dval not in (None, '') else None
                    except Exception:
                        return None
                m.lead_time_theoretical = _to_minutes(lead_time_theoretical)
                m.lead_time_real = _to_minutes(lead_time_real)
                # Parse additional cycles
                import json
                additional_cycles: list = []
                if cycles_json_str:
                    try:
                        parsed = json.loads(cycles_json_str)
                        if isinstance(parsed, list):
                            for cyc in parsed:
                                if isinstance(cyc, dict) and any(cyc.get(key) for key in [
                                    'work_phase_id', 'processing_type', 'supplier_id', 'work_center_id', 'standard_time', 'lead_time_theoretical', 'lead_time_real', 'processing_cost']):
                                    cleaned = {
                                        'work_phase_id': cyc.get('work_phase_id') or None,
                                        'processing_type': cyc.get('processing_type') or None,
                                        'supplier_id': cyc.get('supplier_id') or None,
                                        'work_center_id': cyc.get('work_center_id') or None,
                                        'standard_time': cyc.get('standard_time') or None,
                                        'lead_time_theoretical': cyc.get('lead_time_theoretical') or None,
                                        'lead_time_real': cyc.get('lead_time_real') or None,
                                        'processing_cost': cyc.get('processing_cost') or None,
                                    }
                                    additional_cycles.append(cleaned)
                    except Exception:
                        additional_cycles = []
                # Compute processing cost.  Prefer user-provided aggregated cost, else compute.
                aggregated_cost: float | None = None
                if processing_cost_input:
                    try:
                        aggregated_cost = float(processing_cost_input)
                    except (ValueError, TypeError):
                        aggregated_cost = None
                if aggregated_cost is None:
                    total_cost = 0.0
                    # Primary cycle cost (internal only)
                    if processing_type == 'internal' and std_hours is not None and m.work_center_id:
                        wc_obj = WorkCenter.query.get(m.work_center_id)
                        if wc_obj and wc_obj.hourly_cost is not None:
                            try:
                                total_cost += std_hours * float(wc_obj.hourly_cost)
                            except Exception:
                                pass
                    # Additional cycles cost
                    for cycle in additional_cycles:
                        try:
                            ctype = cycle.get('processing_type')
                            if ctype == 'internal':
                                st_val = cycle.get('standard_time')
                                wc_id = cycle.get('work_center_id')
                                if st_val and wc_id:
                                    st_float = float(st_val)
                                    wc_obj = WorkCenter.query.get(int(wc_id))
                                    if wc_obj and wc_obj.hourly_cost is not None:
                                        total_cost += st_float * float(wc_obj.hourly_cost)
                            elif ctype == 'external':
                                cost_str = cycle.get('processing_cost')
                                if cost_str:
                                    total_cost += float(cost_str)
                        except Exception:
                            pass
                    aggregated_cost = total_cost if total_cost > 0 else None
                m.processing_cost = aggregated_cost
                # Compose notes JSON if cycles exist
                if additional_cycles:
                    try:
                        m.notes = json.dumps({"notes": notes_text or "", "cycles": additional_cycles})
                    except Exception:
                        m.notes = notes_text
                else:
                    m.notes = notes_text
                # Persist master
                db.session.commit()
                # Mirror master fields back to component to ensure values are visible immediately
                try:
                    component.description = m.description
                    component.weight = m.weight
                    component.work_phase_id = m.work_phase_id
                    component.supplier_id = m.supplier_id
                    component.work_center_id = m.work_center_id
                    component.processing_type = m.processing_type
                    component.standard_time = m.standard_time
                    component.lead_time_theoretical = m.lead_time_theoretical
                    component.lead_time_real = m.lead_time_real
                    component.processing_cost = m.processing_cost
                    # Copy only the plain notes from the master to the component
                    component.notes = extract_plain_notes(m.notes)
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                flash('Componente aggiornato.', 'success')
            else:
                # No master: fall back to updating the component only
                # Convert helpers for minutes
                def to_minutes(days):
                    try:
                        return float(days) * 1440.0
                    except (ValueError, TypeError):
                        return None
                component.work_phase_id = int(phase_id) if phase_id else None
                # Safely convert supplier identifier to integer if possible
                if supplier_id:
                    try:
                        component.supplier_id = int(supplier_id)
                    except (ValueError, TypeError):
                        component.supplier_id = None
                else:
                    component.supplier_id = None
                component.work_center_id = int(center_id) if center_id else None
                component.processing_type = processing_type
                try:
                    component.weight = round(float(weight_val), 3) if weight_val else None
                except (ValueError, TypeError):
                    component.weight = None
                try:
                    std_hours = float(standard_time_input) if standard_time_input else None
                except (ValueError, TypeError):
                    std_hours = None
                lt_theo_min = to_minutes(lead_time_theoretical)
                lt_real_min = to_minutes(lead_time_real)
                if processing_type == 'internal':
                    component.standard_time = std_hours * 60.0 if std_hours is not None else None
                    component.lead_time_theoretical = lt_theo_min
                    component.lead_time_real = lt_real_min
                    if std_hours is not None and component.work_center_id:
                        wc = WorkCenter.query.get(component.work_center_id)
                        if wc and wc.hourly_cost is not None:
                            component.processing_cost = std_hours * wc.hourly_cost
                        else:
                            component.processing_cost = None
                    else:
                        component.processing_cost = None
                elif processing_type == 'external':
                    component.standard_time = None
                    component.lead_time_theoretical = lt_theo_min
                    component.lead_time_real = lt_real_min
                    component.processing_cost = None
                else:
                    component.standard_time = std_hours * 60.0 if std_hours is not None else None
                    component.lead_time_theoretical = lt_theo_min
                    component.lead_time_real = lt_real_min
                    try:
                        component.processing_cost = float(processing_cost_input) if processing_cost_input else None
                    except (ValueError, TypeError):
                        component.processing_cost = None
                # Notes and cycles
                import json
                additional_cycles = []
                if cycles_json_str:
                    try:
                        parsed = json.loads(cycles_json_str)
                        if isinstance(parsed, list):
                            additional_cycles = parsed
                    except Exception:
                        additional_cycles = []
                if additional_cycles:
                    component.notes = json.dumps({"notes": notes_text or "", "cycles": additional_cycles})
                else:
                    component.notes = notes_text
                component.description = description
                db.session.commit()
                flash('Componente aggiornato.', 'success')
        elif category == 'commercial':
            # Parse form fields for commercial components.  The master holds
            # supplier, price and lead time information globally.  When a master
            # exists, update its fields; otherwise fall back to updating the
            # component directly.
            supplier_id = request.form.get('supplier_id') or None
            weight_val = request.form.get('weight') or None
            price_per_unit = request.form.get('price_per_unit') or None
            minimum_order_qty = request.form.get('minimum_order_qty') or None
            lead_time_theoretical = request.form.get('lead_time_theoretical') or None
            lead_time_real = request.form.get('lead_time_real') or None
            description = request.form.get('description') or None
            # Raw notes string from the form.  May be encoded into JSON with
            # additional flags such as lot_management.  We keep the original
            # value separately to construct the notes dictionary below.
            raw_notes = request.form.get('notes') or None
            # Capture whether the "gestione a lotti" toggle was checked.
            lot_mgmt_flag = True if request.form.get('lot_management') else False
            # Ensure a master exists
            m = getattr(component, 'component_master', None)
            if not m:
                try:
                    ensure_component_master_for_structure(struct)
                except Exception:
                    pass
                m = getattr(component, 'component_master', None)
            if m:
                # ------------------------------
                # Update fields on the master
                # ------------------------------
                # Persist sellable/guiding flags on the master when present
                try:
                    m.is_sellable = sellable_flag
                    m.guiding_part = guiding_flag
                except Exception:
                    # Some legacy masters may not have these attributes; ignore silently
                    pass
                # Update numeric and string fields on the master
                try:
                    m.supplier_id = int(supplier_id) if supplier_id else None
                except Exception:
                    m.supplier_id = None
                try:
                    m.weight = round(float(weight_val), 3) if weight_val else None
                except (ValueError, TypeError):
                    m.weight = None
                try:
                    m.price_per_unit = float(price_per_unit) if price_per_unit else None
                except (ValueError, TypeError):
                    m.price_per_unit = None
                try:
                    m.minimum_order_qty = int(minimum_order_qty) if minimum_order_qty else None
                except (ValueError, TypeError):
                    m.minimum_order_qty = None
                # Convert lead times (days) to minutes for persistence
                def _to_minutes(days_val):
                    try:
                        return float(days_val) * 1440.0 if days_val not in (None, '') else None
                    except Exception:
                        return None
                m.lead_time_theoretical = _to_minutes(lead_time_theoretical)
                m.lead_time_real = _to_minutes(lead_time_real)
                # Update description on master
                m.description = description
                # Build a notes dictionary containing the user‑entered notes and optional lot flag
                notes_dict: dict[str, Any] = {}
                notes_dict['notes'] = raw_notes or ''
                if lot_mgmt_flag:
                    notes_dict['lot_management'] = True
                # Persist the JSON string on the master; if encoding fails, fall back to raw notes
                try:
                    m.notes = json.dumps(notes_dict)
                except Exception:
                    m.notes = raw_notes or ''
                # Commit master changes so that the id is set and values are saved
                db.session.commit()
                # ------------------------------
                # Mirror master values onto the component for immediate display
                # ------------------------------
                try:
                    component.supplier_id = m.supplier_id
                    component.weight = m.weight
                    component.price_per_unit = m.price_per_unit
                    component.minimum_order_qty = m.minimum_order_qty
                    component.lead_time_theoretical = m.lead_time_theoretical
                    component.lead_time_real = m.lead_time_real
                    component.description = m.description
                    # Only copy the plain notes to the component so the UI never shows raw JSON
                    component.notes = extract_plain_notes(m.notes)
                    component.is_sellable = sellable_flag
                    component.guiding_part = guiding_flag
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                flash('Componente aggiornato.', 'success')
            else:
                # ------------------------------
                # No master exists – update the component and structure directly
                # ------------------------------
                # Parse numeric fields on the component
                try:
                    component.supplier_id = int(supplier_id) if supplier_id else None
                except Exception:
                    component.supplier_id = None
                try:
                    component.weight = round(float(weight_val), 3) if weight_val else None
                except (ValueError, TypeError):
                    component.weight = None
                try:
                    component.price_per_unit = float(price_per_unit) if price_per_unit else None
                except (ValueError, TypeError):
                    component.price_per_unit = None
                try:
                    component.minimum_order_qty = int(minimum_order_qty) if minimum_order_qty else None
                except (ValueError, TypeError):
                    component.minimum_order_qty = None
                # Convert lead times (days) to minutes for persistence
                def to_minutes(days_val):
                    try:
                        return float(days_val) * 1440.0 if days_val not in (None, '') else None
                    except Exception:
                        return None
                component.lead_time_theoretical = to_minutes(lead_time_theoretical)
                component.lead_time_real = to_minutes(lead_time_real)
                # Update description on the component
                component.description = description
                # Build a notes dictionary containing the user‑entered notes and optional lot flag
                notes_dict: dict[str, Any] = {}
                notes_dict['notes'] = raw_notes or ''
                if lot_mgmt_flag:
                    notes_dict['lot_management'] = True
                # Store the JSON notes on the underlying structure so that the batch flag
                # can be detected via structure.notes during production box rendering.  When
                # JSON encoding fails, fall back to raw notes.
                try:
                    component.structure.notes = json.dumps(notes_dict)
                except Exception:
                    component.structure.notes = raw_notes or ''
                # Always store only the plain notes on the component itself to avoid
                # exposing JSON in the UI.
                component.notes = raw_notes or ''
                # Persist sellable/guiding flags directly on the component
                try:
                    component.is_sellable = sellable_flag
                    component.guiding_part = guiding_flag
                except Exception:
                    pass
                db.session.commit()
                flash('Componente aggiornato.', 'success')
        # Assemblies are read‑only; no update occurs
        return redirect(url_for('products.component_detail', product_id=product_id, component_id=component_id))
    # Prepare dictionary lists for selects
    suppliers = Supplier.query.order_by(Supplier.name.asc()).all()
    work_centers = WorkCenter.query.order_by(WorkCenter.name.asc()).all()
    work_phases = WorkPhase.query.order_by(WorkPhase.name.asc()).all()
    # When displaying a part component, attempt to extract any additional
    # processing cycles from the notes field.  If the notes field contains
    # a JSON object with keys "notes" and "cycles", assign the cycles to
    # additional_cycles and restore component.notes to just the textual notes.
    additional_cycles = []
    if category == 'part':
        try:
            import json
            if component.notes:
                parsed = json.loads(component.notes)
                if isinstance(parsed, dict) and 'cycles' in parsed:
                    cycles = parsed.get('cycles')
                    if isinstance(cycles, list):
                        additional_cycles = cycles
                    # Replace component.notes with the plain notes for display
                    component.notes = parsed.get('notes', '')
        except Exception:
            additional_cycles = []
        # If no additional cycles were found in the component's notes, and the
        # component has a master with cycles_json defined, populate
        # additional_cycles from the master.  The master stores cycles as a
        # JSON-encoded list under the ``cycles_json`` attribute.  Do not
        # override existing cycles; only use master cycles when none are
        # present on the component itself.
        if not additional_cycles:
            try:
                m = getattr(component, 'component_master', None)
                if m and getattr(m, 'cycles_json', None):
                    parsed_master = json.loads(m.cycles_json)
                    if isinstance(parsed_master, list):
                        additional_cycles = parsed_master
            except Exception:
                pass
    # Derive the plain notes string for the notes input.  The template
    # references `notes_value` to populate the notes field.  Use the
    # current component.notes (which may have been replaced above) when
    # available; otherwise fall back to an empty string.  This avoids
    # leaking any JSON-encoded cycles or additional flags into the notes input.
    notes_value = component.notes or ''
    # If notes is JSON (e.g. contains cycles or lot_management), extract the plain notes
    try:
        parsed_notes = json.loads(component.notes) if component.notes else None
        if isinstance(parsed_notes, dict):
            notes_value = parsed_notes.get('notes', '')
            # Replace component.notes for display so templates don't see JSON when iterating
            component.notes = notes_value
    except Exception:
        pass

    # Determine batch management flag (gestione a lotti) for commercial parts.
    lot_management_flag = False
    if category == 'commercial':
        # Attempt to parse the notes JSON and extract the lot_management key
        try:
            if component.notes:
                parsed_notes = json.loads(component.notes)
                if isinstance(parsed_notes, dict) and 'lot_management' in parsed_notes:
                    lot_management_flag = bool(parsed_notes.get('lot_management'))
            elif getattr(component, 'component_master', None) and component.component_master and component.component_master.notes:
                parsed_notes = json.loads(component.component_master.notes)
                if isinstance(parsed_notes, dict) and 'lot_management' in parsed_notes:
                    lot_management_flag = bool(parsed_notes.get('lot_management'))
        except Exception:
            lot_management_flag = False
    # ---------------------------------------------------------------------
    # Before rendering the component detail page, gather any documents that
    # have been previously uploaded for this component.  Documents are
    # stored under static/documents/<product>/<structure path>/<folder>.  We
    # determine the relevant folders based on the component category and
    # build a dictionary mapping each folder to a list of file names and
    # relative paths.  This information is passed to the template so it
    # can display links to the existing documents.
    existing_documents: dict[str, list[dict]] = {}
    if category == 'part':
        doc_folders = ['qualita', '3_1_materiale', 'step_tavole', 'altro']
    elif category == 'assembly':
        doc_folders = ['qualita', 'step_tavole', 'funzionamento', 'istruzioni', 'altro']
    else:
        doc_folders = ['qualita', 'ddt_fornitore', 'step_tavole', '3_1_materiale', 'altro']
    def _safe(name: str) -> str:
        return secure_filename(name) or 'unnamed'
    prod_dir = _safe(product.name)
    struct_path_parts: list[str] = []
    def _collect_path(node):
        if node.parent:
            _collect_path(node.parent)
        struct_path_parts.append(_safe(node.name))
    _collect_path(struct)
    base_path = os.path.join(current_app.static_folder, 'documents', prod_dir, *struct_path_parts)
    # When the structure has a non‑empty revision label, prefer a
    # revision‑specific folder for documents.  The revision folder is
    # named using the safe base name with a `_Rev_<letter>` suffix
    # (e.g. "chiodo_Rev_A").  If such a directory exists it means a
    # revision has been created and no files from previous revisions
    # should be displayed.  Only update the path when the revision
    # directory exists.
    try:
        # Determine the revision letter from the structure
        rev_label = struct.revision_label  # e.g. 'Rev.A'
        rev_letter = None
        if isinstance(rev_label, str) and '.' in rev_label:
            rev_letter = rev_label.split('.')[-1]
        if rev_letter:
            from werkzeug.utils import secure_filename
            # Build a new folder name incorporating the revision letter (e.g. "partname_Rev_A").
            safe_base = secure_filename(struct.name) or 'unnamed'
            rev_name = f"{safe_base}_Rev_{rev_letter}"
            # Construct alternate path segments by replacing the last element of
            # the structure path with the revision-specific folder.  When
            # struct_path_parts is empty (root node), simply use the revision name.
            alt_parts = struct_path_parts[:-1] + [rev_name] if struct_path_parts else [rev_name]
            # Update the base path to point at the revision-specific directory.  This
            # prevents documents from previous revisions from appearing in the list.
            alt_base = os.path.join(current_app.static_folder, 'documents', prod_dir, *alt_parts)
            base_path = alt_base
            # Replace the structure path parts with the revision-specific parts so
            # that relative file paths reflect the correct directory.  Without
            # this assignment, rel_path values would continue to reference the
            # original folder name (e.g. ``partname``) rather than the revision
            # folder (e.g. ``partname_Rev_A``), leading to mismatches when
            # comparing against checklist entries and when downloading files.
            struct_path_parts = alt_parts
    except Exception:
        pass
    for folder in doc_folders:
        files: list[dict] = []
        folder_path = os.path.join(base_path, folder)
        try:
            if os.path.isdir(folder_path):
                for fname in os.listdir(folder_path):
                    full_path = os.path.join(folder_path, fname)
                    if os.path.isfile(full_path):
                        rel_path = os.path.join('documents', prod_dir, *struct_path_parts, folder, fname)
                        files.append({'name': fname, 'path': rel_path})
        except Exception:
            # Ignore errors accessing the folder; treat as no files
            pass
        existing_documents[folder] = files
    # Also gather any default documents defined at the structure type or node level.
    # Default documents are stored under static/tmp_structures/<type>/<node path>/<folder>.
    # When the structure has been revised (revision_label is non‑empty), skip
    # merging any default or master documents so that the new revision starts
    # with no inherited files.  Only use default documents for initial
    # (non‑revised) structures.
    try:
        if not struct.revision_label:
            # Determine the type directory using the structure's type name
            type_dir = _safe(struct.type.name)
            # Build paths for default documents: one for node-level defaults (uses the
            # hierarchical struct_path_parts) and one for type-level defaults (just
            # the type directory).  Default documents can be defined at either
            # granularity.
            base_default_node = os.path.join(current_app.static_folder, 'tmp_structures', type_dir, *struct_path_parts)
            base_default_type = os.path.join(current_app.static_folder, 'tmp_structures', type_dir)
            for folder in doc_folders:
                files = existing_documents.get(folder, [])
                # Helper to merge files from a given default folder path
                def _merge_default(folder_path: str, rel_prefix_parts: list[str]):
                    if os.path.isdir(folder_path):
                        for fname in os.listdir(folder_path):
                            full_default_path = os.path.join(folder_path, fname)
                            if os.path.isfile(full_default_path):
                                rel_default_path = os.path.join(*rel_prefix_parts, folder, fname)
                                # Avoid duplicate file names
                                if not any(d['name'] == fname for d in files):
                                    files.append({'name': fname, 'path': rel_default_path})
                # Node-level defaults
                _merge_default(os.path.join(base_default_node, folder), ['tmp_structures', type_dir, *struct_path_parts])
                # Type-level defaults
                _merge_default(os.path.join(base_default_type, folder), ['tmp_structures', type_dir])
                # Master-level defaults: docs stored under tmp_components/<code>/<folder>
                try:
                    m = getattr(component, 'component_master', None)
                    if m:
                        code_dir = _safe(m.code)
                        base_master = os.path.join(current_app.static_folder, 'tmp_components', code_dir)
                        _merge_default(os.path.join(base_master, folder), ['tmp_components', code_dir])
                except Exception:
                    pass
                existing_documents[folder] = files
    except Exception:
        pass
    # Human‑friendly labels for each document folder.  If additional
    # folders are introduced in the future, update this mapping as well.
    doc_label_map: dict[str, str] = {
        'qualita': 'Modulo Cert. qualità',
        '3_1_materiale': '3.1 Materiale',
        'step_tavole': 'Step/tavola',
        'funzionamento': 'Verifica funzionamento',
        'istruzioni': 'Montaggio istruzioni',
        'ddt_fornitore': 'DDT fornitore',
        'altro': 'Altro',
    }
    # Compute the set of flagged documents for this structure.  Flags are
    # persisted in the checklist file under the structure's id and store the
    # relative path of each required document.  To robustly match these
    # stored paths against the actual files available for the component,
    # normalise each stored path, extract the folder and filename, and
    # compare them to the entries in ``existing_documents``.  This
    # approach avoids issues when the product name or structure path changes
    # (e.g. due to renaming or revision) because file names and categories
    # remain constant.
    try:
        cl_data = load_checklist()
        # Determine the structure id used as the key in the checklist
        try:
            struct_id = component.structure.id if component and component.structure else None
        except Exception:
            struct_id = None
        if struct_id is not None:
            raw_paths = cl_data.get(str(struct_id), []) or []
            # Build a set of (folder, filename) pairs and a set of filenames
            # from the stored paths.  As in the category view, we normalise
            # paths by replacing backslashes with forward slashes and
            # trimming whitespace.  Matching against both folder/filename and
            # filename alone makes the checklist resilient to product or
            # revision path changes.
            flagged_names: set[tuple[str, str]] = set()
            flagged_filenames: set[str] = set()
            flagged_basenames: set[str] = set()
            for p in raw_paths:
                # Normalise each stored path by converting backslashes to forward slashes
                # and trimming whitespace.  Skip any non-string entries or empty strings.
                if not isinstance(p, str):
                    continue
                try:
                    norm = p.replace('\\', '/').strip()
                except Exception:
                    norm = p
                if not norm:
                    continue
                parts = norm.split('/')
                if len(parts) >= 2:
                    folder = parts[-2]
                    filename = parts[-1]
                    # Lowercase both folder and filename for case-insensitive matching
                    folder_lower = str(folder).lower()
                    filename_lower = str(filename).lower()
                    flagged_names.add((folder_lower, filename_lower))
                    # Always include the raw filename
                    flagged_filenames.add(filename_lower)
                    try:
                        # Compact form with spaces/underscores removed
                        filename_sanitised = re.sub(r'[\s_]+', '', filename_lower)
                        if filename_sanitised:
                            flagged_filenames.add(filename_sanitised)
                        # Replace spaces with underscores and underscores with spaces
                        variant_spaces_to_underscores = filename_lower.replace(' ', '_')
                        variant_underscores_to_spaces = filename_lower.replace('_', ' ')
                        if variant_spaces_to_underscores:
                            flagged_filenames.add(variant_spaces_to_underscores)
                        if variant_underscores_to_spaces:
                            flagged_filenames.add(variant_underscores_to_spaces)
                    except Exception:
                        pass
                    # Derive the base name (without extension and compiled suffix) in lowercase
                    base_no_ext = filename_lower.rsplit('.', 1)[0]
                    base_original = base_no_ext.split('_compiled_', 1)[0]
                    if base_original:
                        # Add the raw base
                        flagged_basenames.add(base_original)
                        try:
                            # Include compact and swapped variants for the base
                            base_sanitised = re.sub(r'[\s_]+', '', base_original)
                            if base_sanitised:
                                flagged_basenames.add(base_sanitised)
                            variant_spaces_to_underscores = base_original.replace(' ', '_')
                            variant_underscores_to_spaces = base_original.replace('_', ' ')
                            if variant_spaces_to_underscores:
                                flagged_basenames.add(variant_spaces_to_underscores)
                            if variant_underscores_to_spaces:
                                flagged_basenames.add(variant_underscores_to_spaces)
                        except Exception:
                            pass
            # Traverse existing_documents and select any file whose (folder,
            # filename) pair matches a flagged name or whose filename alone
            # matches a flagged filename.  Include the full path so that
            # checkboxes render as checked.
            matches: set[str] = set()
            for folder, files in existing_documents.items():
                for file_dict in files:
                    try:
                        fname = file_dict.get('name')
                    except Exception:
                        fname = None
                    if fname:
                        # Lowercase both folder and filename for case-insensitive matching
                        folder_lower = str(folder).lower()
                        fname_lower = str(fname).lower()
                        # Base name of current file (without extension and compiled suffix)
                        fname_no_ext = fname_lower.rsplit('.', 1)[0]
                        fname_base = fname_no_ext.split('_compiled_', 1)[0]
                        # Compute sanitised versions of the filename and base name by removing
                        # both spaces and underscores.  This allows matching between names
                        # like "data sheet" and "data_sheet".
                        try:
                            fname_sanitised = re.sub(r'[\s_]+', '', fname_lower)
                        except Exception:
                            fname_sanitised = fname_lower
                        try:
                            fname_base_sanitised = re.sub(r'[\s_]+', '', fname_base)
                        except Exception:
                            fname_base_sanitised = fname_base
                        # A match occurs when either the exact folder and filename pair
                        # (in lowercase) is present in flagged_names, the filename (raw or
                        # sanitised) appears in flagged_filenames, or the base name (raw
                        # or sanitised) matches one of the flagged_basenames.  Matching
                        # on sanitised names accommodates differences in spacing and
                        # underscore usage.
                        if (
                            (folder_lower, fname_lower) in flagged_names
                            or fname_lower in flagged_filenames
                            or fname_sanitised in flagged_filenames
                            or fname_base in flagged_basenames
                            or fname_base_sanitised in flagged_basenames
                        ):
                            try:
                                matches.add(file_dict['path'])
                            except Exception:
                                pass
            # Additionally, include any stored paths themselves.  This allows
            # flags referring to default documents in tmp_components or
            # tmp_structures to remain selected when those files are not
            # present in the per-product documents list.  Normalise the
            # stored paths before adding.
            for p in raw_paths:
                if not isinstance(p, str):
                    continue
                try:
                    norm = p.replace('\\', '/').strip()
                except Exception:
                    norm = p
                if norm:
                    matches.add(norm)
            flagged_docs = matches
            # Capture the per-structure flagged filenames and base names for the template.
            flagged_filenames_lower = flagged_filenames
            flagged_basenames_lower = flagged_basenames
        else:
            flagged_docs = set()
            flagged_filenames_lower = set()
            flagged_basenames_lower = set()
    except Exception:
        flagged_docs = set()
        flagged_filenames_lower = set()
        flagged_basenames_lower = set()
    return render_template(
        'products/component_detail.html',
        product=product,
        component=component,
        category=category,
        suppliers=suppliers,
        work_centers=work_centers,
        work_phases=work_phases,
        additional_cycles=additional_cycles,
        existing_documents=existing_documents,
        doc_label_map=doc_label_map,
        # Provide the total weight of all descendant parts for assemblies.
        parts_weight=parts_weight,
        # Provide the plain notes value separate from component.notes so the
        # template uses the correct value even when component.notes is JSON.
        notes_value=notes_value,
        # Provide the master image filename so the template can display it when
        # the component itself has no uploaded image.
        master_image_filename=master_image_filename,
        # Provide return_to parameter for back navigation
        return_to=request.args.get('return_to'),
        # Provide lot management flag for commercial parts
        lot_management=lot_management_flag,
        # Pass flagged documents for the current structure to mark checkboxes
        flagged_docs=flagged_docs,
        flagged_filenames=flagged_filenames_lower,
        flagged_basenames=flagged_basenames_lower,
    )

@products_bp.route('/create', methods=['GET', 'POST'])
@login_required
def create():
    """Create a new product and associate selected structures."""
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        selected_components = request.form.getlist('components')
        # Handle uploaded image
        image_file = request.files.get('image')
        def allowed_file(filename: str) -> bool:
            return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'png', 'jpg', 'jpeg', 'gif', 'bmp'}

        if not name:
            flash('Il nome del prodotto è obbligatorio.', 'danger')
        else:
            product = Product(name=name, description=description)
            db.session.add(product)
            db.session.flush()
            # Associate selected structures
            for sid in selected_components:
                try:
                    sid_int = int(sid)
                except (TypeError, ValueError):
                    continue
                qty_str = request.form.get(f'qty_{sid_int}', '1').strip()
                try:
                    qty = int(qty_str)
                    if qty < 1:
                        qty = 1
                except ValueError:
                    qty = 1
                comp = ProductComponent(product_id=product.id, structure_id=sid_int, quantity=qty)
                # If the selected structure has a component master, record the
                # master id on the product component.  This ensures the
                # component shares its canonical attributes with other
                # occurrences of the same code.  Assigning component_id does
                # not preclude overriding individual fields on the component.
                try:
                    structure = Structure.query.get(sid_int)
                    if structure and structure.component_id:
                        comp.component_id = structure.component_id
                except Exception:
                    structure = None
                # Assign default attributes from the structure or its type.  When
                # creating new components from selected structures we inherit
                # values defined on the structure itself.  If no per‑node
                # defaults are defined we fall back to the structure type
                # defaults.  This makes subsequent editing of components
                # easier and provides sensible initial values for cost and
                # processing details.
                # Use the previously fetched structure (if any) to inherit default values
                if structure:
                    # Use structure-level defaults when present
                    if structure.weight is not None:
                        comp.weight = structure.weight
                    if structure.processing_type:
                        comp.processing_type = structure.processing_type
                    if structure.work_phase_id:
                        comp.work_phase_id = structure.work_phase_id
                    if structure.supplier_id:
                        comp.supplier_id = structure.supplier_id
                    if structure.work_center_id:
                        comp.work_center_id = structure.work_center_id
                    if structure.standard_time is not None:
                        comp.standard_time = structure.standard_time
                    if structure.lead_time_theoretical is not None:
                        comp.lead_time_theoretical = structure.lead_time_theoretical
                    if structure.lead_time_real is not None:
                        comp.lead_time_real = structure.lead_time_real
                    if structure.description:
                        comp.description = structure.description
                    if structure.notes:
                        comp.notes = structure.notes
                    if structure.price_per_unit is not None:
                        comp.price_per_unit = structure.price_per_unit
                    if structure.minimum_order_qty is not None:
                        comp.minimum_order_qty = structure.minimum_order_qty
                    # Compute processing cost for internal processes
                    if structure.processing_type == 'internal' and structure.standard_time is not None and structure.work_center_id:
                        wc = WorkCenter.query.get(structure.work_center_id)
                        if wc and wc.hourly_cost is not None:
                            comp.processing_cost = (structure.standard_time / 60.0) * wc.hourly_cost
                    elif structure.processing_type == 'external' and structure.processing_cost is not None:
                        comp.processing_cost = structure.processing_cost
                # If still not set, fall back to type defaults
                if not comp.weight and structure and structure.type:
                    stype = structure.type
                    if stype.default_weight is not None:
                        comp.weight = stype.default_weight
                    if stype.default_processing_type:
                        comp.processing_type = stype.default_processing_type
                    if stype.default_work_phase_id:
                        comp.work_phase_id = stype.default_work_phase_id
                    if stype.default_supplier_id:
                        comp.supplier_id = stype.default_supplier_id
                    if stype.default_work_center_id:
                        comp.work_center_id = stype.default_work_center_id
                    if stype.default_standard_time is not None:
                        comp.standard_time = stype.default_standard_time
                    if stype.default_lead_time_theoretical is not None:
                        comp.lead_time_theoretical = stype.default_lead_time_theoretical
                    if stype.default_lead_time_real is not None:
                        comp.lead_time_real = stype.default_lead_time_real
                    if stype.default_description:
                        comp.description = stype.default_description
                    if stype.default_notes:
                        comp.notes = stype.default_notes
                    if stype.default_price_per_unit is not None:
                        comp.price_per_unit = stype.default_price_per_unit
                    if stype.default_minimum_order_qty is not None:
                        comp.minimum_order_qty = stype.default_minimum_order_qty
                    # Compute processing cost using type defaults
                    if stype.default_processing_type == 'internal' and stype.default_standard_time is not None and stype.default_work_center_id:
                        wc2 = WorkCenter.query.get(stype.default_work_center_id)
                        if wc2 and wc2.hourly_cost is not None:
                            comp.processing_cost = (stype.default_standard_time / 60.0) * wc2.hourly_cost
                    elif stype.default_processing_type == 'external' and stype.default_processing_cost is not None:
                        comp.processing_cost = stype.default_processing_cost
                db.session.add(comp)
            db.session.commit()
            # Process image upload after the product has an id
            if image_file and allowed_file(image_file.filename):
                filename = secure_filename(image_file.filename)
                # Prepend product id to ensure uniqueness
                filename = f"{product.id}_{filename}"
                # Determine upload directory relative to static
                upload_dir = os.path.join(current_app.static_folder, 'uploads')
                os.makedirs(upload_dir, exist_ok=True)
                path = os.path.join(upload_dir, filename)
                image_file.save(path)
                product.image_filename = filename
                db.session.commit()
            flash('Prodotto creato.', 'success')
            return redirect(url_for('products.index'))
    # On GET or validation errors, render form
    # Group structures by their type for easier display in the form
    types = StructureType.query.order_by(StructureType.name.asc()).all()
    nodes_by_type = {t.id: Structure.query.filter_by(type_id=t.id).order_by(Structure.name.asc()).all() for t in types}
    return render_template('products/create.html', types=types, nodes_by_type=nodes_by_type)

@products_bp.route('/<int:id>')
@login_required
def detail(id: int):
    """Show details for a single product.

    This view collects both legacy structure‑based components and the new
    hierarchical bill of materials (BOM) relationships.  It also retrieves
    custom fields defined for products and prefetches any existing values for
    the current product.  The resulting context is passed to the template
    which renders tabs for product info, structures and the BOM tree.
    """
    product = Product.query.get_or_404(id)

    # Prefetch all components associated with this product for display
    components = (ProductComponent.query
                  .join(Structure)
                  .filter(ProductComponent.product_id == product.id)
                  .all())
    # ---------------------------------------------------------------------
    # Before constructing the BOM tree, prefill component fields from their
    # associated ComponentMaster records.  Similar to the logic used in
    # the category_table view, this ensures that when a product component
    # lacks certain attributes (description, notes, weight, etc.) the
    # values defined on its master record are displayed instead.  Also
    # fallback to the master image when a component does not have its own
    # image.  These assignments affect only the in‑memory objects used for
    # rendering and do not persist to the database.
    try:
        upload_dir = os.path.join(current_app.static_folder, 'uploads')
    except Exception:
        upload_dir = None
    for comp in components:
        try:
            # Helper to decide whether a string field is empty
            def _empty(val: str | None) -> bool:
                return val is None or (isinstance(val, str) and val.strip() == '')
            master = getattr(comp, 'component_master', None)
            # -----------------------------------------------------------------
            # Prefill description and notes.  Prefer component's own fields; if
            # empty fall back to master, then to the structure itself (legacy
            # description/notes).  Finally fall back to the structure type.
            if _empty(getattr(comp, 'description', None)):
                if master and not _empty(getattr(master, 'description', None)):
                    comp.description = master.description
                elif not _empty(getattr(comp.structure, 'description', None)):
                    comp.description = comp.structure.description
            if _empty(getattr(comp, 'notes', None)):
                if master and not _empty(getattr(master, 'notes', None)):
                    # Populate notes with the plain text portion of the master notes
                    comp.notes = extract_plain_notes(master.notes)
                elif not _empty(getattr(comp.structure, 'notes', None)):
                    # Fall back to the plain notes on the structure
                    comp.notes = extract_plain_notes(comp.structure.notes)
            # Weight
            if getattr(comp, 'weight', None) is None:
                if master and getattr(master, 'weight', None) is not None:
                    comp.weight = master.weight
                elif getattr(comp.structure, 'weight', None) is not None:
                    comp.weight = comp.structure.weight
            # Processing parameters and commercial fields
            if _empty(getattr(comp, 'processing_type', None)):
                if master and not _empty(getattr(master, 'processing_type', None)):
                    comp.processing_type = master.processing_type
                elif not _empty(getattr(comp.structure, 'processing_type', None)):
                    comp.processing_type = comp.structure.processing_type
            if getattr(comp, 'work_phase_id', None) is None:
                if master and getattr(master, 'work_phase_id', None) is not None:
                    comp.work_phase_id = master.work_phase_id
                elif getattr(comp.structure, 'work_phase_id', None) is not None:
                    comp.work_phase_id = comp.structure.work_phase_id
            if getattr(comp, 'supplier_id', None) is None:
                if master and getattr(master, 'supplier_id', None) is not None:
                    comp.supplier_id = master.supplier_id
                elif getattr(comp.structure, 'supplier_id', None) is not None:
                    comp.supplier_id = comp.structure.supplier_id
            if getattr(comp, 'work_center_id', None) is None:
                if master and getattr(master, 'work_center_id', None) is not None:
                    comp.work_center_id = master.work_center_id
                elif getattr(comp.structure, 'work_center_id', None) is not None:
                    comp.work_center_id = comp.structure.work_center_id
            if getattr(comp, 'standard_time', None) is None:
                if master and getattr(master, 'standard_time', None) is not None:
                    comp.standard_time = master.standard_time
                elif getattr(comp.structure, 'standard_time', None) is not None:
                    comp.standard_time = comp.structure.standard_time
            if getattr(comp, 'lead_time_theoretical', None) is None:
                if master and getattr(master, 'lead_time_theoretical', None) is not None:
                    comp.lead_time_theoretical = master.lead_time_theoretical
                elif getattr(comp.structure, 'lead_time_theoretical', None) is not None:
                    comp.lead_time_theoretical = comp.structure.lead_time_theoretical
            if getattr(comp, 'lead_time_real', None) is None:
                if master and getattr(master, 'lead_time_real', None) is not None:
                    comp.lead_time_real = master.lead_time_real
                elif getattr(comp.structure, 'lead_time_real', None) is not None:
                    comp.lead_time_real = comp.structure.lead_time_real
            if getattr(comp, 'processing_cost', None) is None:
                if master and getattr(master, 'processing_cost', None) is not None:
                    comp.processing_cost = master.processing_cost
                elif getattr(comp.structure, 'processing_cost', None) is not None:
                    comp.processing_cost = comp.structure.processing_cost
            # Commercial fields
            if hasattr(comp, 'price_per_unit') and getattr(comp, 'price_per_unit', None) is None:
                if master and getattr(master, 'price_per_unit', None) is not None:
                    comp.price_per_unit = master.price_per_unit
                elif getattr(comp.structure, 'price_per_unit', None) is not None:
                    comp.price_per_unit = comp.structure.price_per_unit
            if hasattr(comp, 'minimum_order_qty') and getattr(comp, 'minimum_order_qty', None) is None:
                if master and getattr(master, 'minimum_order_qty', None) is not None:
                    comp.minimum_order_qty = master.minimum_order_qty
                elif getattr(comp.structure, 'minimum_order_qty', None) is not None:
                    comp.minimum_order_qty = comp.structure.minimum_order_qty
            # -----------------------------------------------------------------
            # Image fallback: choose the first available among component's own image,
            # master image (cm_<master id>_) and structure image (sn_<structure id>_).
            # Do not override if an image is already set.
            if not getattr(comp, 'image_filename', None) and upload_dir and os.path.isdir(upload_dir):
                # search for master image first
                chosen: str | None = None
                if master:
                    prefix_master = f"cm_{master.id}_"
                    try:
                        for fname in os.listdir(upload_dir):
                            if fname.startswith(prefix_master):
                                chosen = fname
                                break
                    except Exception:
                        pass
                # If not found, search for structure image
                if not chosen:
                    prefix_struct = f"sn_{comp.structure.id}_"
                    try:
                        for fname in os.listdir(upload_dir):
                            if fname.startswith(prefix_struct):
                                chosen = fname
                                break
                    except Exception:
                        pass
                if chosen:
                    comp.image_filename = chosen
        except Exception:
            # Silently continue on any failure to avoid breaking the page
            continue

    # Map structure_id to its product component (for quantity and details)
    comp_map = {comp.structure_id: comp for comp in components}

    # Identify root nodes: those structures whose parent is either None or not associated with the product
    root_pairs = []
    for comp in components:
        struct = comp.structure
        if struct.parent_id is None or struct.parent_id not in comp_map:
            root_pairs.append((struct, comp))

    # Recursively build rows representing the structure hierarchy. Each row
    # contains a numbering string (e.g. "1", "1.1"), the structure node,
    # and the corresponding component (if any). Children that are not part of
    # the product still appear with ``component`` set to ``None``.
    def build_rows(pairs, parent_num: str = ''):
        """
        Recursively construct a flat list of row dictionaries representing the
        hierarchy of structures.  Each row includes the following keys:

        - ``number``: a dot‑separated string (e.g. "1", "1.2") indicating the
          position within the tree.
        - ``structure``: the Structure instance for this row.
        - ``component``: the corresponding ProductComponent if one exists for the
          current product, otherwise ``None``.
        - ``parent_number``: the numbering string of this row's parent, or an
          empty string for top‑level entries.
        - ``has_children``: boolean indicating whether this row has any child
          structures.
        - ``depth``: integer depth level (0 for top level, 1 for first child, etc.).
        """
        rows = []
        for idx, (struct, comp) in enumerate(pairs, start=1):
            number = f"{idx}" if not parent_num else f"{parent_num}.{idx}"
            # Gather children pairs for this structure
            children: list[tuple[Structure, ProductComponent | None]] = []
            for child in struct.children:
                child_comp = comp_map.get(child.id)
                children.append((child, child_comp))
            has_children = len(children) > 0
            rows.append({
                'number': number,
                'structure': struct,
                'component': comp,
                'parent_number': parent_num,
                'has_children': has_children,
                'depth': number.count('.')
            })
            if has_children:
                rows.extend(build_rows(children, number))
        return rows

    structure_rows = build_rows(root_pairs)

    # Calcola il peso totale dei componenti di questo prodotto.  Per ogni
    # componente con un peso definito moltiplichiamo il peso per la
    # quantità.  Se il peso o la quantità non sono definiti o non sono
    # numerici, quel componente viene ignorato nella somma.
    total_weight = 0.0
    for comp in components:
        if comp.weight is not None and comp.quantity is not None:
            try:
                total_weight += float(comp.weight) * (comp.quantity or 0)
            except Exception:
                pass

    # Costruisce una struttura ad albero per i componenti.  Ogni
    # elemento in structure_rows riceve una lista ``children`` contenente
    # le righe figlie.  Gli elementi radice sono quelli con parent_number
    # vuoto.
    number_map = {row['number']: row for row in structure_rows}
    for row in structure_rows:
        row['children'] = []
    for row in structure_rows:
        # parent_number è una stringa; radice se vuota
        parent_num = row['parent_number']
        if parent_num:
            parent = number_map.get(parent_num)
            if parent:
                parent['children'].append(row)
    rows_tree = [row for row in structure_rows if not row['parent_number']]

    # -----------------------------------------------------------------
    # Gather product‑level documents for display.  Documents uploaded at
    # the product level are stored under ``static/documents/<product>``.
    # Each subfolder inside this directory represents a document category
    # (e.g. "qualita", "manuale", "disegni", "altro").  Build a
    # dictionary mapping the folder name to a list of files.  Each file is
    # represented as a dictionary with ``name`` and ``path`` keys.  If
    # no documents exist the dictionary will be empty.  Errors are
    # suppressed to avoid breaking the product detail page if the
    # directories do not exist or are unreadable.
    product_documents: dict[str, list[dict[str, str]]] = {}
    try:
        prod_dir = secure_filename(product.name) or 'unnamed'
        base_doc_dir = os.path.join(current_app.static_folder, 'documents', prod_dir)
        if os.path.isdir(base_doc_dir):
            for folder in os.listdir(base_doc_dir):
                folder_path = os.path.join(base_doc_dir, folder)
                if os.path.isdir(folder_path):
                    files: list[dict[str, str]] = []
                    for fname in os.listdir(folder_path):
                        full_path = os.path.join(folder_path, fname)
                        if os.path.isfile(full_path):
                            rel_path = os.path.join('documents', prod_dir, folder, fname)
                            files.append({'name': fname, 'path': rel_path})
                    product_documents[folder] = files
    except Exception:
        # Ignore errors when listing product documents
        pass

    return render_template('products/detail.html',
                           product=product,
                           components=components,
                           structure_rows=structure_rows,
                           total_weight=total_weight,
                           rows_tree=rows_tree,
                           product_documents=product_documents)

# -----------------------------------------------------------------------------
# Product level document upload
#
# The product detail page allows users to upload general documentation that
# pertains to the product as a whole (e.g. manuals, certificates or other
# attachments not tied to a specific component).  The form includes a
# ``doc_type`` select that determines the subfolder within the product's
# documents directory.  Uploaded files are stored under
# ``static/documents/<product>/<doc_type>``.  Multiple files can be
# uploaded at once.  After saving the files, the user is redirected
# back to the product detail page and a success message is flashed.

@products_bp.route('/<int:id>/upload_documents', methods=['POST'])
@login_required
def upload_product_documents(id: int):
    """Handle file uploads for product‑level documents.

    This endpoint accepts POST requests containing uploaded files under
    the ``product_docs`` field and a ``doc_type`` field indicating the
    document category.  Files are saved into
    ``static/documents/<product>/<doc_type>``.  Errors during file
    saving are silently ignored; a success flash message is shown when
    at least one file is saved.
    """
    product = Product.query.get_or_404(id)
    # Determine document category; fallback to 'altro' when missing.
    doc_type = request.form.get('doc_type', 'altro')
    doc_type = secure_filename(doc_type) or 'altro'
    files = request.files.getlist('product_docs') or []
    prod_dir = secure_filename(product.name) or 'unnamed'
    dest_folder = os.path.join(current_app.static_folder, 'documents', prod_dir, doc_type)
    saved_any = False
    # Ensure the destination directory exists
    try:
        os.makedirs(dest_folder, exist_ok=True)
    except Exception:
        pass
    for f in files:
        if f and f.filename:
            filename = secure_filename(f.filename)
            try:
                f.save(os.path.join(dest_folder, filename))
                saved_any = True
            except Exception:
                # ignore errors on individual files
                pass
    if saved_any:
        flash('Documenti caricati.', 'success')
    return redirect(url_for('products.detail', id=id))

