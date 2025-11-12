"""API endpoints for warehouse operations.

This module defines a set of minimal JSON APIs to support the new
reservation, production and archive functionality requested for the
magazzino application.  The endpoints provide a thin abstraction
over the database models defined in ``app.models`` and are designed
to be consumed via asynchronous calls from the client‑side UI.

Key endpoints include:

* POST /api/reservations – create a new reservation and associated
  production box along with unique DataMatrix codes for each stock item.
* GET /api/production-box/<id> – retrieve details of a production box
  including its stock items and their document status.
* POST /api/production-box/<id>/load – finalise loading of a box,
  updating item statuses and recording scan events.
* GET /api/products/<id>/loaded – list all stock items loaded for
  a given product.
* GET /api/datamatrix/<code> – resolve a DataMatrix code to its
  underlying stock item and related entities.
* GET /api/products/<id>/archive – return the audit history of
  scan events for a product.

The JSON format loosely follows the specification outlined in the
project brief.  Some optional fields (e.g. guiding parts) are not
implemented to keep the initial version simple.
"""

import datetime
import json
from typing import Any

from flask import request, jsonify, abort
# Import current_user to capture the operator performing actions.  When
# running without an authenticated user (e.g. API calls from scripts),
# current_user.is_authenticated will be False and user_id will not be
# recorded in ScanEvent meta.
from flask_login import current_user

from . import api_bp
from ...extensions import db
from ...models import (
    Product,
    Reservation,
    ProductionBox,
    StockItem,
    ScanEvent,
    Document,
)


def _generate_box_code() -> str:
    """Generate a unique production box code.

    The format is ``BOX-YYYY-NNNNN`` where ``YYYY`` is the current
    calendar year and ``NNNNN`` is a zero‑padded sequential number.  The
    sequence is derived from the current count of boxes in the
    database.  Because ``ProductionBox.code`` is marked as unique, the
    combination of year and sequence will not collide within a single
    application instance.
    """
    year = datetime.datetime.utcnow().year
    count = ProductionBox.query.count() + 1
    return f"BOX-{year}-{count:05d}"


def _generate_datamatrix(stock_item_id: int, product: Product, type_label: str = 'PARTE', guide: str | None = None, component_code: str | None = None) -> str:
    """Generate a DataMatrix code for a stock item.

    A simple algorithm maps the stock item id to a two‑letter prefix and
    four‑digit suffix.  The prefix is calculated by dividing the id by
    10,000 (integer division) and converting the resulting number into
    two base‑26 characters.  The suffix is the remainder of the id
    modulo 10,000 padded to four digits.  This yields up to 6.76
    million unique combinations before wrapping, exceeding practical
    inventory needs.  The full DataMatrix follows the format:

    ``DMV1|P={product.code}|S={AA0000}|T={type_label}[|G={guide}]``

    Where ``product.code`` is the product's name (used here as a
    unique identifier), ``S`` is the generated prefix/suffix, ``T`` is
    the type of the item (PARTE, ASSIEME or COMMERCIALE) and ``G`` is
    an optional GUIDA field identifying a guiding parent code.
    """
    # Compute two‑letter prefix from id (0‑based)
    idx = max(stock_item_id - 1, 0)
    prefix_val = idx // 10000
    first_letter_index = prefix_val // 26
    second_letter_index = prefix_val % 26
    # Clamp to alphabet range
    first_letter_index = first_letter_index % 26
    second_letter_index = second_letter_index % 26
    letters = chr(ord('A') + first_letter_index) + chr(ord('A') + second_letter_index)
    suffix = f"{idx % 10000:04d}"
    base_code = f"{letters}{suffix}"
    # Use the provided component code when available; fall back to the product name
    code_part = component_code if component_code else product.name
    parts = [f"DMV1", f"P={code_part}", f"S={base_code}", f"T={type_label}"]
    if guide:
        parts.append(f"G={guide}")
    return '|'.join(parts)


# -----------------------------------------------------------------------------
# Component association endpoint
#
# When building an assembly operators need to link existing, produced
# components (stock items) to the assembly they are assembling.  This
# association establishes a parent/child relationship via the
# StockItem.parent_code field and removes the component from the
# general archive by placing it into a dedicated "ASSOCIATO" state.  A
# corresponding ScanEvent is recorded for traceability.  The client
# must provide the DataMatrix code of the assembly (``assembly_code``)
# and the code of the component being associated (``component_code``).
# Only stock items that are not already associated (i.e. have a
# null/empty parent_code) are eligible for association.  If the
# component is not found or has already been linked, an error is
# returned.  On success the endpoint returns ``{"status": "ok"}``.

@api_bp.route('/associate', methods=['POST'])
def associate_component() -> Any:
    """Associate a produced component with an assembly.

    Expects a JSON payload containing ``assembly_code`` and
    ``component_code``.  Finds the corresponding StockItem for the
    component, ensures it is eligible for association and then sets
    its ``parent_code`` to the provided assembly code.  The status of
    the stock item is updated to ``ASSOCIATO`` to exclude it from
    future archival listings.  A ScanEvent with action ``ASSOCIA``
    records the event.  Returns a JSON object indicating success or
    an error with an appropriate HTTP status code.
    """
    try:
        data = request.get_json(silent=True) or {}
    except Exception:
        data = {}
    assembly_code = data.get('assembly_code') or ''
    component_code = data.get('component_code') or ''
    if not assembly_code or not component_code:
        return jsonify({'error': 'assembly_code and component_code are required'}), 400
    # Look up the stock item for the provided component code.  Use
    # equality match on the full DataMatrix because multiple items may
    # share the same prefix but represent different physical units.
    item = StockItem.query.filter_by(datamatrix_code=component_code).first()
    if not item:
        return jsonify({'error': 'Component not found'}), 404
    # Ensure the item is not already linked to another assembly
    if item.parent_code:
        return jsonify({'error': 'Component already associated'}), 400
    # Update parent_code and set a distinct status so that the item
    # no longer appears in the archive views.  Status values other
    # than LIBERO, PRENOTATO, IN_PRODUZIONE, CARICATO, COMPLETATO and
    # SCARTO are accepted by the database; using "ASSOCIATO" conveys
    # that the item has been consumed by an assembly.
    item.parent_code = assembly_code
    item.status = 'ASSOCIATO'
    # Record a scan event for auditing.  Include the assembly code in
    # the meta field so that later review can determine which assembly the
    # component was linked to.  Persist the operator's identifier and
    # email when available.  Additionally, capture static component details
    # (name, description and revision) so that they remain unchanged even if
    # the underlying anagrafica is updated later.
    try:
        meta_dict: dict[str, Any] = {'assembly_code': assembly_code}
        # Capture user id and email when the request is authenticated
        if current_user and hasattr(current_user, 'is_authenticated') and current_user.is_authenticated:
            try:
                meta_dict['user_id'] = current_user.id
                meta_dict['user_email'] = current_user.email
            except Exception:
                pass
        # Import models locally to avoid circular imports and only when needed
        from ...models import Structure, ProductComponent  # type: ignore
        # Determine the underlying structure referenced by the component's DataMatrix
        struct = None
        component_name = None
        try:
            # The component_code variable holds the full DataMatrix.  Extract the P= segment.
            for seg in (component_code or '').split('|'):
                if seg.startswith('P='):
                    component_name = seg.split('=', 1)[1]
                    break
        except Exception:
            component_name = None
        if component_name:
            try:
                struct = Structure.query.filter_by(name=component_name).first()
            except Exception:
                struct = None
        # Fallback: if the structure cannot be found via name, use the stock item's product root component
        if not struct:
            try:
                prod_tmp = getattr(item, 'product', None)
                if prod_tmp:
                    root_comp_tmp = (
                        ProductComponent.query
                        .filter_by(product_id=prod_tmp.id)
                        .order_by(ProductComponent.id.asc())
                        .first()
                    )
                    if root_comp_tmp:
                        struct = Structure.query.get(root_comp_tmp.structure_id)
            except Exception:
                struct = None
        if struct:
            try:
                meta_dict['structure_name'] = getattr(struct, 'name', '') or ''
            except Exception:
                meta_dict['structure_name'] = ''
            try:
                meta_dict['structure_description'] = getattr(struct, 'description', '') or ''
            except Exception:
                meta_dict['structure_description'] = ''
            # Persist both the human‑readable revision label and the numeric index
            try:
                rev_lbl = struct.revision_label
            except Exception:
                rev_lbl = ''
            if rev_lbl:
                meta_dict['revision_label'] = rev_lbl
            try:
                rev_idx = getattr(struct, 'revision', None)
                if rev_idx is not None:
                    meta_dict['revision_index'] = int(rev_idx)
            except Exception:
                pass
        # Serialise the metadata to JSON.  Fallback to an empty string on failure.
        try:
            meta = json.dumps(meta_dict)
        except Exception:
            meta = json.dumps({'assembly_code': assembly_code})
    except Exception:
        # In case of any unexpected error, at least record the assembly code
        try:
            meta = json.dumps({'assembly_code': assembly_code})
        except Exception:
            meta = ''
    ev = ScanEvent(datamatrix_code=component_code, action='ASSOCIA', meta=meta)
    db.session.add(ev)
    db.session.commit()
    return jsonify({'status': 'ok'})


@api_bp.route('/reservations', methods=['POST'])
def create_reservation() -> Any:
    """Create a new reservation and associated production box(es).

    This endpoint accepts a JSON payload containing ``productId`` (int),
    ``quantity`` (int) and an optional ``note``.  It returns a JSON
    object with the reservation id, a primary production box id (for
    backward compatibility), and a list of stock items with their
    DataMatrix codes.  When the requested box type is ``ASSIEME`` and
    the quantity is greater than one, a separate production box is
    created for each unit so that each assembly is loaded into its own
    container.  Otherwise a single box holds all requested items as
    before.  Invalid inputs return a 400 error and missing products
    return a 404 error.
    """
    data = request.get_json(silent=True) or {}
    product_id = data.get('productId')
    quantity = data.get('quantity', 1)
    note = data.get('note', '')

    # Validate product id and quantity
    try:
        product_id = int(product_id)
        quantity = int(quantity)
        if quantity <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({'error': 'Invalid productId or quantity'}), 400

    product = Product.query.get(product_id)
    if not product:
        return jsonify({'error': 'Product not found'}), 404

    # Create reservation
    reservation = Reservation(product=product, qty=quantity, note=note, status='APERTO')
    db.session.add(reservation)
    db.session.flush()  # assign id

    # Determine default box type and component code from the product's
    # root structure when no override is supplied.  Use the first
    # ProductComponent associated with the product to infer whether
    # assemblies, parts or commercial items are being reserved.  The
    # component name is used in the DataMatrix codes when available.
    from ...models import ProductComponent, Structure
    override_box_type = data.get('boxType')
    override_component_code = data.get('componentCode')
    root_comp = (
        ProductComponent.query
        .filter_by(product_id=product.id)
        .order_by(ProductComponent.id.asc())
        .first()
    )
    default_box_type = 'PARTE'
    default_component_code = None
    if root_comp:
        try:
            struct: Structure = Structure.query.get(root_comp.structure_id)
        except Exception:
            struct = None
        if struct:
            default_component_code = struct.name
            if getattr(struct, 'flag_assembly', False):
                default_box_type = 'ASSIEME'
            elif getattr(struct, 'flag_commercial', False):
                default_box_type = 'COMMERCIALE'
            else:
                default_box_type = 'PARTE'

    # ------------------------------------------------------------------
    # Determine whether batch (lot) management is enabled.  When enabled,
    # all stock items created within the same production box should share
    # the identical DataMatrix code.  The flag may be specified on the
    # structure or its component master via a JSON object stored in the
    # ``notes`` field.  Additionally, when the client provides a
    # ``componentCode`` override, the lot management flag should be
    # derived from that specific component rather than from the first
    # component of the product.  Fallback to the root structure when
    # no override is supplied.  Default to False when parsing fails or
    # the flag is absent.
    # Determine whether batch (lot) management is enabled.  When enabled,
    # all stock items created within the same production box should share
    # the identical DataMatrix code.  To detect this flag we attempt to
    # parse any JSON stored in the notes fields of the relevant structure
    # and its component master.  If parsing fails (e.g. notes contain
    # plain text), fall back to a simple substring search for
    # "lot_management" to support legacy data where the flag may have
    # been stored without proper JSON encoding.
    lot_management_enabled = False
    try:
        import json as _json
        # Identify the structure to inspect for the lot flag.  Prefer the
        # override component code when provided; otherwise use the root
        # structure inferred from the product's first component.
        structure_to_check = None
        comp_name_candidate = None
        if isinstance(override_component_code, str) and override_component_code.strip():
            comp_name_candidate = override_component_code.strip()
        elif default_component_code:
            comp_name_candidate = default_component_code
        if comp_name_candidate:
            try:
                structure_to_check = Structure.query.filter_by(name=comp_name_candidate).first()
            except Exception:
                structure_to_check = None
        # Fallback to the root component's structure when no override exists
        if not structure_to_check and root_comp:
            try:
                structure_to_check = Structure.query.get(root_comp.structure_id)
            except Exception:
                structure_to_check = None
        if structure_to_check:
            candidates: list[str] = []
            try:
                cm = structure_to_check.component_master
            except Exception:
                cm = None
            if cm and getattr(cm, 'notes', None):
                candidates.append(cm.notes)
            if getattr(structure_to_check, 'notes', None):
                candidates.append(structure_to_check.notes)
            # First attempt strict JSON parsing
            for candidate in candidates:
                if not candidate:
                    continue
                try:
                    parsed = _json.loads(candidate)
                    if isinstance(parsed, dict) and 'lot_management' in parsed:
                        lot_management_enabled = bool(parsed.get('lot_management'))
                        if lot_management_enabled:
                            break
                except Exception:
                    # Ignore parsing errors and try substring fallback below
                    pass
            # Fallback: scan plain text for the "lot_management" flag
            if not lot_management_enabled:
                for candidate in candidates:
                    if not candidate or not isinstance(candidate, str):
                        continue
                    # perform a case-insensitive search for the key
                    if 'lot_management' in candidate.lower():
                        lot_management_enabled = True
                        break
    except Exception:
        lot_management_enabled = False

    # Apply overrides when provided and valid.  Normalise box type to
    # uppercase and accept only recognised values.
    box_type = default_box_type
    component_code_override = None
    if isinstance(override_box_type, str):
        ov = override_box_type.strip().upper()
        # Allow overriding the box type for finished products as well.  In addition
        # to the existing types (PARTE, ASSIEME, COMMERCIALE) accept PRODOTTO as
        # a valid option.  When PRODOTTO is supplied the reservation will
        # create a production box intended for building a finished product via
        # the guided product build workflow.  The DataMatrix codes will use
        # ``T=PRODOTTO`` to clearly distinguish these boxes from assembly
        # containers.
        if ov in ('PARTE', 'ASSIEME', 'COMMERCIALE', 'PRODOTTO'):
            box_type = ov
    if isinstance(override_component_code, str) and override_component_code.strip():
        component_code_override = override_component_code.strip()

    # Determine the component code used in DataMatrix generation.  Use
    # the override when supplied, otherwise fall back to the default
    # component name derived from the product's root structure.
    component_code = component_code_override if component_code_override else default_component_code

    # Prepare list to collect info about created stock items.  Each
    # entry contains the stock item id, the generated DataMatrix and
    # the id of the production box that contains the item.  Adding
    # ``boxId`` allows clients to associate individual items with
    # their boxes when multiple boxes are created.
    items_data: list[dict[str, Any]] = []

    # If the box type is ASSIEME and quantity > 1, create one box per
    # assembly.  Otherwise create a single box and add all items to
    # that box.
    created_boxes: list[int] = []
    if box_type == 'ASSIEME' and quantity > 1:
        # For each assembly, create a separate production box and a
        # single stock item.  All stock items belong to the same
        # reservation.  The reservation qty reflects the total
        # assemblies requested.
        for _ in range(quantity):
            box_code = _generate_box_code()
            box = ProductionBox(code=box_code, box_type=box_type, status='APERTO')
            db.session.add(box)
            db.session.flush()
            created_boxes.append(box.id)
            # Create one stock item for this box
            import uuid
            temp_code = f"TMP-{uuid.uuid4().hex}"
            stock_item = StockItem(
                product=product,
                datamatrix_code=temp_code,
                status='IN_PRODUZIONE',
                reservation=reservation,
                production_box=box
            )
            db.session.add(stock_item)
            db.session.flush()
            dm_code = _generate_datamatrix(stock_item.id, product, type_label=box_type, component_code=component_code)
            # Assemblies always generate unique DataMatrix codes because each
            # assembly resides in its own box.  Lot management does not
            # apply when there is only one item per box.  Assign the
            # generated code directly.
            stock_item.datamatrix_code = dm_code
            items_data.append({'stockItemId': stock_item.id, 'datamatrix': dm_code, 'boxId': box.id})
    else:
        # Create a single production box for parts or commercial items.  When
        # lot management is enabled, all stock items in this box share
        # the same DataMatrix code.  Otherwise each item receives a unique
        # code.  Track the first generated code as the common code.
        box_code = _generate_box_code()
        box = ProductionBox(code=box_code, box_type=box_type, status='APERTO')
        db.session.add(box)
        db.session.flush()
        created_boxes.append(box.id)
        common_dm: str | None = None
        # Create ``quantity`` stock items in the same box
        for _ in range(quantity):
            import uuid
            temp_code = f"TMP-{uuid.uuid4().hex}"
            stock_item = StockItem(
                product=product,
                datamatrix_code=temp_code,
                status='IN_PRODUZIONE',
                reservation=reservation,
                production_box=box
            )
            db.session.add(stock_item)
            db.session.flush()
            # Generate a candidate DataMatrix for this item.  When lot
            # management is active we ignore subsequent codes after the first
            # and assign the common code to every item.
            dm_code = _generate_datamatrix(stock_item.id, product, type_label=box_type, component_code=component_code)
            if lot_management_enabled:
                if common_dm is None:
                    common_dm = dm_code
                # Assign the common DataMatrix to this stock item
                stock_item.datamatrix_code = common_dm
                items_data.append({'stockItemId': stock_item.id, 'datamatrix': common_dm, 'boxId': box.id})
            else:
                stock_item.datamatrix_code = dm_code
                items_data.append({'stockItemId': stock_item.id, 'datamatrix': dm_code, 'boxId': box.id})

    # Commit to persist datamatrix codes, boxes and items
    db.session.commit()

    # Maintain backward compatibility by returning a single
    # ``productionBoxId``.  Use the first created box id; when
    # multiple boxes are created this allows clients that expect a
    # single id to still function (e.g. redirecting to the first box).
    primary_box_id = created_boxes[0] if created_boxes else None
    response = {
        'reservationId': reservation.id,
        'productionBoxId': primary_box_id,
        'items': items_data
    }
    # Include list of all box ids when more than one box has been
    # created.  Front‑end code can optionally use this field to
    # display or navigate to additional boxes.  When only one box
    # exists the field is omitted to avoid breaking existing clients.
    if len(created_boxes) > 1:
        response['productionBoxIds'] = created_boxes
    return jsonify(response), 201


@api_bp.route('/production-box/<int:box_id>', methods=['GET'])
def get_production_box(box_id: int) -> Any:
    """Retrieve information about a production box.

    Returns the box object with its status and a list of stock items,
    including the number of required and uploaded documents.  This
    simplified implementation does not enforce document requirements; it
    merely reports the count of associated Document records.
    """
    box = ProductionBox.query.get(box_id)
    if not box:
        return jsonify({'error': 'Box not found'}), 404
    items = []
    for item in box.stock_items:
        docs = Document.query.filter_by(owner_type='STOCK', owner_id=item.id).all()
        required = len(docs)
        uploaded = len([d for d in docs if d.status in ('CARICATO', 'APPROVATO')])
        items.append({
            'stockItemId': item.id,
            'productCode': item.product.name,
            'datamatrix': item.datamatrix_code,
            'stato': item.status,
            'docs': {
                'required': required,
                'uploaded': uploaded
            }
        })
    return jsonify({'box': {'id': box.id, 'stato': box.status}, 'items': items})


@api_bp.route('/production-box/<int:box_id>/load', methods=['POST'])
def load_production_box(box_id: int) -> Any:
    """Complete the loading of a production box.

    Finalises the loading process for the given box.  All contained
    stock items are transitioned to the ``COMPLETATO`` state.  On‑hand
    quantities for the related products and their corresponding
    structures are incremented based on how many items of each type
    appear in the box.  A ScanEvent is recorded for each item.  When
    finished, the box status moves to ``COMPLETATO``.

    This implementation groups stock items by their underlying product
    and root structure.  Rather than incrementing the stock for each
    item individually, it accumulates increments per product/structure
    to ensure that the final stock reflects the total number of items
    loaded.  For structures, it also synchronises duplicate entries
    (matching on ``component_id`` or case‑insensitive ``name``) by
    determining the global maximum on‑hand quantity across all
    duplicates, adding the accumulated delta and writing back the
    updated quantity to every match.  This avoids inadvertently
    lowering stock when some duplicates hold larger quantities and
    ensures consistent inventory levels across legacy and migrated
    records.
    """
    box = ProductionBox.query.get(box_id)
    if not box:
        return jsonify({'error': 'Box not found'}), 404

    # Optionally allow loading a single item at a time by passing an
    # ``item_id`` query parameter.  When provided only the specified
    # stock item will be marked as completed and counted towards
    # inventory increments.  The box status transitions to
    # ``COMPLETATO`` only when all contained items have been loaded.
    item_id_str = request.args.get('item_id')
    selected_items = []
    if item_id_str:
        try:
            sel_id = int(item_id_str)
        except Exception:
            return jsonify({'error': 'Invalid item_id'}), 400
        # Find the matching stock item within this box
        match = None
        for it in box.stock_items:
            if it.id == sel_id:
                match = it
                break
        if not match:
            return jsonify({'error': 'Item not found in this box'}), 404
        selected_items = [match]
    else:
        # Load all items when no item_id is provided
        selected_items = list(box.stock_items)

    # Mark the selected stock items as completed and record scan events.
    # While iterating, accumulate increments per product and per root
    # structure so that we can update their quantities in bulk afterwards.
    from collections import defaultdict
    from ...models import Product, ProductComponent, Structure

    product_increments: defaultdict[int, int] = defaultdict(int)
    # Map structure key -> (representative Structure, count)
    structure_increments: dict[tuple[str | int, str], dict[str, Any]] = {}

    # Create a scan event for each stock item.  Even when multiple stock
    # items share the same DataMatrix (lot management), a separate event
    # is recorded for each to reflect individual quantities in the archive.
    for item in selected_items:
        # Skip if already completed to avoid double counting
        if item.status == 'COMPLETATO':
            continue
        # Mark the stock item as completed
        item.status = 'COMPLETATO'
        # Increment product counter: each stock item contributes one unit to the product
        prod = item.product
        if prod:
            product_increments[prod.id] += 1
        # Build metadata for the scan event.  Start with the box id and include
        # the user identifier and email when an authenticated operator is
        # performing the load.  Additional static information about the
        # component (name, description and revision) is added after resolving
        # the structure below.
        meta_dict: dict[str, Any] = {'box_id': box.id}
        if current_user and hasattr(current_user, 'is_authenticated') and current_user.is_authenticated:
            try:
                meta_dict['user_id'] = current_user.id
                meta_dict['user_email'] = current_user.email
            except Exception:
                # Best effort; ignore failures when retrieving user info
                pass
        # Determine the actual structure corresponding to this stock item.
        struct = None
        component_code = None
        dm = item.datamatrix_code or ''
        try:
            parts = dm.split('|')
            for part_str in parts:
                if part_str.startswith('P='):
                    component_code = part_str.split('=', 1)[1]
                    break
        except Exception:
            component_code = None
        if component_code:
            try:
                struct = Structure.query.filter_by(name=component_code).first()
            except Exception:
                struct = None
        # Fallback: use the product's first component when no code or lookup fails
        if not struct and prod:
            try:
                root_comp = (
                    ProductComponent.query
                    .filter_by(product_id=prod.id)
                    .order_by(ProductComponent.id.asc())
                    .first()
                )
                if root_comp:
                    struct = Structure.query.get(root_comp.structure_id)
            except Exception:
                struct = None
        # Populate static component details in the meta dictionary when a structure is found.
        if struct:
            try:
                meta_dict['structure_name'] = getattr(struct, 'name', '') or ''
            except Exception:
                meta_dict['structure_name'] = ''
            try:
                meta_dict['structure_description'] = getattr(struct, 'description', '') or ''
            except Exception:
                meta_dict['structure_description'] = ''
            # Record the human-readable revision label (e.g. "Rev.A") when defined.
            try:
                rev_lbl = struct.revision_label
            except Exception:
                rev_lbl = ''
            if rev_lbl:
                meta_dict['revision_label'] = rev_lbl
            # Also persist the numeric revision index when available.
            try:
                rev_idx = getattr(struct, 'revision', None)
                if rev_idx is not None:
                    meta_dict['revision_index'] = int(rev_idx)
            except Exception:
                pass
        # Serialise the meta dictionary to JSON
        try:
            meta_json = json.dumps(meta_dict)
        except Exception:
            # Fall back to a minimal metadata payload containing only the box id
            meta_json = json.dumps({'box_id': box.id})
        # Create and persist a scan event for this stock item.  Duplicate
        # events with the same DataMatrix code are allowed; this ensures
        # that each unit appears as a separate row in the archive while
        # still sharing the same documents when lot management is enabled.
        event = ScanEvent(
            datamatrix_code=(item.datamatrix_code or ''),
            action='CARICA',
            meta=meta_json
        )
        db.session.add(event)
        if struct:
            # Determine a key for grouping duplicate structures.  Prefer the
            # component_id when available, otherwise fall back to the
            # lower‑cased structure name.  If both are absent the
            # structure id is used as a unique key.
            if struct.component_id:
                key = ('id', struct.component_id)
            elif struct.name:
                key = ('name', struct.name.strip().lower())
            else:
                key = ('unique', str(struct.id))
            entry = structure_increments.get(key)
            if entry:
                entry['count'] += 1
            else:
                structure_increments[key] = {'struct': struct, 'count': 1}

    # Update product-level quantities based on increments
    for prod_id, inc in product_increments.items():
        try:
            prod = Product.query.get(prod_id)
            if prod:
                current_qty = prod.quantity_in_stock or 0
                prod.quantity_in_stock = current_qty + inc
        except Exception:
            pass

    # Update structure-level quantities using the accumulated counts.
    # For each structure group determine the global maximum stock across
    # all duplicates, add the accumulated increment and apply the new
    # quantity to every matching structure and to the representative
    # structure itself.  Negative quantities are floored at zero (should
    # not occur when loading stock but retained for symmetry with
    # build_assembly logic).
    for key, entry in structure_increments.items():
        struct = entry['struct']
        inc = entry['count']
        # Build the list of all matching structures (component_id and name
        # based).  Deduplicate using a dictionary keyed by structure id.
        matches_dict: dict[int, Structure] = {}
        try:
            # Always include name-based matches when a name exists
            try:
                if struct.name:
                    for m in Structure.query.filter(Structure.name == struct.name).all():
                        matches_dict[m.id] = m
            except Exception:
                pass
            # Include component_id matches when present
            if struct.component_id:
                try:
                    for m in Structure.query.filter(Structure.component_id == struct.component_id).all():
                        matches_dict[m.id] = m
                except Exception:
                    pass
        except Exception:
            matches_dict = {}
        matches = list(matches_dict.values())
        # Determine the current global quantity as the maximum across all matches
        quantities: list[float] = []
        for m in matches:
            try:
                quantities.append(float(m.quantity_in_stock or 0))
            except Exception:
                pass
        # Fallback to the representative structure's quantity if no matches found
        if not quantities:
            try:
                quantities.append(float(struct.quantity_in_stock or 0))
            except Exception:
                quantities.append(0)
        current_qty: float = max(quantities) if quantities else 0.0
        new_qty: float = current_qty + inc
        if new_qty < 0:
            new_qty = 0.0
        # Apply the new quantity to the representative and all matches
        struct.quantity_in_stock = new_qty
        for m in matches:
            m.quantity_in_stock = new_qty

    # Determine the appropriate box status based on what has been loaded.  When
    # loading a single item (via the item_id query parameter) the box
    # remains in progress (``IN_CARICO``) until all items have been
    # completed.  When loading all items at once the box is immediately
    # marked as completed.
    if item_id_str:
        # Check if any items remain in a non-completed state
        incomplete = False
        for si in box.stock_items:
            if si.status != 'COMPLETATO':
                incomplete = True
                break
        if incomplete:
            box.status = 'IN_CARICO'
        else:
            box.status = 'COMPLETATO'
    else:
        box.status = 'COMPLETATO'
    # Persist all changes
    db.session.commit()
    return jsonify({'boxId': box.id, 'status': box.status}), 200


@api_bp.route('/products/<int:product_id>/loaded', methods=['GET'])
def get_loaded_items(product_id: int) -> Any:
    """Return all stock items that have been loaded for a product.

    Includes items whose status is either ``CARICATO`` or ``COMPLETATO``,
    reflecting both partially and fully loaded states.  Each item is
    returned with its DataMatrix code, production box identifier and
    current status.
    """
    product = Product.query.get(product_id)
    if not product:
        return jsonify({'error': 'Product not found'}), 404
    items = StockItem.query.filter(
        StockItem.product_id == product.id,
        StockItem.status.in_(['CARICATO', 'COMPLETATO'])
    ).all()
    result = []
    for item in items:
        result.append({
            'stockItemId': item.id,
            'datamatrix': item.datamatrix_code,
            'boxId': item.production_box_id,
            'status': item.status
        })
    return jsonify({'items': result})


@api_bp.route('/datamatrix/<path:code>', methods=['GET'])
def resolve_datamatrix(code: str) -> Any:
    """Resolve a DataMatrix code into its associated entities."""
    item = StockItem.query.filter_by(datamatrix_code=code).first()
    if not item:
        return jsonify({'error': 'Code not found'}), 404
    data = {
        'stockItemId': item.id,
        'productId': item.product_id,
        'productName': item.product.name,
        'reservationId': item.reservation_id,
        'productionBoxId': item.production_box_id,
        'status': item.status
    }
    return jsonify(data)


@api_bp.route('/products/<int:product_id>/archive', methods=['GET'])
def product_archive(product_id: int) -> Any:
    """Return the timeline of scan events for all stock items of a product."""
    product = Product.query.get(product_id)
    if not product:
        return jsonify({'error': 'Product not found'}), 404
    # Collect datamatrix codes for this product
    items = StockItem.query.filter_by(product_id=product.id).all()
    codes = [item.datamatrix_code for item in items]
    if not codes:
        return jsonify({'events': []})
    events = ScanEvent.query.filter(ScanEvent.datamatrix_code.in_(codes)).order_by(ScanEvent.created_at.desc()).all()
    result = []
    for ev in events:
        try:
            meta = json.loads(ev.meta) if ev.meta else {}
        except Exception:
            meta = {}
        result.append({
            'id': ev.id,
            'datamatrix_code': ev.datamatrix_code,
            'action': ev.action,
            'meta': meta,
            'timestamp': ev.created_at.isoformat() if ev.created_at else None
        })
    return jsonify({'events': result})
