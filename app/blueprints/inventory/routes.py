"""
Routes for the inventory (magazzino) module.

This blueprint implements a comprehensive warehouse dashboard.  It lists
all assemblies along with their nested components in the same order defined
in the structure hierarchy.  Quantities for assemblies are recalculated
on the fly based on the minimum quantity available among their immediate
children.  A guided procedure allows operators to build assemblies
manually by consuming parts and uploading supporting documents.
"""

import os
import shutil  # Added for copying files
import time
from typing import List, Any
import base64
import io
from threading import RLock

from flask import Blueprint, render_template, redirect, url_for, request, flash, current_app, abort, send_file
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from ...extensions import db
from ...models import Structure, Product, ProductComponent, InventoryLog, BOMLine
from ...models import StockItem, Reservation, ProductionBox, ScanEvent, Document
# Import ComponentMaster for resolving human friendly names in event-specific
# document view.  Without this import the product_event_docs_view would
# raise a NameError when attempting to query ComponentMaster.  The import
# is intentionally placed alongside other model imports so that it is
# available throughout the module.
from ...models import ComponentMaster
from ...checklist import load_checklist

# Cache for mapping upload filename prefixes to the first matching file.  Building
# this map requires scanning the ``static/uploads`` directory which can contain
# hundreds of images.  Recreating the map for every request causes noticeable
# delays when loading the warehouse and anagrafiche views.  A module level cache
# guarded by a re-entrant lock lets us reuse the mapping across requests and only
# refresh it when the directory contents change (detected via ``st_mtime``).
_upload_prefix_cache: dict[str, Any] = {
    'map': {},
    'mtime': None,
}
_upload_prefix_cache_lock = RLock()


def _get_upload_prefix_map() -> dict[str, str]:
    """Return a cached mapping of upload filename prefixes to filenames.

    The mapping associates prefixes such as ``sn_<id>_`` or ``cm_<id>_`` with the
    first file found in ``static/uploads`` matching that prefix.  The directory is
    scanned only when its modification time changes so repeated requests avoid the
    expensive ``os.listdir`` / ``os.scandir`` call.  When the directory does not
    exist an empty mapping is returned.
    """

    upload_dir = os.path.join(current_app.static_folder, 'uploads')
    try:
        dir_stat = os.stat(upload_dir)
    except OSError:
        return {}

    current_mtime = dir_stat.st_mtime
    with _upload_prefix_cache_lock:
        cached_mtime = _upload_prefix_cache.get('mtime')
        if cached_mtime == current_mtime:
            cached_map = _upload_prefix_cache.get('map', {})
            # Return the cached dictionary directly; callers treat it as read-only.
            return cached_map

        prefix_map: dict[str, str] = {}
        try:
            for entry in os.scandir(upload_dir):
                try:
                    if not entry.is_file():
                        continue
                except AttributeError:
                    # ``os.DirEntry`` on some platforms might not expose ``is_file``;
                    # fall back to ``os.path.isfile``.
                    if not os.path.isfile(entry.path):
                        continue
                parts = entry.name.split('_', 2)
                if len(parts) >= 2:
                    prefix = parts[0] + '_' + parts[1] + '_'
                    if prefix not in prefix_map:
                        prefix_map[prefix] = entry.name
        except Exception:
            prefix_map = {}

        _upload_prefix_cache['map'] = prefix_map
        _upload_prefix_cache['mtime'] = current_mtime
        return prefix_map

# -----------------------------------------------------------------------------
# Helper to generate a simple PNG representation of a DataMatrix/QR code.
#
# To meet the requirement of displaying the DataMatrix code as an image with
# a white background, this function renders the code text itself onto a
# square canvas.  It does not produce a true scannable barcode but
# presents the encoded value as a graphic, avoiding any external API calls
# or additional dependencies.  The resulting image is returned as a
# base64‑encoded string suitable for embedding directly in HTML.
try:
    from PIL import Image, ImageDraw, ImageFont  # type: ignore
except Exception:
    # Pillow may not be installed in some environments; provide a dummy
    # fallback to prevent crashes.  In such cases the calling code should
    # detect a None return and handle accordingly.
    Image = None  # type: ignore
    ImageDraw = None  # type: ignore
    ImageFont = None  # type: ignore

def _generate_dm_image(code: str) -> str:
    """
    Generate a DataMatrix image for the given code and return it as a
    base64‑encoded PNG string.  The image uses a white background and
    attempts to render a true DataMatrix barcode when the optional
    ``pylibdmtx`` library is available.  If ``pylibdmtx`` is not
    installed or any error occurs, the function falls back to drawing
    the code text itself onto a white square.  This fallback keeps the
    table cells in the archive a consistent width and prevents long
    strings from overflowing the page.

    Args:
        code: The full DataMatrix payload (e.g. ``"P=ABC|T=PART"``).

    Returns:
        A base64‑encoded PNG (without a ``data:image/png;base64,`` prefix)
        representing the DataMatrix or a simple text image when the
        barcode library is unavailable.  An empty string is returned
        when Pillow cannot be imported.
    """
    # Bail out if Pillow is missing.  Without Pillow we cannot draw
    # anything at all; the template will display the raw code text.
    if not Image or not ImageDraw:
        return ''
    # Try to import pylibdmtx on demand.  Placing the import here
    # avoids raising ImportError during module import when the optional
    # dependency is not installed.
    try:
        from pylibdmtx.pylibdmtx import encode as dmtx_encode  # type: ignore
        has_dmtx = True
    except Exception:
        has_dmtx = False
    # Attempt to render a real DataMatrix barcode when possible
    if has_dmtx:
        try:
            # Encode the payload; pylibdmtx returns a C structure with
            # width, height and raw pixel data (monochrome).  Convert to
            # a Pillow image and paste onto a slightly larger white
            # canvas so that the barcode is framed and easier to scan.
            raw = dmtx_encode(code.encode('utf-8'))
            img = Image.frombytes('L', (raw.width, raw.height), raw.pixels)
            img = img.convert('RGB')
            # Add a 4 pixel margin on each side (8 total in each dimension)
            bg = Image.new('RGB', (img.width + 8, img.height + 8), 'white')
            bg.paste(img, (4, 4))
            buffer = io.BytesIO()
            bg.save(buffer, format='PNG')
            return base64.b64encode(buffer.getvalue()).decode('ascii')
        except Exception:
            # Fall back to drawing the code as text below
            pass
    # Fallback: draw the pipe‑delimited payload onto a square canvas.
    try:
        # Break the payload into multiple lines at '|' separators.  When
        # there is no code the parts list will be empty and the final
        # image will remain blank.
        parts = code.split('|') if code else []
        # Choose a modest canvas size; this is scaled down in the
        # template to 60×60 pixels so there is no need for a large
        # resolution.  A 120×120 canvas gives crisp rendering when
        # downscaled by the browser.
        size = 120
        img = Image.new('RGB', (size, size), 'white')
        draw = ImageDraw.Draw(img)
        # Use a monospaced font when available; fall back to the
        # default system font.  ``load_default`` always succeeds.
        try:
            font = ImageFont.load_default()
        except Exception:
            font = None  # type: ignore
        # Compose the text by joining segments with newlines.  This
        # breaks long payloads into shorter lines, centred on the
        # canvas.
        text = '\n'.join(parts)
        # Compute the bounding box of the multiline text to center it.
        if font:
            try:
                bbox = draw.multiline_textbbox((0, 0), text, font=font)
            except Exception:
                # Older Pillow versions may not have multiline_textbbox;
                # use textbbox instead.
                bbox = draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
        else:
            # Roughly estimate width/height if no font metrics are
            # available.  Each character is assumed ~6 px wide and
            # 10 px tall.
            max_len = max((len(p) for p in parts), default=0)
            text_width = min(max_len * 6, size)
            text_height = len(parts) * 10
        x = (size - text_width) / 2
        y = (size - text_height) / 2
        draw.multiline_text((x, y), text, fill='black', font=font, align='center')
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        return base64.b64encode(buffer.getvalue()).decode('ascii')
    except Exception:
        # In case of any unforeseen error return an empty string.  The
        # calling code will display the payload as plain text.
        return ''


def _dedupe_docs(docs: List[tuple[str, Any]]) -> List[tuple[str, Any]]:
    """
    Remove duplicate document entries from a list while preserving order.

    Documents are deduplicated by filename only, ignoring differences in
    URLs.  This ensures that when the same file is attached both via
    the Document table (static URL) and via the production archive
    (download URL), only one entry appears.  The first occurrence is
    kept and subsequent entries with the same filename are discarded.

    Args:
        docs: A list of (filename, url) tuples.  The url may be None
            when a static URL cannot be resolved.

    Returns:
        A new list containing only the first occurrence of each unique
        filename.
    """
    seen_names: set[str] = set()
    unique: List[tuple[str, Any]] = []
    for name, url in docs:
        # Normalise the filename to compare duplicates.  Use the raw name
        # directly; case sensitivity is preserved to avoid unintended
        # merging of different files that differ only by case.
        if name in seen_names:
            continue
        seen_names.add(name)
        unique.append((name, url))
    return unique


def _assign_image_to_structure(struct: Structure) -> None:
    """Assign a dynamic image filename to a structure if one is not already defined.

    Many templates in the inventory module attempt to access
    ``structure.image_filename`` when rendering component images.  The
    Structure model itself does not define an ``image_filename`` column,
    therefore the attribute is normally absent.  However, users often
    associate images with a ProductComponent linking to the structure.
    To make these images available across the warehouse views we assign
    the first available component image to the structure as a dynamic
    attribute.  This helper walks through the structure (and its
    descendants) and attaches the image on demand.

    :param struct: The Structure instance to decorate.  Any children of
        this structure will also be processed recursively.
    """
    # If the structure already has an image_filename attribute (either
    # because one was dynamically assigned previously or via another
    # extension), skip further work.  Only assign when the attribute
    # is missing or falsy.
    current = getattr(struct, 'image_filename', None)
    if not current:
        # Look up the first ProductComponent that references this structure and
        # has a non‑null image_filename.  Some structures may be referenced by
        # multiple components across different products; the original logic
        # picked the first component regardless of whether an image was set.  If
        # that component lacked an image the view would show no picture even
        # when another component did define one.  To avoid this issue we
        # explicitly filter for components with a defined image_filename and
        # choose the first such match.  This ensures that any uploaded image
        # associated with a part, commercial item or assembly is surfaced in
        # the warehouse views.
        try:
            # Only consider components with a non‑null and non‑empty image filename.
            # Without the second check the query may return a component whose
            # ``image_filename`` is an empty string which results in a broken
            # <img> tag on the frontend.  Filtering out empty strings avoids
            # selecting such placeholders when a valid image exists on another
            # component.
            comp = (
                ProductComponent.query
                .filter_by(structure_id=struct.id)
                .filter(ProductComponent.image_filename != None)
                .filter(ProductComponent.image_filename != '')
                .first()
            )
            if comp:
                setattr(struct, 'image_filename', comp.image_filename)
            else:
                # If no component image is found, fall back to other sources
                fallback = _lookup_structure_image(struct)
                if fallback:
                    setattr(struct, 'image_filename', fallback)
        except Exception:
            # If any database error occurs, leave the attribute unset
            pass
    # Recurse into children if present.  Note: children relationship
    # returns Structure instances via backref.
    for child in getattr(struct, 'children', []):
        _assign_image_to_structure(child)


def _lookup_structure_image(struct: Structure) -> str | None:
    """Return the best available image filename for a structure.

    This helper centralises the logic for determining which image to use
    when rendering structures in the inventory views.  The priority is:

      1. Any ProductComponent referencing the structure with a non‑null
         and non‑empty ``image_filename``.
      2. A file stored in ``static/uploads`` with a prefix matching
         ``sn_<structure.id>_`` (structure‑level fallback).
      3. A file stored in ``static/uploads`` with a prefix matching
         ``cm_<component_master.id>_`` (component master fallback).
      4. A file stored in ``static/uploads`` with a prefix matching
         ``st_<structure.type_id>_`` (structure type fallback).

    The first match among these options is returned.  If no image is
    found the function returns ``None``.

    :param struct: Structure instance for which to find an image.
    :return: Filename (not full path) of the selected image or None.
    """
    # Cache lookups on flask.g within the request to avoid querying for the same
    # structure multiple times.  When no request context is active the cache is
    # simply skipped.
    struct_id = getattr(struct, 'id', None)
    cache = None
    try:
        from flask import g
        cache = getattr(g, 'structure_image_cache', None)
        if cache is None:
            cache = {}
            g.structure_image_cache = cache
        if struct_id is not None and struct_id in cache:
            return cache[struct_id]
    except Exception:
        cache = None

    # Step 1: look for a product component image
    try:
        comp = (
            ProductComponent.query
            .filter_by(structure_id=struct.id)
            .filter(ProductComponent.image_filename != None)
            .filter(ProductComponent.image_filename != '')
            .first()
        )
        if comp and comp.image_filename:
            if cache is not None and struct_id is not None:
                cache[struct_id] = comp.image_filename
            return comp.image_filename
    except Exception:
        # ignore database errors and continue with fallbacks
        pass
    # Step 2–4: search file system for fallback images.  To avoid scanning the
    # uploads directory repeatedly (which can be expensive when many images
    # exist), build a prefix map on the first call within a request.  The
    # mapping is stored on flask.g to persist for the duration of the request.
    try:
        upload_dir = os.path.join(current_app.static_folder, 'uploads')
        if not os.path.isdir(upload_dir):
            if cache is not None and struct_id is not None:
                cache[struct_id] = None
            return None
        from flask import g
        # Build prefix map if not already present on g using the cached helper.
        if not hasattr(g, 'upload_prefix_map'):
            g.upload_prefix_map = _get_upload_prefix_map()
        prefix_map = getattr(g, 'upload_prefix_map', {})
        # Structure‑level image (sn_<structure.id>_*)
        prefix_struct = f"sn_{struct.id}_"
        if prefix_struct in prefix_map:
            result = prefix_map[prefix_struct]
            if cache is not None and struct_id is not None:
                cache[struct_id] = result
            return result
        # Component master image (cm_<master.id>_*)
        master = getattr(struct, 'component_master', None)
        if master:
            prefix_master = f"cm_{master.id}_"
            if prefix_master in prefix_map:
                result = prefix_map[prefix_master]
                if cache is not None and struct_id is not None:
                    cache[struct_id] = result
                return result
        # Structure type image (st_<type.id>_*)
        stype = getattr(struct, 'type', None)
        if stype:
            prefix_type = f"st_{stype.id}_"
            if prefix_type in prefix_map:
                result = prefix_map[prefix_type]
                if cache is not None and struct_id is not None:
                    cache[struct_id] = result
                return result
    except Exception:
        pass
    if cache is not None and struct_id is not None:
        cache[struct_id] = None
    return None


inventory_bp = Blueprint('inventory', __name__, template_folder='../../templates')

# -----------------------------------------------------------------------------
# Production history cleanup
#
# To ensure that the global production history starts empty when the
# application is launched, perform a one‑time cleanup of any existing
# ProductBuild records and associated files.  Without this step the
# archive could display sample data from a previous run, even when the
# operator has not yet built any assemblies or products.  The
# ``before_app_request`` decorator guarantees that the cleanup runs
# before the first request to the application.  A flag stored on
# ``current_app`` ensures the cleanup executes exactly once per application
# lifecycle and is skipped on subsequent requests.
@inventory_bp.before_app_request
def clear_production_history() -> None:
    """Remove all ProductBuild records and purge assembly folders."""
    # Guard against multiple executions: run only once per app lifecycle
    from flask import current_app as _cur_app  # alias to avoid confusion in nested scopes
    if getattr(_cur_app, '_cleared_production_history', False):
        return
    # Mark as executed so that further requests skip this cleanup
    _cur_app._cleared_production_history = True
    # Remove all rows from ProductBuildItem and ProductBuild.  Wrap in try/except
    # to avoid failing when the tables do not exist (e.g. during initial setup).
    try:
        from ..models import ProductBuild, ProductBuildItem
        # Delete ProductBuildItem entries before ProductBuild to satisfy
        # foreign key constraints.
        ProductBuildItem.query.delete()
        ProductBuild.query.delete()
        from ..extensions import db
        db.session.commit()
    except Exception:
        # Roll back any partial transaction on error
        try:
            from ..extensions import db
            db.session.rollback()
        except Exception:
            pass
    # Purge completed assembly directories under Produzione/Assiemi_completati.
    # This directory contains nested documentation for assemblies that have
    # already been built.  Removing its contents prevents stale data from
    # appearing in the archive when no new builds have been performed.
    try:
        root_path = _cur_app.root_path
        assiemi_path = os.path.join(root_path, 'Produzione', 'Assiemi_completati')
        # Use the correct variable name when checking existence and iterating
        if os.path.isdir(assiemi_path):
            for entry in os.listdir(assiemi_path):
                full_path = os.path.join(assiemi_path, entry)
                if os.path.isdir(full_path):
                    shutil.rmtree(full_path, ignore_errors=True)
    except Exception:
        # Ignore any filesystem errors during cleanup
        pass

@inventory_bp.app_context_processor
def inject_inventory_logs():
    """Provide a list of recent inventory logs to all templates within this blueprint.

    The logs are ordered from most recent to oldest and limited to the latest 10
    entries to avoid overwhelming the UI.  Templates can access this variable
    via ``inventory_logs``.  If the query fails for any reason an empty list
    is returned.
    """
    try:
        logs = (
            InventoryLog.query
            .order_by(InventoryLog.created_at.desc())
            .limit(10)
            .all()
        )
    except Exception:
        logs = []
    return dict(inventory_logs=logs)


def _recalculate_assemblies() -> None:
    """Previously updated assembly stock quantities based on children.

    The warehouse module now treats assembly stock as representing
    physically built units only.  Assemblies no longer automatically
    derive their ``quantity_in_stock`` from the quantities of their
    children.  Instead, the number of buildable assemblies is computed
    dynamically when rendering views.  This helper intentionally does
    nothing but is retained for backwards compatibility with older
    calls.  Invoking it will not alter any database state.
    """
    # No action needed: assembly stock is no longer recalculated here.
    return


def _assign_complete_qty_recursive(structure: Structure) -> None:
    """Assign a complete_qty attribute to an assembly and its descendants.

    The complete quantity represents how many full units of the given
    structure can be built from available stock.  For assemblies it is
    computed by taking the minimum on‑hand quantity among all
    immediate children (assuming one of each child is required) using
    `_calculate_assembly_stock` with an empty component map.  For
    non‑assemblies ``complete_qty`` equals the on‑hand stock.  The
    function recurses into child nodes to assign the attribute to
    descendants as well.

    :param structure: The structure node to decorate.  Must be loaded with
      its children relationship.
    """
    try:
        if structure.flag_assembly:
            # Compute how many assemblies can be built given current stock of children.
            qty = _calculate_assembly_stock(structure, {})
        else:
            # For parts and commercial components, complete_qty equals on‑hand stock.
            qty = int(structure.quantity_in_stock or 0)
        setattr(structure, 'complete_qty', qty)
    except Exception:
        # In case of errors, default to zero buildable quantity.
        setattr(structure, 'complete_qty', 0)
    # Recurse through children to assign complete_qty attributes
    for child in getattr(structure, 'children', []):
        _assign_complete_qty_recursive(child)

# -----------------------------------------------------------------------------
# Reserved assemblies calculation
#
# When operators reserve assemblies via the production module (creating
# production boxes of type 'ASSIEME'), the quantities of those assemblies
# should no longer be available for additional reservations from the
# warehouse interface.  To prevent duplicate reservations, we compute
# a mapping of assembly structure IDs to the number of units reserved in
# open or in-progress production boxes.  A reserved unit corresponds to
# one stock item in a production box whose status is not 'COMPLETATO'.

def _calculate_reserved_assemblies() -> dict[int, int]:
    """Return a mapping from assembly Structure.id to the number of units
    currently reserved in production boxes.  Only boxes of type 'ASSIEME'
    with status 'APERTO' or 'IN_CARICO' are considered.  Each stock item
    in such a box counts as one reserved unit for the assembly identified
    by the component code extracted from its DataMatrix.

    :return: dict mapping Structure.id to reserved quantity
    """
    from ...models import ProductionBox, Structure
    reserved_counts: dict[int, int] = {}
    try:
        # Select boxes where assemblies are reserved but not yet completed
        boxes = ProductionBox.query.filter(
            ProductionBox.status.in_(['APERTO', 'IN_CARICO']),
            ProductionBox.box_type == 'ASSIEME'
        ).all()
    except Exception:
        boxes = []
    for box in boxes:
        # Skip boxes without items
        if not box.stock_items:
            continue
        # Determine the component code (structure name) from the first stock item
        dm = box.stock_items[0].datamatrix_code or ''
        component_code = None
        try:
            parts = dm.split('|')
            for part in parts:
                if part.startswith('P='):
                    component_code = part.split('=', 1)[1]
                    break
        except Exception:
            component_code = None
        if not component_code:
            continue
        # Look up the corresponding Structure by name
        try:
            struct = Structure.query.filter_by(name=component_code).first()
        except Exception:
            struct = None
        if struct:
            reserved_counts[struct.id] = reserved_counts.get(struct.id, 0) + len(box.stock_items)
    return reserved_counts


def _assign_available_qty_recursive(structure: Structure, reserved_counts: dict[int, int]) -> None:
    """Decorate a Structure and its descendants with an available_qty attribute.

    The available quantity for an assembly equals its complete_qty
    (number of buildable units) minus the number of units currently
    reserved in open or in-progress production boxes.  For non-assemblies
    the available quantity equals the on-hand stock.  Negative values are
    floored at zero.

    :param structure: Structure node to decorate.  Must already have
        complete_qty assigned for assemblies.
    :param reserved_counts: Mapping from Structure.id to reserved units.
    """
    try:
        if structure.flag_assembly:
            # For assemblies, subtract reserved units from the complete quantity.
            complete_qty = getattr(structure, 'complete_qty', 0)
            reserved = reserved_counts.get(structure.id, 0)
            try:
                avail = int(complete_qty) - int(reserved)
            except Exception:
                avail = complete_qty
            if avail < 0:
                avail = 0
            setattr(structure, 'available_qty', avail)
        else:
            # Assign None to indicate no reservation effect for non-assembly components.
            # This prevents Jinja from treating an Undefined value as truthy when
            # comparing against ``None``.  Templates can detect None and
            # gracefully fall back to complete_qty or quantity_in_stock.
            try:
                setattr(structure, 'available_qty', None)
            except Exception:
                # If unable to set the attribute, ignore.
                pass
    except Exception:
        # On error, remove available_qty if present to avoid misleading values
        try:
            if hasattr(structure, 'available_qty'):
                delattr(structure, 'available_qty')
        except Exception:
            pass
    # Recurse through children
    for child in getattr(structure, 'children', []):
        _assign_available_qty_recursive(child, reserved_counts)


def _collect_root_assemblies() -> List[Structure]:
    """Return a list of all assembly structures sorted by insertion order.

    Assemblies may be nested within other assemblies; for the purpose of
    the inventory dashboard we list all assemblies at the top level.  The
    recursive template will render children appropriately.  Sorting by
    primary key preserves the original creation order defined by
    administrators.
    """
    return Structure.query.filter_by(flag_assembly=True).order_by(Structure.id.asc()).all()


# -----------------------------------------------------------------------------
# Helper to compute the maximum number of complete products available.
#
# A product is composed of one or more structure nodes via the
# ProductComponent association.  Each component defines how many units of
# the corresponding structure are required to build a single product.  To
# determine how many complete products can be produced from the current
# inventory, we compute the ratio of on‑hand quantity to the required
# quantity for each component and take the minimum across all components.
def _calculate_product_stock(product: Product) -> int:
    """Return the maximum number of complete products that can be built from stock.

    This helper considers only the immediate structures associated with a
    product (i.e. those structures that are not descendants of other
    structures on the same product).  For each root component we divide
    the available quantity of its underlying structure (``quantity_in_stock``)
    by the quantity required (``ProductComponent.quantity``).  The
    smallest of these ratios determines how many complete units of the
    product can be assembled.  Nested components (children of an
    assembly) are ignored in this calculation because their availability
    is accounted for when the assembly itself is built.  If the product
    has no defined components the function returns 0.

    :param product: The Product instance to evaluate.
    :return: The integer number of complete products that can be built.
    """
    # Fetch all component associations for this product
    comps: list[ProductComponent] = []
    try:
        comps = ProductComponent.query.filter_by(product_id=product.id).all()
    except Exception:
        comps = []
    if not comps:
        return 0
    # Build a mapping of structure_id to the component association
    comp_map: dict[int, ProductComponent] = {comp.structure_id: comp for comp in comps}
    ratios: list[float] = []
    for comp in comps:
        struct = comp.structure
        # Skip non-root structures.  A structure is considered a root if it has
        # no parent or its parent structure is not part of this product's
        # component map.  This ensures that only top-level assemblies or parts
        # constrain the finished product count.
        try:
            parent_id = struct.parent_id
        except Exception:
            parent_id = None
        if parent_id is not None and parent_id in comp_map:
            # Nested component; do not include it in the product stock calculation
            continue
        # Determine how many units of this structure are on hand
        try:
            on_hand = struct.quantity_in_stock or 0
        except Exception:
            on_hand = 0
        # Required quantity for this component defaults to 1 when undefined
        try:
            required_qty = comp.quantity or 1
        except Exception:
            required_qty = 1
        if required_qty <= 0:
            # Skip invalid or zero quantities
            continue
        try:
            ratio = (on_hand / required_qty) if required_qty else 0
        except Exception:
            ratio = 0
        ratios.append(ratio)
    if not ratios:
        return 0
    try:
        # The number of complete products equals the smallest integer ratio
        return int(min(ratios))
    except Exception:
        return 0

# Helper to compute how many complete units of an assembly can be built
# from current stock, based on the quantities required for each child
# component in the product BOM.  The ``comp_map`` argument maps
# structure IDs to their corresponding ProductComponent (if any) and
# provides the required quantity (defaults to 1 when missing).
def _calculate_assembly_stock(structure: Structure, comp_map: dict[int, ProductComponent]) -> int:
    """Return the number of complete assemblies that can be built for a given
    structure based on its children and the component quantities defined in
    the product BOM.

    For each immediate child of the given structure we determine the
    quantity of stock on hand (child.quantity_in_stock) and the quantity
    required to build one unit of the assembly (taken from the
    corresponding ProductComponent.quantity when available, otherwise
    defaulting to 1).  The ratio of on‑hand quantity to required quantity
    is computed for each child; the smallest ratio (rounded down) defines
    how many complete assemblies can be built.  If the structure has no
    children the function falls back to its own quantity_in_stock.

    :param structure: The assembly Structure for which to compute complete quantity.
    :param comp_map:  A mapping from structure_id to ProductComponent used
        in the current product BOM.  Components not present in the map
        default to a required quantity of 1.
    :return: The integer number of complete assemblies that can be built.
    """
    children = structure.children
    if not children:
        return int(structure.quantity_in_stock or 0)
    ratios: list[float] = []
    for child in children:
        on_hand = child.quantity_in_stock or 0
        child_comp = comp_map.get(child.id)
        required = getattr(child_comp, 'quantity', None)
        if required is None or required <= 0:
            required = 1
        # Avoid division by zero
        if required == 0:
            continue
        ratios.append(on_hand / required)
    if not ratios:
        # No ratios computed -> treat as zero
        return 0
    return int(min(ratios))


@inventory_bp.route('/')
@login_required
def index():
    """Display the product overview for the warehouse.

    When users access the inventory module they are first presented with
    an overview of all products available in the system.  Each product is
    represented by a rectangular card showing its image (when defined),
    name and the number of complete units that can be assembled from
    current stock.  Clicking a card navigates to the detailed component
    table for that product.  Assembly quantities are recalculated prior
    to computing product stock to ensure up‑to‑date values.
    """
    _recalculate_assemblies()
    products = Product.query.order_by(Product.name.asc()).all()
    product_cards: list[dict[str, object]] = []
    for product in products:
        qty_complete = _calculate_product_stock(product)
        product_cards.append({
            'product': product,
            'quantity_complete': qty_complete
        })
    return render_template('inventory/home.html', product_cards=product_cards,
                          active_tab='products')


@inventory_bp.route('/products')
@login_required
def list_products():
    """Display all products with their component explosion.

    This view presents a list of products in the warehouse along with a
    hierarchical explosion of the assemblies, parts and commercial items
    that make up each product.  The structure of each product is
    constructed using the same logic as the product detail page: for a
    given product, the function determines which structure nodes are
    attached to the product via ProductComponent, identifies root
    structures whose parents are either absent or not part of the
    product, and recursively builds a numbered tree.  Each product
    therefore appears as a top-level row with its name, optional
    picture and description, and can be expanded to reveal the nested
    components.  Quantities shown in the explosion are taken from the
    underlying Structure records; assemblies retain their on‑hand
    quantity calculated via ``_recalculate_assemblies``.
    """
    _recalculate_assemblies()
    # Fetch all products ordered alphabetically by name so that users
    # can quickly locate a product.
    products = Product.query.order_by(Product.name.asc()).all()
    product_trees: list[dict[str, object]] = []
    for product in products:
        # Load all components associated with this product.  Each
        # ProductComponent references a Structure via structure_id.
        components = ProductComponent.query.filter_by(product_id=product.id).all()
        # Build a lookup table mapping structure_id to the ProductComponent.
        comp_map = {comp.structure_id: comp for comp in components}
        # Identify root structures: those whose parent is either None or
        # not associated with this product.  These form the first level
        # of the explosion tree.
        root_pairs: list[tuple[Structure, ProductComponent]] = []
        for comp in components:
            struct = comp.structure
            if struct.parent_id is None or struct.parent_id not in comp_map:
                root_pairs.append((struct, comp))

        # Ensure each root structure (and its children) has an image_filename
        # attribute assigned from the first ProductComponent referencing it.
        for struct, _ in root_pairs:
            _assign_image_to_structure(struct)
        # Recursively build a flat list of row dictionaries representing
        # the hierarchy.  Each row includes a numbering string to
        # indicate its position, references to the structure and
        # component, the numbering of its parent row and the depth
        # within the hierarchy.  Children are temporarily stored in a
        # flat list and later regrouped into a nested tree.
        def build_rows(pairs: list[tuple[Structure, ProductComponent | None]], parent_num: str = '') -> list[dict[str, object]]:
            rows: list[dict[str, object]] = []
            for idx, (struct, comp) in enumerate(pairs, start=1):
                number = f"{idx}" if not parent_num else f"{parent_num}.{idx}"
                # Gather child pairs for this structure: all children of
                # the Structure with their corresponding ProductComponent
                # (if the child appears in this product).  Children not
                # associated with the product still appear with
                # component set to None so that the explosion shows the
                # complete structure definition.
                children_pairs: list[tuple[Structure, ProductComponent | None]] = []
                for child in struct.children:
                    child_comp = comp_map.get(child.id)
                    # Append the tuple; child_comp may be None for
                    # unbound structures (e.g. parts defined in the
                    # structure hierarchy but not used by this product).
                    children_pairs.append((child, child_comp))
                has_children = len(children_pairs) > 0
                rows.append({
                    'number': number,
                    'structure': struct,
                    'component': comp,
                    'parent_number': parent_num,
                    'has_children': has_children,
                    'depth': number.count('.'),
                    # Children will be assigned after flattening
                    'children': []
                })
                if has_children:
                    rows.extend(build_rows(children_pairs, number))
            return rows
        structure_rows = build_rows(root_pairs)
        # Populate the complete quantity for each row.  For assemblies
        # the quantity is computed based on the ratio of child stock to
        # required quantities; for parts and commercial items it equals
        # the on‑hand stock of the structure.
        for row in structure_rows:
            struct = row['structure']
            if struct.flag_assembly:
                row['complete_qty'] = _calculate_assembly_stock(struct, comp_map)
            else:
                row['complete_qty'] = int(struct.quantity_in_stock or 0)

        # Convert the flat list of rows into a nested tree structure by
        # mapping parent numbers to their children.  This mirrors the
        # approach used in the product detail blueprint.  Nodes with no
        # parent_number are considered roots of the tree.
        number_map = {row['number']: row for row in structure_rows}
        for row in structure_rows:
            row['children'] = []
        for row in structure_rows:
            parent_num = row['parent_number']
            if parent_num:
                parent = number_map.get(parent_num)
                if parent:
                    parent['children'].append(row)
        rows_tree = [row for row in structure_rows if not row['parent_number']]
        # Compute how many complete units of this product can be built based on
        # the on‑hand quantities of its component structures.  Instead of
        # consulting the BOMLine table (which expresses product‑to‑product
        # relationships) we leverage the helper that calculates the
        # buildable product quantity from ProductComponent entries.  This
        # ensures that the quantity shown in the UI reflects the actual
        # stock levels of the underlying structures.  When a product has
        # no components or an error occurs the value defaults to zero.
        try:
            buildable_qty = _calculate_product_stock(product)
        except Exception:
            buildable_qty = 0
        product_trees.append({'product': product, 'rows_tree': rows_tree, 'buildable_qty': buildable_qty})
    return render_template('inventory/products.html', product_trees=product_trees, active_tab='products')


# -----------------------------------------------------------------------------
# Assemblies view moved from the root index.  This view presents the same
# recursive tree of assemblies and components that was previously shown on
# the default landing page.  Operators can access it via the "Assiemi"
# tab in the warehouse navigation bar.
@inventory_bp.route('/assemblies')
@login_required
def list_assemblies():
    """Display the assembly tree for the warehouse.

    Assemblies and their children are recalculated and rendered via the
    original index template.  Completed and incomplete counts are
    computed and passed to the template for display.
    """
    # Compute the list of root assemblies without automatically modifying their
    # quantity_in_stock.  Assembly stock now represents physically
    # built units only.  For each root and its descendants we assign
    # complete_qty (number of assemblies that can be built) and attach
    # images.
    assemblies = _collect_root_assemblies()
    for assembly in assemblies:
        # Assign a display image to this structure and its children
        _assign_image_to_structure(assembly)
        # Assign complete_qty recursively for build readiness
        _assign_complete_qty_recursive(assembly)
    # Compute reserved assembly quantities and assign available_qty recursively
    try:
        reserved_counts = _calculate_reserved_assemblies()
    except Exception:
        reserved_counts = {}
    for assembly in assemblies:
        _assign_available_qty_recursive(assembly, reserved_counts)
    # Compute summary counts.
    #
    # "Assiemi completati" should reflect how many complete sets of assemblies
    # exist in stock.  A single set requires that every top‑level assembly
    # has at least one physically built unit (quantity_in_stock >= 1).  When
    # all assemblies have two units on hand the completed counter increases
    # to 2, and so on.  Therefore the number of completed sets equals the
    # minimum stock among all root assemblies.  When no assemblies are
    # defined the value defaults to zero.
    if assemblies:
        # quantity_in_stock may be None for newly created assemblies; treat
        # missing values as zero.  Cast to int to avoid float comparisons.
        completed = min(int(a.quantity_in_stock or 0) for a in assemblies)
    else:
        completed = 0
    # "Assiemi da completare" represents how many root assemblies still
    # need to be built at least once.  It counts the number of assemblies
    # whose on‑hand quantity is zero.  Assemblies with one or more units
    # already assembled are not considered incomplete in this context.
    incomplete = sum(1 for a in assemblies if int(a.quantity_in_stock or 0) == 0)
    return render_template(
        'inventory/assemblies.html',
        assemblies=assemblies,
        completed=completed,
        incomplete=incomplete,
        active_tab='assemblies'
    )


# -----------------------------------------------------------------------------
# Product detail view showing the exploded bill of materials for a single
# product.  This route is invoked when clicking on a product card in the
# inventory overview.  It reconstructs the hierarchical list of structures
# associated with the product and renders a collapsible table with a
# built‑in search bar for filtering components.
@inventory_bp.route('/product/<int:product_id>')
@login_required
def product_detail(product_id: int):
    """Render the component table for a single product.

    The component hierarchy is built using the same logic as in
    `list_products` but restricted to a single product.  Assembly
    quantities are refreshed before computing the tree.  The resulting
    nested list of rows (rows_tree) is passed to the template along
    with the product instance.

    :param product_id: Primary key of the product to display.
    """
    _recalculate_assemblies()
    product = Product.query.get_or_404(product_id)
    components = ProductComponent.query.filter_by(product_id=product.id).all()
    comp_map = {comp.structure_id: comp for comp in components}
    root_pairs: list[tuple[Structure, ProductComponent]] = []
    for comp in components:
        struct = comp.structure
        if struct.parent_id is None or struct.parent_id not in comp_map:
            root_pairs.append((struct, comp))

    # Assign image filenames to each root structure and its descendants
    for struct, _ in root_pairs:
        _assign_image_to_structure(struct)
    def build_rows(pairs: list[tuple[Structure, ProductComponent | None]], parent_num: str = '') -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for idx, (struct, comp) in enumerate(pairs, start=1):
            number = f"{idx}" if not parent_num else f"{parent_num}.{idx}"
            children_pairs: list[tuple[Structure, ProductComponent | None]] = []
            for child in struct.children:
                child_comp = comp_map.get(child.id)
                children_pairs.append((child, child_comp))
            has_children = len(children_pairs) > 0
            rows.append({
                'number': number,
                'structure': struct,
                'component': comp,
                'parent_number': parent_num,
                'has_children': has_children,
                'depth': number.count('.'),
                'children': []
            })
            if has_children:
                rows.extend(build_rows(children_pairs, number))
        return rows
    structure_rows = build_rows(root_pairs)
    number_map = {row['number']: row for row in structure_rows}
    for row in structure_rows:
        # Compute complete quantity: for assemblies derive from children ratios;
        # for non-assemblies use the structure's on-hand stock.
        struct = row['structure']
        if struct.flag_assembly:
            row['complete_qty'] = _calculate_assembly_stock(struct, comp_map)
        else:
            row['complete_qty'] = int(struct.quantity_in_stock or 0)
        row['children'] = []

    # Compute reserved counts for assemblies and assign available quantities
    try:
        reserved_counts_pd = _calculate_reserved_assemblies()
    except Exception:
        reserved_counts_pd = {}
    for row in structure_rows:
        struct = row['structure']
        if struct.flag_assembly:
            complete_qty = row.get('complete_qty', 0)
            reserved = reserved_counts_pd.get(struct.id, 0)
            try:
                avail = int(complete_qty) - int(reserved)
            except Exception:
                avail = complete_qty
            if avail < 0:
                avail = 0
            row['available_qty'] = avail
        else:
            # For non-assembly rows do not define available_qty (set to None) to
            # allow templates to fall back on complete_qty.  This avoids
            # inadvertently using the on-hand stock when reservations do not apply.
            row['available_qty'] = None
    for row in structure_rows:
        parent_num = row['parent_number']
        if parent_num:
            parent = number_map.get(parent_num)
            if parent:
                parent['children'].append(row)
    rows_tree = [row for row in structure_rows if not row['parent_number']]
    # Render the product bill of materials page.  Use the key 'product'
    # for the active_tab so that the navigation bar highlights the
    # appropriate tab on the product detail view.  Previously this
    # parameter was set to 'products' which conflicted with the global
    # products listing.  Aligning it with the local tab names avoids
    # accidental highlighting of the wrong tab.
    return render_template('inventory/product_detail.html', product=product,
                          rows_tree=rows_tree, active_tab='product')


# -----------------------------------------------------------------------------
# Per‑product specialised views
#
# Operators requested dedicated tabs within the product detail page to view
# only assemblies, only parts or only commercial components for a given
# product.  The following routes implement the logic to gather the
# appropriate structures from a product's bill of materials and render
# dedicated templates.  Each handler recalculates assembly stock on
# page load to ensure up‑to‑date quantities and assigns images and
# buildable quantities where applicable.

@inventory_bp.route('/product/<int:product_id>/assemblies')
@login_required
def product_assemblies(product_id: int):
    """Display all assemblies and sub‑assemblies for a specific product.

    The returned page lists every structure flagged as an assembly that is
    reachable from the product's component hierarchy.  Assemblies are shown
    as separate roots even if they are nested within other assemblies.  For
    each assembly, the tree of its child components (parts and commercial
    items) is displayed beneath it.  The list is sorted by primary key to
    reflect insertion order.

    :param product_id: Primary key of the product whose assemblies should be listed.
    """
    # Refresh assembly quantities for accuracy
    _recalculate_assemblies()
    product = Product.query.get_or_404(product_id)
    # Build a lookup of all component structures used directly in the product
    components = ProductComponent.query.filter_by(product_id=product.id).all()
    comp_map = {comp.structure_id: comp for comp in components}
    # Identify root structures attached to this product.  A root is a
    # structure whose parent is either None or not itself part of the product.
    root_structs: list[Structure] = []
    for comp in components:
        struct = comp.structure
        if struct.parent_id is None or struct.parent_id not in comp_map:
            root_structs.append(struct)
    # Recursively collect assemblies from the product hierarchy
    assemblies_dict: dict[int, Structure] = {}
    def collect_assemblies(struct: Structure) -> None:
        if struct.flag_assembly:
            assemblies_dict[struct.id] = struct
        for child in getattr(struct, 'children', []):
            collect_assemblies(child)
    for struct in root_structs:
        collect_assemblies(struct)
    assemblies: list[Structure] = list(assemblies_dict.values())
    assemblies.sort(key=lambda s: s.id)
    # Assign images and compute complete quantities for each assembly and its children
    for assembly in assemblies:
        _assign_image_to_structure(assembly)
        _assign_complete_qty_recursive(assembly)
    # Compute reserved assemblies and assign available quantities for assemblies and children
    try:
        reserved_counts_pa = _calculate_reserved_assemblies()
    except Exception:
        reserved_counts_pa = {}
    for assembly in assemblies:
        _assign_available_qty_recursive(assembly, reserved_counts_pa)
    return render_template(
        'inventory/product_assemblies.html',
        product=product,
        assemblies=assemblies,
        active_tab='assemblies'
    )


@inventory_bp.route('/product/<int:product_id>/parts')
@login_required
def product_parts(product_id: int):
    """Display all non‑commercial, non‑assembly parts for a specific product.

    This handler flattens the product's bill of materials to extract every
    structure that is a part (``flag_part``) but not an assembly and not
    commercial.  Each part appears once in the list regardless of how many
    times it is used in the BOM.  Parts are sorted alphabetically and
    display their on‑hand quantity along with an action to load stock via
    the production module.

    :param product_id: Primary key of the product whose parts should be listed.
    """
    _recalculate_assemblies()
    product = Product.query.get_or_404(product_id)
    components = ProductComponent.query.filter_by(product_id=product.id).all()
    comp_map = {comp.structure_id: comp for comp in components}
    root_structs: list[Structure] = []
    for comp in components:
        struct = comp.structure
        if struct.parent_id is None or struct.parent_id not in comp_map:
            root_structs.append(struct)
    # Collect non‑assembly, non‑commercial parts from the product BOM.  Use a
    # dictionary keyed by component_id when available or by the lower‑cased
    # name otherwise.  This collapses duplicate structures representing the
    # same physical part (absolute items) into a single entry.  The first
    # encountered structure for a given key is retained.
    parts_dict: dict[str | int, Structure] = {}
    def collect_parts(struct: Structure) -> None:
        # Include parts that are neither assemblies nor commercial items
        if not struct.flag_assembly and not struct.flag_commercial:
            if struct.component_id:
                key: str | int = struct.component_id
            elif struct.name:
                key = struct.name.strip().lower()
            else:
                key = struct.id
            # Insert only if not already present
            if key not in parts_dict:
                parts_dict[key] = struct
        for child in getattr(struct, 'children', []):
            collect_parts(child)
    for struct in root_structs:
        collect_parts(struct)
    parts: list[Structure] = list(parts_dict.values())
    # Sort alphabetically by name for predictable ordering
    parts.sort(key=lambda s: (s.name or '').lower())
    # Attach display images for each part using the shared helper
    for part in parts:
        try:
            image_filename: str | None = _lookup_structure_image(part)
        except Exception:
            image_filename = None
        setattr(part, 'display_image_filename', image_filename)
    return render_template(
        'inventory/product_parts.html',
        product=product,
        parts=parts,
        active_tab='parts'
    )


@inventory_bp.route('/product/<int:product_id>/commercial')
@login_required
def product_commercial(product_id: int):
    """Display all commercial components for a specific product.

    The list includes all structures flagged ``flag_commercial`` (and not
    assemblies) that are reachable from the product's bill of materials.
    Each commercial component appears only once even if referenced multiple
    times.  Components are sorted alphabetically and show their stock level
    and a link to the production module to load more stock.

    :param product_id: Primary key of the product whose commercial components should be listed.
    """
    _recalculate_assemblies()
    product = Product.query.get_or_404(product_id)
    components = ProductComponent.query.filter_by(product_id=product.id).all()
    comp_map = {comp.structure_id: comp for comp in components}
    root_structs: list[Structure] = []
    for comp in components:
        struct = comp.structure
        if struct.parent_id is None or struct.parent_id not in comp_map:
            root_structs.append(struct)
    # Collect commercial parts from the product BOM.  Deduplicate by
    # component_id or lower‑cased name to treat duplicates as absolute items.
    commercial_dict: dict[str | int, Structure] = {}
    def collect_comm(struct: Structure) -> None:
        if struct.flag_commercial and not struct.flag_assembly:
            if struct.component_id:
                key: str | int = struct.component_id
            elif struct.name:
                key = struct.name.strip().lower()
            else:
                key = struct.id
            if key not in commercial_dict:
                commercial_dict[key] = struct
        for child in getattr(struct, 'children', []):
            collect_comm(child)
    for struct in root_structs:
        collect_comm(struct)
    parts: list[Structure] = list(commercial_dict.values())
    parts.sort(key=lambda s: (s.name or '').lower())
    for part in parts:
        try:
            image_filename: str | None = _lookup_structure_image(part)
        except Exception:
            image_filename = None
        setattr(part, 'display_image_filename', image_filename)
    return render_template(
        'inventory/product_commercial.html',
        product=product,
        parts=parts,
        active_tab='commercial'
    )


@inventory_bp.route('/build/<int:assembly_id>', methods=['GET', 'POST'])
@login_required
def build_assembly(assembly_id: int):
    """Guided procedure for manually building an assembly.

    Operators select how many units to build, upload documents for each
    component part, and confirm the operation.  The handler checks that
    all parts have sufficient stock before decrementing their quantities
    and increasing the assembly quantity.  Uploaded files are saved
    alongside the original documentation.  Documentation is discovered
    by scanning the ``static/documents`` folder for subdirectories
    named after each part (using a safe version of the part name).  All
    files under such directories are presented to the user for download
    and require a corresponding upload of the compiled version.  This
    behaviour replaces the old pattern where only files prefixed with
    ``<id>_quality_`` or ``<id>_manual_`` were detected.
    """
    assembly = Structure.query.get_or_404(assembly_id)
    # Determine if the build page is rendered in embedded mode (e.g. inside an iframe).
    embedded_flag = bool(request.args.get('embedded'))
    # Capture the referring URL so that we can return the user to the page they
    # came from after completing or cancelling the build.  If no referrer
    # exists (e.g. when the page is opened directly), fall back to the
    # assemblies list.  This variable is reused later in POST handling.
    from_url = request.referrer or url_for('inventory.list_assemblies')
    # Retrieve immediate children of the assembly (parts, sub‑assemblies or commercial parts).
    parts: List[Structure] = (
        Structure.query
        .filter_by(parent_id=assembly.id)
        .order_by(Structure.id.asc())
        .all()
    )
    # When the component has no children (e.g. a part or commercial component),
    # treat the assembly itself as the only part so that the document
    # checklist applies directly to it.  This allows the guided build
    # procedure to be reused for single components outside of assemblies.
    if not parts:
        parts = [assembly]

    # Load checklist data once to determine which documents are required per structure.
    try:
        _checklist_map = load_checklist()
    except Exception:
        _checklist_map = {}
    # Base directory for documents
    doc_base_dir = os.path.join(current_app.static_folder, 'documents')
    # existing_docs maps part id -> arbitrary key -> list of docs (filename and display_name)
    existing_docs: dict[int, dict[str, list[dict[str, str]]]] = {}
    # required_docs maps part id -> list of required upload fields for existing docs
    # Each entry: { 'type': 'doc', 'display_name': doc_display, 'field_name': field_name, 'original_filename': rel_path }
    required_docs: dict[int, list[dict[str, str]]] = {}
    # user_docs maps part id -> list of relative paths of compiled documents
    user_docs: dict[int, list[str]] = {}
    # Mapping of document folder keys to human readable labels.  Used both
    # for display and for dummy document generation when no files exist.
    doc_label_map: dict[str, str] = {
        'qualita': 'Modulo Cert. qualità',
        '3_1_materiale': '3.1 Materiale',
        'step_tavole': 'Step/tavola',
        'funzionamento': 'Verifica funzionamento',
        'istruzioni': 'Montaggio istruzioni',
        'ddt_fornitore': 'DDT fornitore',
        'altro': 'Altro'
    }
    for part in parts:
        # Assign a display image for this part using the fallback helper
        try:
            image_filename = _lookup_structure_image(part)
        except Exception:
            image_filename = None
        setattr(part, 'display_image_filename', image_filename)

        # Build a list of candidate directory names for this part.  Historically
        # documentation folders have used a variety of naming conventions: some
        # are sanitised via ``secure_filename`` (lowercase and underscores), others
        # preserve the original case and spacing of the part name.  To ensure
        # previously uploaded documents can always be located we try both the
        # sanitised and the original names when searching for directories.
        # Fallback to ``id_<id>`` when the part name is empty or sanitises to
        # nothing.  Duplicates are removed while preserving order.
        safe_name = secure_filename(part.name) or f"id_{part.id}"
        candidates: list[str] = []
        if safe_name:
            candidates.append(safe_name)
        # Only add the raw name if it differs from the sanitised version
        raw_name = (part.name or '').strip()
        if raw_name and raw_name != safe_name:
            candidates.append(raw_name)
        # Normalise case for case-insensitive filesystems and to cover
        # directories created with different capitalisation.  For example a
        # directory named "DQS100" will not be found by the sanitised name
        # "dqs100" on a case-sensitive filesystem, so include a lowercase
        # variant as an additional candidate.  Avoid duplicates.
        lower_raw = raw_name.lower()
        if lower_raw and lower_raw not in candidates:
            candidates.append(lower_raw)

        # Decide which document folders apply to this component based on its typology.
        if getattr(part, 'flag_commercial', False):
            doc_folders = ['qualita', 'ddt_fornitore', 'step_tavole', '3_1_materiale']
        elif getattr(part, 'flag_assembly', False):
            doc_folders = ['qualita', 'step_tavole', 'funzionamento', 'istruzioni']
        else:
            # Mechanical parts (non‑commercial, non‑assembly)
            doc_folders = ['qualita', '3_1_materiale', 'step_tavole']

        docs_by_folder: dict[str, list[dict[str, str]]] = {}
        # Search for the first existing directory matching one of the candidate names.
        part_doc_dir = None
        for dname in candidates:
            possible_dir = os.path.join(doc_base_dir, dname)
            if os.path.isdir(possible_dir):
                part_doc_dir = possible_dir
                break
        if part_doc_dir:
            for folder in doc_folders:
                folder_path = os.path.join(part_doc_dir, folder)
                files: list[dict[str, str]] = []
                if os.path.isdir(folder_path):
                    for fname in os.listdir(folder_path):
                        full_path = os.path.join(folder_path, fname)
                        if os.path.isfile(full_path):
                            # Relative path within the static folder.  Use static root so that
                            # documentation stored in other top-level folders (e.g. tmp_components)
                            # is referenced correctly.  This path will be passed directly to
                            # products.download_file without prefixing 'documents/'.
                            rel_path = os.path.relpath(full_path, current_app.static_folder)
                            files.append({'filename': rel_path, 'display_name': fname})
                docs_by_folder[folder] = files
        else:
            # When no dedicated directory exists, populate empty lists for each folder
            docs_by_folder = {folder: [] for folder in doc_folders}
        # Fallback: if no documents were found in any folder, support legacy naming
        # conventions by scanning the top‑level documents directory for files
        # prefixed with the part id or safe name.  These are assigned to a
        # generic "qualita" folder so they still appear in the interface.
        if all(len(files) == 0 for files in docs_by_folder.values()) and os.path.isdir(doc_base_dir):
            prefix_id = f"{part.id}_"
            prefix_name = f"{safe_name}_"
            extra_files: list[dict[str, str]] = []
            for fname in os.listdir(doc_base_dir):
                full_path = os.path.join(doc_base_dir, fname)
                if os.path.isfile(full_path) and (fname.startswith(prefix_id) or fname.startswith(prefix_name)):
                    # Compute path relative to static folder for uniform handling
                    rel_path = os.path.relpath(full_path, current_app.static_folder)
                    extra_files.append({'filename': rel_path, 'display_name': fname})
            if extra_files:
                docs_by_folder['qualita'] = extra_files

        # -------------------------------------------------------------------
        # Include component master/default documentation.  Some parts and
        # commercial components define their default documentation under
        # ``static/tmp_components/<component>/<folder>`` or by a master code.  To
        # ensure these defaults are visible in the build interface, scan
        # candidate directories under ``tmp_components`` for this part.  We
        # consider the sanitised name, the raw name and lower-case variants,
        # along with the code of any associated ComponentMaster.  Found files
        # are appended to ``docs_by_folder`` without overwriting existing
        # entries.
        tmp_candidates: list[str] = []
        # Prioritise master code when available
        try:
            master = getattr(part, 'component_master', None)
        except Exception:
            master = None
        if master:
            try:
                master_code = secure_filename(master.code) or master.code
                if master_code:
                    tmp_candidates.append(master_code)
            except Exception:
                pass
        # Include sanitised, raw and lower-case names
        if safe_name and safe_name not in tmp_candidates:
            tmp_candidates.append(safe_name)
        if raw_name and raw_name != safe_name and raw_name not in tmp_candidates:
            tmp_candidates.append(raw_name)
        if lower_raw and lower_raw not in tmp_candidates:
            tmp_candidates.append(lower_raw)
        # Scan each candidate directory for documentation
        for cand in tmp_candidates:
            tmp_base = os.path.join(current_app.static_folder, 'tmp_components', cand)
            if not os.path.isdir(tmp_base):
                continue
            for folder in doc_folders:
                folder_path = os.path.join(tmp_base, folder)
                if not os.path.isdir(folder_path):
                    continue
                try:
                    for fname in os.listdir(folder_path):
                        full_path = os.path.join(folder_path, fname)
                        if not os.path.isfile(full_path):
                            continue
                        rel_path = os.path.relpath(full_path, current_app.static_folder)
                        # Initialise category list if absent
                        if folder not in docs_by_folder:
                            docs_by_folder[folder] = []
                        # Append if not already recorded
                        if not any(d.get('filename') == rel_path for d in docs_by_folder[folder]):
                            docs_by_folder[folder].append({'filename': rel_path, 'display_name': fname})
                except Exception:
                    # Ignore filesystem errors silently
                    pass
        # -------------------------------------------------------------------
        # Augment documentation with product‑level files.
        #
        # Operators often upload documentation for a component while editing
        # products.  Those files live under ``static/documents/<product>/<structure path>/<folder>``.
        # To provide visibility into previously uploaded documents when
        # building an assembly, search for product‑level files referencing
        # this part and merge them into ``docs_by_folder``.  Only consider
        # product components that link this structure; component master
        # (``tmp_components``) documents are intentionally ignored here
        # because they are design defaults rather than operator uploads.
        try:
            # Find all product components referencing this structure
            comps_for_part = ProductComponent.query.filter_by(structure_id=part.id).all()
        except Exception:
            comps_for_part = []
        for comp in comps_for_part:
            try:
                product = comp.product
            except Exception:
                product = None
            if not product:
                continue
            # Build candidate directory names for the product similar to parts.  Try
            # multiple variants to locate the correct document folder even when
            # names have been stored without sanitisation.
            prod_safe = secure_filename(product.name) or f"id_{product.id}"
            prod_candidates: list[str] = []
            if prod_safe:
                prod_candidates.append(prod_safe)
            raw_prod_name = (product.name or '').strip()
            if raw_prod_name and raw_prod_name != prod_safe:
                prod_candidates.append(raw_prod_name)
            lower_prod_name = raw_prod_name.lower()
            if lower_prod_name and lower_prod_name not in prod_candidates:
                prod_candidates.append(lower_prod_name)
            # Build structure path variants (sanitised, raw and lowercase)
            sanitized_parts: list[str] = []
            raw_parts: list[str] = []
            lower_parts: list[str] = []
            def _traverse(n):
                if n.parent:
                    _traverse(n.parent)
                sanitized_parts.append(secure_filename(n.name) or f"id_{n.id}")
                rn = (n.name or '').strip()
                raw_parts.append(rn)
                lower_parts.append(rn.lower() if rn else rn)
            try:
                _traverse(part)
            except Exception:
                continue
            # Attempt to locate existing directory using product and structure variants
            base_dir = None
            for prod_name in prod_candidates:
                # sanitised path
                candidate = os.path.join(current_app.static_folder, 'documents', prod_name, *sanitized_parts)
                if os.path.isdir(candidate):
                    base_dir = candidate
                    break
                candidate = os.path.join(current_app.static_folder, 'documents', prod_name, *raw_parts)
                if os.path.isdir(candidate):
                    base_dir = candidate
                    break
                candidate = os.path.join(current_app.static_folder, 'documents', prod_name, *lower_parts)
                if os.path.isdir(candidate):
                    base_dir = candidate
                    break
            # Build a list of base directories to search.  Only include the
            # structure‑specific directory (base_dir).  If found, this
            # directory corresponds to ``static/documents/<product>/<structure path>``.
            # We will scan it to surface product-level documents for this component.
            search_dirs: list[str] = []
            if base_dir:
                search_dirs.append(base_dir)
            # ----------------------------------------------------------------------
            # Merge product-level documentation into ``docs_by_folder``.  When
            # operators edit a product and upload documents for a component, those
            # files are stored under ``documents/<product>/<structure path>/<folder>``.
            # Without scanning these directories, the warehouse build page would not
            # display previously uploaded documentation, leading to confusion about
            # missing files.  For each base directory discovered, iterate over the
            # expected folders (qualita, 3_1_materiale, etc.) and append any files
            # found to the corresponding entry in ``docs_by_folder``.  Duplicate
            # detection prevents listing the same document multiple times when
            # multiple products reference the same component.
            for sdir in search_dirs:
                for folder in doc_folders:
                    folder_path = os.path.join(sdir, folder)
                    if not os.path.isdir(folder_path):
                        continue
                    try:
                        for fname in os.listdir(folder_path):
                            full_path = os.path.join(folder_path, fname)
                            if not os.path.isfile(full_path):
                                continue
                            rel_path = os.path.relpath(full_path, current_app.static_folder)
                            # Initialise the list if this category has not been seen
                            if folder not in docs_by_folder:
                                docs_by_folder[folder] = []
                            # Avoid duplicates by comparing relative paths
                            if not any(d.get('filename') == rel_path for d in docs_by_folder[folder]):
                                docs_by_folder[folder].append({'filename': rel_path, 'display_name': fname})
                    except Exception:
                        # Ignore filesystem errors silently
                        pass
        # ----------------------------------------------------------------------
        # After augmenting documentation with part, master and product‑level files,
        # determine which documents are visible and which uploads are required.
        # Use the checklist configuration to optionally filter documents but
        # always assign existing and required documents even when no product
        # references exist.  Without this block outside of the product loop
        # parts without product associations would never display documentation.
        part_id_str = str(part.id)
        flagged_list = _checklist_map.get(part_id_str, []) or []
        # Normalise flagged paths for comparison (convert backslashes and lower)
        flagged_norm = {p.replace('\\', '/').lower() for p in flagged_list}
        if flagged_list:
            # Filter docs_by_folder to retain only files whose normalised path is flagged
            for fkey, fdocs in list(docs_by_folder.items()):
                filtered = []
                for d in fdocs:
                    fname_norm = d.get('filename', '').replace('\\', '/').lower()
                    if fname_norm in flagged_norm:
                        filtered.append(d)
                docs_by_folder[fkey] = filtered
        # Store the (possibly filtered) existing documents for rendering
        existing_docs[part.id] = docs_by_folder
        # Build the list of required uploads.  When checklist flags exist for
        # this component we will derive the required documents from the flagged
        # list; otherwise the operator has not selected any documents in the
        # anagrafiche so no uploads are requested (empty list).
        req_list: list[dict[str, str]] = []
        if flagged_list:
            # For each flagged document, create an entry that specifies the
            # document type and the download path.  The type is inferred from
            # the path segment; if no known folder is present it defaults to
            # "altro".  Each flagged document gets its own upload field.
            override_list: list[dict[str, str]] = []
            idx_counter = 1
            for _p in flagged_list:
                doc_type = 'altro'
                norm_path = _p.replace('\\', '/').lower()
                for key in doc_label_map.keys():
                    if f"/{key}/" in norm_path:
                        doc_type = key
                        break
                field_name = f"{doc_type}_{part.id}_{idx_counter}"
                idx_counter += 1
                override_list.append({
                    'type': doc_type,
                    'display_name': doc_label_map.get(doc_type, doc_type),
                    'field_name': field_name,
                    'original_filename': _p
                })
            required_docs[part.id] = override_list
        else:
            # No flagged documents: do not request any documentation for this part.
            # Operators will see "Nessuna documentazione richiesta" and can
            # choose to flag documents in anagrafiche if necessary.
            required_docs[part.id] = req_list
        # Discover compiled (uploaded) documents.  These include any file under
        # the part's own directories, the component master directory and
        # any product-level directories referencing this component.
        compiled_list: list[str] = []
        # 1) Part-level documentation directories
        for dname in candidates:
            candidate_dir = os.path.join(doc_base_dir, dname)
            if os.path.isdir(candidate_dir):
                for _root, _dirs, files in os.walk(candidate_dir):
                    for _f in files:
                        fullp = os.path.join(_root, _f)
                        relp = os.path.relpath(fullp, current_app.static_folder)
                        if relp not in compiled_list:
                            compiled_list.append(relp)
        # 2) Component master default documents
        try:
            master = getattr(part, 'component_master', None)
            if master:
                master_code = secure_filename(master.code) or master.code
                tmp_dir = os.path.join(current_app.static_folder, 'tmp_components', master_code)
                if os.path.isdir(tmp_dir):
                    for _root, _dirs, files in os.walk(tmp_dir):
                        for _f in files:
                            fullp = os.path.join(_root, _f)
                            relp = os.path.relpath(fullp, current_app.static_folder)
                            if relp not in compiled_list:
                                compiled_list.append(relp)
        except Exception:
            pass
        # 3) Do not scan product-level structure paths for compiled documents.  Compiled
        # documentation is stored in the component-specific folder and in the
        # warehouse (magazzino) folder.  Avoid scanning product-level paths
        # to prevent cross contamination between components.
        user_docs[part.id] = compiled_list
    # -------------------------------------------------------------------------
    # Compute documentation and required uploads for the assembly itself.
    # In the warehouse context operators now need to upload documents only
    # for the assembly as a whole, not for each individual component.  To
    # support this behaviour we build the same structures (existing_docs,
    # required_docs and user_docs) for the assembly.  This allows the
    # template to render a consolidated documentation section at the bottom
    # of the build page.
    assembly_id_str = str(assembly.id)
    try:
        safe_asm_name = secure_filename(assembly.name) or f"id_{assembly.id}"
    except Exception:
        safe_asm_name = f"id_{assembly.id}"
    asm_candidates: list[str] = []
    if safe_asm_name:
        asm_candidates.append(safe_asm_name)
    raw_asm_name = (assembly.name or '').strip()
    if raw_asm_name and raw_asm_name != safe_asm_name:
        asm_candidates.append(raw_asm_name)
    lower_asm = raw_asm_name.lower()
    if lower_asm and lower_asm not in asm_candidates:
        asm_candidates.append(lower_asm)
    # Determine which folders to search based on assembly typology
    if getattr(assembly, 'flag_commercial', False):
        asm_doc_folders = ['qualita', 'ddt_fornitore', 'step_tavole', '3_1_materiale']
    elif getattr(assembly, 'flag_assembly', False):
        asm_doc_folders = ['qualita', 'step_tavole', 'funzionamento', 'istruzioni']
    else:
        asm_doc_folders = ['qualita', '3_1_materiale', 'step_tavole']
    asm_docs_by_folder: dict[str, list[dict[str, str]]] = {}
    asm_doc_dir: str | None = None
    for dname in asm_candidates:
        maybe_dir = os.path.join(doc_base_dir, dname)
        if os.path.isdir(maybe_dir):
            asm_doc_dir = maybe_dir
            break
    if asm_doc_dir:
        for folder in asm_doc_folders:
            folder_path = os.path.join(asm_doc_dir, folder)
            file_list: list[dict[str, str]] = []
            if os.path.isdir(folder_path):
                for fname in os.listdir(folder_path):
                    fpath = os.path.join(folder_path, fname)
                    if os.path.isfile(fpath):
                        rel_path = os.path.relpath(fpath, current_app.static_folder)
                        file_list.append({'filename': rel_path, 'display_name': fname})
            asm_docs_by_folder[folder] = file_list
    else:
        asm_docs_by_folder = {folder: [] for folder in asm_doc_folders}
    # Fallback: if no documents were found in any folder, scan the base documents
    # directory for files prefixed with the assembly id or safe name.  Assign
    # such files to the "qualita" folder to maintain consistency with component
    # logic.
    if all(len(fl) == 0 for fl in asm_docs_by_folder.values()) and os.path.isdir(doc_base_dir):
        prefix_id = f"{assembly.id}_"
        prefix_name = f"{safe_asm_name}_"
        extra_files: list[dict[str, str]] = []
        for fname in os.listdir(doc_base_dir):
            fpath = os.path.join(doc_base_dir, fname)
            if os.path.isfile(fpath) and (fname.startswith(prefix_id) or fname.startswith(prefix_name)):
                rel_path = os.path.relpath(fpath, current_app.static_folder)
                extra_files.append({'filename': rel_path, 'display_name': fname})
        if extra_files:
            asm_docs_by_folder['qualita'] = extra_files
    # Record existing documents for the assembly
    existing_docs[assembly.id] = asm_docs_by_folder
    # Determine which documents must be uploaded for the assembly based on the
    # checklist.  When checklist entries exist for the assembly we create an
    # upload field for each flagged document; otherwise no documentation is
    # required (empty list).
    asm_flagged_list = _checklist_map.get(assembly_id_str, []) or []
    asm_req_list: list[dict[str, str]] = []
    if asm_flagged_list:
        idx_counter = 1
        for pth in asm_flagged_list:
            doc_type = 'altro'
            norm = pth.replace('\\', '/').lower()
            for key in doc_label_map.keys():
                if f"/{key}/" in norm:
                    doc_type = key
                    break
            field_name = f"{doc_type}_{assembly.id}_{idx_counter}"
            idx_counter += 1
            asm_req_list.append({
                'type': doc_type,
                'display_name': doc_label_map.get(doc_type, doc_type),
                'field_name': field_name,
                'original_filename': pth
            })
        required_docs[assembly.id] = asm_req_list
    else:
        # No flagged documents for assembly: do not require any uploads
        required_docs[assembly.id] = asm_req_list
    # Discover compiled documentation for the assembly by scanning its own
    # directories.  This helps operators see previously uploaded files.
    asm_compiled: list[str] = []
    for dname in asm_candidates:
        candidate_dir = os.path.join(doc_base_dir, dname)
        if os.path.isdir(candidate_dir):
            for root_dir, _dirs, files in os.walk(candidate_dir):
                for f in files:
                    fullp = os.path.join(root_dir, f)
                    relp = os.path.relpath(fullp, current_app.static_folder)
                    if relp not in asm_compiled:
                        asm_compiled.append(relp)
    user_docs[assembly.id] = asm_compiled

    if request.method == 'POST':
        # Read back_url from the submitted form.  Fall back to the referrer captured
        # earlier if it is not provided.  This allows the view to redirect
        # correctly after a POST instead of returning to the build page itself.
        back_url = request.form.get('back_url') or from_url
        # Parse quantity requested by user
        quantity_str = request.form.get('quantity', '1')
        try:
            quantity = float(quantity_str)
            if quantity <= 0:
                flash('La quantità deve essere maggiore di zero.', 'warning')
                # Re-render page with defaults
                ready_parts = all((p.quantity_in_stock or 0) > 0 for p in parts) and len(parts) > 0
                return render_template(
                    'inventory/build_assembly.html',
                    assembly=assembly,
                    parts=parts,
                    ready_parts=ready_parts,
                    docs_ready=False,
                    ready=False,
                    existing_docs=existing_docs,
                    required_docs=required_docs,
                    user_docs=user_docs,
                    back_url=back_url,
                    embedded=embedded_flag
                )
        except ValueError:
            flash('Inserisci un numero valido per la quantità.', 'warning')
            ready_parts = all((p.quantity_in_stock or 0) > 0 for p in parts) and len(parts) > 0
            return render_template(
                'inventory/build_assembly.html',
                assembly=assembly,
                parts=parts,
                ready_parts=ready_parts,
                docs_ready=False,
                ready=False,
                existing_docs=existing_docs,
                required_docs=required_docs,
                user_docs=user_docs,
                back_url=back_url,
                embedded=embedded_flag
            )
        # Check part stock sufficiency.  Group identical parts and ensure that
        # the available stock is sufficient for the required number of units.
        insuff_messages: list[str] = []
        part_groups_suff: dict[tuple[str | int, str], list[Structure]] = {}
        # Save uploaded documentation for the assembly itself
        asm_safe_name = secure_filename(assembly.name) or f"id_{assembly.id}"
        asm_req = required_docs.get(assembly.id, [])
        for doc_def in asm_req:
            field_name = doc_def['field_name']
            file_obj = request.files.get(field_name)
            if not file_obj or not file_obj.filename:
                continue
            doc_type = doc_def.get('type') or ''
            original_rel = doc_def.get('original_filename')
            # Determine destination directory: if an original file exists,
            # place the new file alongside it (mirroring legacy behaviour).
            # Otherwise create a directory under doc_base_dir using the assembly
            # safe name and document type.
            if original_rel:
                rel_dir = os.path.dirname(original_rel)
                norm_rel = rel_dir.replace(os.sep, '/')
                if norm_rel.startswith('tmp_components'):
                    dest_dir = os.path.join(current_app.static_folder, rel_dir)
                else:
                    dest_dir = os.path.join(doc_base_dir, rel_dir)
            else:
                dest_dir = os.path.join(doc_base_dir, asm_safe_name, doc_type)
            # Ensure the destination directory exists
            os.makedirs(dest_dir, exist_ok=True)
            # Generate a unique filename using the current date and time plus the
            # original uploaded filename.  This prevents collisions without
            # borrowing names from other documents.  Format: YYYYMMDD_HHMMSS_<original_name>
            upload_name = secure_filename(file_obj.filename)
            timestamp_str = time.strftime('%Y%m%d_%H%M%S')
            unique_name = f"{timestamp_str}_{upload_name}"
            dest_path = os.path.join(dest_dir, unique_name)
            try:
                file_obj.save(dest_path)
            except Exception:
                # Ignore errors saving individual files to avoid interrupting
                # the transaction.  Continue to next file.
                pass
            # Always copy compiled docs into the assembly-specific directory
            asm_comp_dest_dir = os.path.join(doc_base_dir, asm_safe_name, doc_type)
            try:
                if os.path.abspath(dest_dir) != os.path.abspath(asm_comp_dest_dir):
                    os.makedirs(asm_comp_dest_dir, exist_ok=True)
                    dest_copy = os.path.join(asm_comp_dest_dir, unique_name)
                    try:
                        shutil.copyfile(dest_path, dest_copy)
                    except Exception:
                        # Silently ignore copy errors
                        pass
            except Exception:
                pass
        # Now process component documentation as before (although no docs are required for components)
        for part in parts:
            if part.component_id:
                key = ('id', part.component_id)
            elif part.name:
                key = ('name', part.name.strip().lower())
            else:
                key = ('unique', part.id)
            part_groups_suff.setdefault(key, []).append(part)
        for key, group_parts in part_groups_suff.items():
            # Determine how many units of this component are required per assembly
            # When this assembly is linked to a product via a ProductComponent, use
            # the BOM quantity for the child structure.  Otherwise fallback to
            # counting duplicate structure rows (legacy behaviour).
            units_required: float | None = None
            representative = group_parts[0]
            try:
                # Build a mapping of structure_id to ProductComponent for a product
                # referencing this assembly.  This map is computed above for the
                # actual build but we recompute it here to avoid referencing
                # variables outside this scope.  When no product references the
                # assembly, comp_map will remain empty.
                from ...models import ProductComponent
                comp_map_suff: dict[int, ProductComponent] = {}
                try:
                    prod_comp_suff = ProductComponent.query.filter_by(structure_id=assembly.id).first()
                    if prod_comp_suff:
                        product_id = prod_comp_suff.product_id
                        comps = ProductComponent.query.filter_by(product_id=product_id).all()
                        comp_map_suff = {c.structure_id: c for c in comps}
                except Exception:
                    comp_map_suff = {}
                comp_entry = comp_map_suff.get(representative.id) if comp_map_suff else None
                if comp_entry and (comp_entry.quantity or 0) > 0:
                    units_required = comp_entry.quantity
            except Exception:
                units_required = None
            if units_required is None:
                units_required = len(group_parts)
            # Compute the global current quantity across matches for this part
            matches_dict: dict[int, Structure] = {}
            try:
                # Name-based matches
                try:
                    if representative.name:
                        for s in Structure.query.filter(Structure.name == representative.name).all():
                            matches_dict[s.id] = s
                except Exception:
                    pass
                # Component_id matches when defined
                if representative.component_id:
                    try:
                        for s in Structure.query.filter(Structure.component_id == representative.component_id).all():
                            matches_dict[s.id] = s
                    except Exception:
                        pass
            except Exception:
                matches_dict = {}
            matches = list(matches_dict.values())
            # Collect available quantities across all matches
            quantities: list[float] = []
            for s in matches:
                try:
                    quantities.append(float(s.quantity_in_stock or 0))
                except Exception:
                    pass
            if not quantities:
                try:
                    quantities.append(float(representative.quantity_in_stock or 0))
                except Exception:
                    quantities.append(0.0)
            current_qty: float = max(quantities) if quantities else 0.0
            total_required: float = units_required * quantity
            if current_qty < total_required:
                # Compose message showing available vs required
                part_name = representative.name or 'Component'
                insuff_messages.append(f"{part_name}: {int(current_qty)} disponibili, {int(total_required)} richiesti")
        if insuff_messages:
            msgs = ["Componenti insufficienti per costruire l'assieme:"]
            msgs.extend(insuff_messages)
            flash('\n'.join(msgs), 'danger')
            ready_parts = False
            return render_template(
                'inventory/build_assembly.html',
                assembly=assembly,
                parts=parts,
                ready_parts=ready_parts,
                docs_ready=False,
                ready=False,
                existing_docs=existing_docs,
                required_docs=required_docs,
                user_docs=user_docs,
                back_url=back_url,
                embedded=embedded_flag
            )
        # Validate documentation uploads: each existing document requires a replacement
        missing_docs: list[str] = []
        # Validate documentation uploads only for the assembly.  Component
        # documentation is no longer required during the build process.
        asm_req_list = required_docs.get(assembly.id, [])
        for doc_def in asm_req_list:
            field_name = doc_def['field_name']
            file_obj = request.files.get(field_name)
            # Document category/type (e.g. qualita, 3_1_materiale).  Used when
            # creating new folders for compiled docs.
            doc_type = doc_def.get('type') or ''
            if not file_obj or file_obj.filename.strip() == '':
                missing_docs.append(f"{assembly.name}: {doc_def['display_name']}")
        if missing_docs:
            # Some required documents are not uploaded
            msgs = ["Carica tutti i documenti richiesti prima di procedere:"]
            msgs.extend(missing_docs)
            flash('\n'.join(msgs), 'danger')
            ready_parts = True
            docs_ready = False
            # Re-render page highlighting missing docs
            return render_template(
                'inventory/build_assembly.html',
                assembly=assembly,
                parts=parts,
                ready_parts=ready_parts,
                docs_ready=docs_ready,
                ready=False,
                existing_docs=existing_docs,
                required_docs=required_docs,
                user_docs=user_docs,
                back_url=back_url,
                embedded=embedded_flag
            )
        # All validations passed: save uploaded documentation and update stock
        # Save each uploaded file in the same folder as the original document.  This
        # preserves the logical organisation of documentation by part.  New files
        # include a timestamp and original filename to prevent collisions.
        for part in parts:
            # Determine a safe filesystem name for the component.  Use a
            # sanitised version of the structure name, falling back to ``id_<id>`` when
            # necessary.  This name is reused for storing and copying files.
            safe_name = secure_filename(part.name) or f"id_{part.id}"
            req_list = required_docs.get(part.id, [])
            for doc_def in req_list:
                field_name = doc_def['field_name']
                file_obj = request.files.get(field_name)
                if not file_obj or not file_obj.filename:
                    continue
                # Determine the document type/category for this upload (e.g. "qualita",
                # "3_1_materiale").  Always derive this from the doc definition to
                # ensure it is available for copying, even when an original file
                # existed.  Without this each subsequent iteration would reuse a
                # previously defined ``doc_type`` leading to misplaced copies.
                doc_type = doc_def.get('type') or ''
                # Compute the destination directory.  When an original file exists, place
                # the compiled document alongside it.  Otherwise create a folder based
                # on the component name and document type.  ``original_rel`` stores a
                # path relative to the static folder (e.g. ``documents/P001-024-052/qualita/doc.pdf``
                # or ``tmp_components/OR2043/qualita/doc.pdf``).  We normalise it to
                # derive the correct destination base.
                original_rel = doc_def.get('original_filename')
                if original_rel:
                    rel_dir = os.path.dirname(original_rel)
                    # If the original document lives in ``tmp_components`` resolve the
                    # destination relative to the static folder.  Otherwise resolve
                    # relative to ``doc_base_dir`` so that files end up alongside
                    # existing documents for the component.
                    norm_rel = rel_dir.replace(os.sep, '/')
                    if norm_rel.startswith('tmp_components'):
                        dest_dir = os.path.join(current_app.static_folder, rel_dir)
                    else:
                        dest_dir = os.path.join(doc_base_dir, rel_dir)
                    base_original = os.path.splitext(os.path.basename(original_rel))[0]
                else:
                    # No existing document: organise compiled docs under the component's
                    # own directory grouped by document type.
                    dest_dir = os.path.join(doc_base_dir, safe_name, doc_type)
                    base_original = f"{doc_type}"
                # Ensure the destination directory exists
                os.makedirs(dest_dir, exist_ok=True)
                # Build a unique filename to avoid collisions.  Use the current
                # date and time plus the original uploaded filename.  Format:
                # YYYYMMDD_HHMMSS_<original_name>.  This avoids reusing names from
                # previous documents or document types.
                upload_name = secure_filename(file_obj.filename)
                timestamp_str = time.strftime('%Y%m%d_%H%M%S')
                unique_name = f"{timestamp_str}_{upload_name}"
                dest_path = os.path.join(dest_dir, unique_name)
                try:
                    # Save the uploaded file to the primary destination
                    file_obj.save(dest_path)
                except Exception:
                    # Ignore errors saving individual files to avoid interrupting
                    # the transaction.  Continue to next file.
                    continue
                # ------------------------------------------------------------------
                # Always copy compiled documents into the component-specific
                # directory under ``static/documents/<component>/<doc_type>``.  This
                # ensures that documentation uploaded during assembly creation is
                # accessible when building assemblies in the future.  Skip the
                # copy when the primary destination already matches the
                # component-specific path to avoid redundant writes.
                comp_dest_dir = os.path.join(doc_base_dir, safe_name, doc_type)
                # Compare normalised absolute paths to detect identical destinations
                try:
                    if os.path.abspath(dest_dir) != os.path.abspath(comp_dest_dir):
                        os.makedirs(comp_dest_dir, exist_ok=True)
                        dest_copy = os.path.join(comp_dest_dir, unique_name)
                        try:
                            shutil.copyfile(dest_path, dest_copy)
                        except Exception:
                            # Silently ignore copy errors
                            pass
                except Exception:
                    pass
                # ------------------------------------------------------------------
                # Do not copy compiled documents into product-specific directories.
                # The previous implementation replicated compiled files into
                # ``static/documents/<product>/<component>/<doc_type>`` for every product
                # referencing this component.  This caused documents uploaded during
                # the assembly build to appear under product folders reserved for
                # anagrafiche (e.g. ``static/documents/DQS100``), which is no longer
                # desirable.  To satisfy the new requirement, compiled documents
                # remain only under the component-level folder and the ``Produzione``
                # archive (handled separately).  Should future replication be needed,
                # it can be implemented here.
        # Decrement stock of parts and increment stock of assembly.  All
        # structures referencing the same component (by component_id or name)
        # share the same on‑hand quantity.  When reducing stock on one
        # component we update every matching structure to reflect the new
        # quantity.  Negative quantities are floored at zero to avoid
        # displaying negative stock levels.
        #
        # Determine BOM quantities for the assembly by inspecting the first
        # product that references this assembly via ProductComponent.  Build
        # a mapping from structure_id to ProductComponent for that product so
        # that we can retrieve the required quantity for each child.  When
        # no product references the assembly, comp_map remains empty and we
        # fall back to counting duplicate structure rows.
        from ...models import ProductComponent, Product  # Import locally to avoid cycles
        comp_map: dict[int, ProductComponent] = {}
        try:
            prod_comp = ProductComponent.query.filter_by(structure_id=assembly.id).first()
            if prod_comp:
                product_id = prod_comp.product_id
                comps = ProductComponent.query.filter_by(product_id=product_id).all()
                comp_map = {c.structure_id: c for c in comps}
        except Exception:
            comp_map = {}

        # Consolidate parts by physical component.  Use a tuple key based on
        # component_id when available, otherwise the lower‑cased name.  This
        # accounts for legacy entries without component_id but with the same
        # name.
        part_groups: dict[tuple[str | int, str], list[Structure]] = {}
        for part in parts:
            if part.component_id:
                key = ('id', part.component_id)
            elif part.name:
                key = ('name', part.name.strip().lower())
            else:
                key = ('unique', part.id)
            part_groups.setdefault(key, []).append(part)

        # For each group compute the total quantity to subtract and update stock
        for key, group_parts in part_groups.items():
            representative = group_parts[0]
            # Determine how many units of this component are required per assembly
            # Prefer the BOM quantity from comp_map when available; fallback
            # to the number of duplicate structure rows in the group.
            units_required: float | None = None
            try:
                comp_entry = comp_map.get(representative.id)
                if comp_entry and (comp_entry.quantity or 0) > 0:
                    units_required = comp_entry.quantity
            except Exception:
                units_required = None
            if units_required is None:
                units_required = len(group_parts)
            # Identify all matching structures across the warehouse for this component
            matches_dict: dict[int, Structure] = {}
            try:
                # Name-based matches
                try:
                    if representative.name:
                        for s in Structure.query.filter(Structure.name == representative.name).all():
                            matches_dict[s.id] = s
                except Exception:
                    pass
                # Component_id matches when defined
                if representative.component_id:
                    try:
                        for s in Structure.query.filter(Structure.component_id == representative.component_id).all():
                            matches_dict[s.id] = s
                    except Exception:
                        pass
            except Exception:
                matches_dict = {}
            matches = list(matches_dict.values())
            # Collect current quantities across all matches
            quantities: list[float] = []
            for s in matches:
                try:
                    quantities.append(float(s.quantity_in_stock or 0))
                except Exception:
                    pass
            # Fallback to the representative part's quantity if no matches found
            if not quantities:
                try:
                    quantities.append(float(representative.quantity_in_stock or 0))
                except Exception:
                    quantities.append(0.0)
            # Global current stock is the maximum across matches
            current_qty: float = max(quantities) if quantities else 0.0
            # Compute new quantity: subtract units_required * quantity_of_assemblies
            total_units_to_subtract: float = units_required * quantity
            new_qty: float = current_qty - total_units_to_subtract
            if new_qty < 0:
                new_qty = 0.0
            # Apply new quantity to all matches and to each part in the group
            for s in matches:
                s.quantity_in_stock = new_qty
            for p in group_parts:
                p.quantity_in_stock = new_qty
        # Increment stock on the assembly itself.  Assemblies do not share stock
        # across codes, so we update only the current assembly node.
        assembly.quantity_in_stock = (assembly.quantity_in_stock or 0) + quantity
        # Persist the stock changes on parts and assembly
        db.session.commit()
        # --- REGISTRAZIONE COSTRUZIONE ASSIEME (DB) ----------------------
        # After adjusting stock, record the assembly build in the ProductBuild
        # archive.  We record a ProductBuild row for the corresponding
        # Product (when a product shares the same name as the assembly) and
        # create ProductBuildItem rows for each BOM component.  The user
        # performing the build and the production box (if supplied via
        # ``box_id`` query parameter) are persisted when available.
        try:
            from ...models import Product, ProductComponent, ProductBuild, ProductBuildItem, BOMLine
            # Map the assembly's name to a Product.  Assemblies often correspond
            # to a Product with the same name.  Fallback to the first Product
            # referencing this structure via ProductComponent when no name
            # match exists.  If no product is found the build is skipped.
            assembly_product = Product.query.filter_by(name=assembly.name).first()
            if not assembly_product:
                try:
                    comp_ref = ProductComponent.query.filter_by(structure_id=assembly.id).first()
                    if comp_ref:
                        assembly_product = Product.query.get(comp_ref.product_id)
                except Exception:
                    assembly_product = None
            if assembly_product:
                # Quantity may be a float from the form; convert to int for storage
                try:
                    build_qty = int(quantity)
                except Exception:
                    build_qty = 1
                pb = ProductBuild(product_id=assembly_product.id,
                                  qty=build_qty,
                                  user_id=getattr(current_user, 'id', None))
                box_id_param = request.args.get('box_id')
                if box_id_param:
                    try:
                        pb.production_box_id = int(box_id_param)
                    except Exception:
                        pb.production_box_id = None
                db.session.add(pb)
                db.session.flush()
                bom_lines = BOMLine.query.filter_by(padre_id=assembly_product.id).all()
                for line in bom_lines:
                    try:
                        qty_required = (line.quantita or 1) * build_qty
                    except Exception:
                        qty_required = build_qty
                    db.session.add(ProductBuildItem(build_id=pb.id,
                                                   product_id=line.figlio_id,
                                                   quantity_required=qty_required))
                db.session.commit()
        except Exception as e:
            # On any error, log the exception and rollback the transaction to
            # avoid leaving partial records.  Do not interrupt the build
            # workflow; the assembly has already been built.
            current_app.logger.exception("Registrazione build assembly fallita: %s", e)
            db.session.rollback()
        # ---------------------------------------------------------------
        # If this build was initiated from a production box (specified via
        # ``box_id`` query parameter), mark the box and its items as completed.
        # This ensures that returning to the production dashboard reflects the
        # updated state and that the box no longer appears as "APERTO".
        try:
            box_id_param = request.args.get('box_id')
            if box_id_param:
                from ...models import ProductionBox
                try:
                    box_id_int = int(box_id_param)
                except (TypeError, ValueError):
                    box_id_int = None
                if box_id_int:
                    box_obj = ProductionBox.query.get(box_id_int)
                    if box_obj and box_obj.status != 'COMPLETATO':
                        box_obj.status = 'COMPLETATO'
                        # Optionally mark contained stock items as completed
                        try:
                            for si in box_obj.stock_items:
                                si.status = 'COMPLETATO'
                        except Exception:
                            pass
                        db.session.commit()
        except Exception:
            # Do not block the assembly build if updating the production box fails
            pass
        # Log the assembly build operation.  Use a try/except guard so that
        # failures in logging do not block the primary action.  The log
        # records the user who performed the build, the assembly affected,
        # the category and a textual description.
        try:
            log = InventoryLog(
                user_id=current_user.id,
                structure_id=assembly.id,
                category='assemblies',
                action=f'Assemblati {quantity:.0f}',
                quantity=quantity
            )
            db.session.add(log)
            db.session.commit()
        except Exception:
            # Ignore errors when writing the log; the main transaction has
            # already succeeded.
            pass
        # Recalculate buildable quantities (now a no-op) and flash success message
        _recalculate_assemblies()
        flash(f'Assemblati {quantity:.0f} pezzi di {assembly.name}.', 'success')
        # ----------------------------------------------------------------------
        # Persist a copy of the completed assembly in the production archive.
        # Assemblies built via the manual build interface should be saved
        # under ``Produzione/Assiemi_completati`` just like those built via
        # ``build_product``.  This ensures the assembly appears in the
        # assemblies archive even when no scan events were recorded.  The
        # directory structure mirrors that used in ``build_product``: a
        # top-level folder named ``<assembly_safe>_<timestamp>`` containing
        # subdirectories for each document type and a ``componenti`` folder
        # where each child component has its own timestamped directory.
        try:
            asm_safe = secure_filename(assembly.name) or f"id_{assembly.id}"
            asm_timestamp = int(time.time())
            asm_folder_name = f"{asm_safe}_{asm_timestamp}"
            app_root = current_app.root_path
            produzione_root = os.path.join(app_root, 'Produzione')
            completati_root = os.path.join(produzione_root, 'Assiemi_completati')
            os.makedirs(completati_root, exist_ok=True)
            asm_dir = os.path.join(completati_root, asm_folder_name)
            os.makedirs(asm_dir, exist_ok=True)
            # ------------------------------------------------------------------
            # Write a metadata file for the completed assembly build.  This file
            # captures the user who performed the build, the quantity built and
            # the timestamp.  When later rendering the assemblies archive this
            # metadata allows the user column to be populated without relying on
            # ScanEvent records (which are not created for manual builds).  The
            # metadata is saved as JSON in a file named ``meta.json`` within the
            # assembly directory.  Failures during writing are silently ignored
            # to avoid interrupting the build workflow.
            try:
                import json as _json
                meta_path = os.path.join(asm_dir, 'meta.json')
                # Capture static metadata about the build.  The meta.json file
                # persists information that should never change once the build is
                # complete, including the user, quantity, timestamp and the
                # assembly's name/description and revision at the moment of
                # assembly.  Storing these values prevents later edits to the
                # Structure (anagrafica) from altering the archive history.
                meta_data = {
                    'user_id': getattr(current_user, 'id', None),
                    'user_username': getattr(current_user, 'username', None),
                    'quantity': quantity,
                    'timestamp': asm_timestamp,
                    'structure_name': getattr(assembly, 'name', None),
                    'structure_description': getattr(assembly, 'description', None),
                    # Store both the human-readable revision label and the numeric index.
                    'revision_label': None,
                    'revision_index': None,
                }
                try:
                    # Compute the revision label via the property on Structure.  When undefined, an empty string is returned.
                    meta_data['revision_label'] = assembly.revision_label or ''
                except Exception:
                    meta_data['revision_label'] = ''
                try:
                    rev_idx = getattr(assembly, 'revision', None)
                    if rev_idx is not None:
                        meta_data['revision_index'] = int(rev_idx)
                except Exception:
                    meta_data['revision_index'] = None
                with open(meta_path, 'w', encoding='utf-8') as _meta_file:
                    _json.dump(meta_data, _meta_file)
            except Exception:
                # Ignore errors writing metadata file
                pass
            # Copy assembly-level documents from static/documents/<assembly_safe>
            try:
                assembly_doc_base = os.path.join(current_app.static_folder, 'documents', asm_safe)
                if os.path.isdir(assembly_doc_base):
                    for doc_type in os.listdir(assembly_doc_base):
                        src_dir = os.path.join(assembly_doc_base, doc_type)
                        if not os.path.isdir(src_dir):
                            continue
                        dest_dir = os.path.join(asm_dir, doc_type)
                        os.makedirs(dest_dir, exist_ok=True)
                        for fname in os.listdir(src_dir):
                            src_file = os.path.join(src_dir, fname)
                            dest_file = os.path.join(dest_dir, fname)
                            try:
                                shutil.copyfile(src_file, dest_file)
                            except Exception:
                                pass
            except Exception:
                pass
            # Copy any documentation saved in the production archive for this assembly
            try:
                archivio_root = os.path.join(produzione_root, 'archivio')
                if os.path.isdir(archivio_root):
                    for dir_name in os.listdir(archivio_root):
                        if not dir_name.startswith(f"{asm_safe}_"):
                            continue
                        arch_dir = os.path.join(archivio_root, dir_name)
                        if not os.path.isdir(arch_dir):
                            continue
                        for dt_name in os.listdir(arch_dir):
                            dt_path = os.path.join(arch_dir, dt_name)
                            if not os.path.isdir(dt_path):
                                continue
                            dest_dir = os.path.join(asm_dir, dt_name)
                            os.makedirs(dest_dir, exist_ok=True)
                            for fname in os.listdir(dt_path):
                                src_file = os.path.join(dt_path, fname)
                                dest_file = os.path.join(dest_dir, fname)
                                try:
                                    if not os.path.exists(dest_file):
                                        shutil.copyfile(src_file, dest_file)
                                except Exception:
                                    pass
            except Exception:
                pass
            # Create folder for component documentation
            comps_dir = os.path.join(asm_dir, 'componenti')
            os.makedirs(comps_dir, exist_ok=True)
            # For each part in the BOM (parts list) copy its documents
            for part in parts:
                child_safe = secure_filename(part.name) or f"id_{part.id}"
                comp_timestamp = int(time.time())
                comp_folder_name = f"{child_safe}_{comp_timestamp}"
                comp_dest_dir = os.path.join(comps_dir, comp_folder_name)
                os.makedirs(comp_dest_dir, exist_ok=True)
                # Copy docs from static documents under assembly/component hierarchy
                try:
                    comp_src_base = os.path.join(current_app.static_folder, 'documents', asm_safe, child_safe)
                    if os.path.isdir(comp_src_base):
                        for doc_type in os.listdir(comp_src_base):
                            src_dir = os.path.join(comp_src_base, doc_type)
                            if not os.path.isdir(src_dir):
                                continue
                            dest_type_dir = os.path.join(comp_dest_dir, doc_type)
                            os.makedirs(dest_type_dir, exist_ok=True)
                            for fname in os.listdir(src_dir):
                                src_file = os.path.join(src_dir, fname)
                                dest_file = os.path.join(dest_type_dir, fname)
                                try:
                                    shutil.copyfile(src_file, dest_file)
                                except Exception:
                                    pass
                except Exception:
                    pass
                # Copy docs from production archive for this component
                try:
                    archivio_root = os.path.join(produzione_root, 'archivio')
                    if os.path.isdir(archivio_root):
                        for dir_name in os.listdir(archivio_root):
                            if not dir_name.startswith(f"{child_safe}_"):
                                continue
                            arch_comp_dir = os.path.join(archivio_root, dir_name)
                            if not os.path.isdir(arch_comp_dir):
                                continue
                            for dt_name in os.listdir(arch_comp_dir):
                                dt_path = os.path.join(arch_comp_dir, dt_name)
                                if not os.path.isdir(dt_path):
                                    continue
                                dest_type_dir = os.path.join(comp_dest_dir, dt_name)
                                os.makedirs(dest_type_dir, exist_ok=True)
                                for fname in os.listdir(dt_path):
                                    src_file = os.path.join(dt_path, fname)
                                    dest_file = os.path.join(dest_type_dir, fname)
                                    try:
                                        if not os.path.exists(dest_file):
                                            shutil.copyfile(src_file, dest_file)
                                    except Exception:
                                        pass
                except Exception:
                    pass
        except Exception:
            # Suppress any errors during archive copy.  The assembly build has
            # already been recorded and stock levels updated so file errors
            # should not block the user.
            pass
        # -------------------------------------------------------------------
        # Create a printable summary of the assembly and its components.  This
        # snapshot captures the state at build time and is saved under
        # ``static/prints``.  Subsequent changes to parts or assemblies will
        # not affect this record.  Operators can access these files directly
        # from the filesystem for auditing or printing.
        try:
            print_dir = os.path.join(current_app.static_folder, 'prints')
            os.makedirs(print_dir, exist_ok=True)
            summary_name = f"build_assembly_{assembly.id}_{int(time.time())}.txt"
            summary_path = os.path.join(print_dir, summary_name)
            with open(summary_path, 'w', encoding='utf-8') as summary:
                summary.write(f"Assemblea: {assembly.name} (ID: {assembly.id})\n")
                summary.write("Componenti associati:\n")
                for p in parts:
                    try:
                        pname = p.name or f"ID {p.id}"
                    except Exception:
                        pname = f"ID {p.id}"
                    summary.write(f" - {pname} (ID: {p.id})\n")
                summary.write("Documenti dell'assieme:\n")
                for doc_def in required_docs.get(assembly.id, []):
                    try:
                        summary.write(f" - {doc_def.get('display_name', '')}\n")
                    except Exception:
                        pass
        except Exception:
            # Do not interrupt the workflow if printing fails
            pass
        # After copying documentation, either return a minimal success page
        # when built inside a production box modal or redirect back to the
        # originating page.  The success page contains JavaScript that
        # closes the modal and reloads the parent page so the box status
        # updates immediately.  When no modal context exists (embedded_flag
        # false or no box_id parameter), perform a normal redirect.
        try:
            box_id_val = request.args.get('box_id')
            if box_id_val and embedded_flag:
                # Render a minimal success page for embedded builds
                return render_template(
                    'inventory/build_assembly_success.html',
                    back_url=back_url
                )
        except Exception:
            # Ignore errors and fall back to redirect
            pass
        # Otherwise redirect back to the originating page.  If the referrer
        # header is not available, fall back to the main inventory overview.
        return redirect(back_url or url_for('inventory.list_assemblies'))
    # GET request: compute readiness indicators and association state
    # A part is ready if it has at least one unit in stock.  If the assembly has no
    # children the ready_parts flag will be False, preventing construction.
    ready_parts = all((p.quantity_in_stock or 0) > 0 for p in parts) and len(parts) > 0
    # Determine whether documentation is ready on initial load.  If no documentation
    # exists for any of the parts (i.e. nothing needs to be uploaded) we mark
    # docs_ready as True so the build button is enabled.  Otherwise, docs_ready
    # starts as False and only becomes True after the POST branch validates uploads.
    # Consider only assembly-level documentation for readiness.  Even when
    # components have documents assigned, operators upload documents solely for
    # the assembly, so docs_ready depends on the assembly requirements.
    docs_ready = len(required_docs.get(assembly.id, [])) == 0
    # -------------------------------------------------------------------------
    # Determine quantities required for each part of this assembly and how many
    # have already been associated via scans.  When a box context is provided
    # (through the ``box_id`` query parameter) we derive required quantities from
    # the ProductComponent definitions of the reserved product.  We also
    # discover the DataMatrix code of the assembly stock item in the box
    # (assembly_code) and count how many child components have been linked to it
    # through the StockItem.parent_code field.  In the absence of a box
    # context all required quantities default to 1 and no associations exist.
    required_quantities: dict[int, int] = {}
    associated_counts: dict[int, int] = {}
    assembly_code: str | None = None
    assoc_ready: bool = True
    box_id_param = request.args.get('box_id')
    if box_id_param:
        try:
            box_id_int = int(box_id_param)
        except (TypeError, ValueError):
            box_id_int = None
        if box_id_int:
            try:
                from ...models import ProductionBox, ProductComponent, StockItem
            except Exception:
                ProductionBox = None
            box = ProductionBox.query.get(box_id_int) if ProductionBox else None
            product_id = None
            if box and box.stock_items:
                try:
                    product_id = box.stock_items[0].product_id
                except Exception:
                    product_id = None
            # Build a map of required quantities per structure id from ProductComponent
            comp_map: dict[int, int] = {}
            if product_id:
                try:
                    comps = ProductComponent.query.filter_by(product_id=product_id).all()
                    for comp in comps:
                        try:
                            qty_val = int(comp.quantity or 1)
                        except Exception:
                            qty_val = 1
                        comp_map[comp.structure_id] = qty_val
                except Exception:
                    comp_map = {}
            for p in parts:
                try:
                    required_quantities[p.id] = int(comp_map.get(p.id, 1)) if True else 1
                except Exception:
                    required_quantities[p.id] = 1
            # Determine the DataMatrix code representing this assembly within the box.
            # When building a sub‑assembly there may not be a stock item in the box
            # whose DataMatrix code matches the assembly name.  In that case we
            # synthesise a code based on the assembly name (P=<name>|T=ASSIEME)
            # instead of defaulting to the first stock item.  This prevents
            # associating components to the wrong assembly.
            if box:
                asm_name = assembly.name
                try:
                    # Try to find a stock item whose P= component matches the assembly name
                    for it in box.stock_items:
                        dm = it.datamatrix_code or ''
                        component_name = None
                        for seg in dm.split('|'):
                            if seg.startswith('P='):
                                component_name = seg.split('=', 1)[1]
                                break
                        # Match component name exactly (case sensitive) to avoid false positives
                        if component_name == asm_name:
                            assembly_code = dm
                            break
                except Exception:
                    assembly_code = None
                # If no matching stock item was found, synthesise a DataMatrix code
                if not assembly_code:
                    assembly_code = f"P={asm_name}|T=ASSIEME"
            # Compute the number of associated components per part when an assembly code is known.
            # Use the StockItem.parent_code field to count how many components have been linked to
            # this assembly_code.  ``associate_component`` sets parent_code on the component's
            # StockItem to the assembly_code, regardless of whether a matching assembly stock item
            # exists in the box.  Therefore this count works for both pre‑existing assemblies and
            # synthesised codes.
            if assembly_code:
                for p in parts:
                    count = 0
                    try:
                        assoc_items = StockItem.query.filter_by(parent_code=assembly_code).all()
                        for sitem in assoc_items:
                            dmcode = sitem.datamatrix_code or ''
                            comp_name = None
                            for seg in dmcode.split('|'):
                                if seg.startswith('P='):
                                    comp_name = seg.split('=', 1)[1]
                                    break
                            if comp_name == p.name:
                                count += 1
                    except Exception:
                        count = 0
                    associated_counts[p.id] = count
                # Determine association readiness: all parts must have at least the required number linked
                for p in parts:
                    req = required_quantities.get(p.id, 1)
                    cnt = associated_counts.get(p.id, 0)
                    if cnt < req:
                        assoc_ready = False
                        break
    else:
        # No box context: default required quantity to 1 and zero associations
        for p in parts:
            required_quantities[p.id] = 1
            associated_counts[p.id] = 0
        assoc_ready = True
    # Overall readiness: documents uploaded, parts in stock and associations complete
    ready_all = ready_parts and docs_ready and assoc_ready
    ready = False
    return render_template(
        'inventory/build_assembly.html',
        assembly=assembly,
        parts=parts,
        ready_parts=ready_parts,
        docs_ready=docs_ready,
        ready=ready,
        existing_docs=existing_docs,
        required_docs=required_docs,
        user_docs=user_docs,
        back_url=from_url,
        embedded=embedded_flag,
        required_quantities=required_quantities,
        associated_counts=associated_counts,
        assembly_code=assembly_code,
        assoc_ready=assoc_ready,
        ready_all=ready_all
    )


# -----------------------------------------------------------------------------
# Additional views for the warehouse (magazzino)
#
# The default index view above renders the assembly tree.  For a more complete
# inventory overview operators requested dedicated lists for parts and
# commercial items, as well as an "archive" view that enumerates every
# structure in the system.  Each handler recalculates assembly stock on
# page load to ensure up‑to‑date quantities and then renders a simple list.

@inventory_bp.route('/parts')
@login_required
def list_parts():
    """Display all non‑commercial parts in the warehouse.

    Mechanical parts (flag_part) that are neither assemblies nor commercial
    components are listed here.  Assemblies are excluded since they appear
    on the main dashboard.  The page shows the part name, description,
    category, on‑hand quantity and a link to the production page for
    topping up stock.  The ``active_tab`` variable highlights the
    corresponding tab in the sub‑navigation.
    """
    _recalculate_assemblies()
    parts: List[Structure] = (
        Structure.query
        .filter_by(flag_part=True, flag_assembly=False, flag_commercial=False)
        .order_by(Structure.name.asc())
        .all()
    )
    # Deduplicate parts: parts and commercial components are considered
    # "absolute" items.  Structures that share the same component master
    # (component_id) or, when missing, share the same name represent the
    # same physical part in the warehouse.  To avoid listing duplicates we
    # collapse structures with the same identifier.  The first occurrence
    # encountered during the sorted iteration is retained.
    unique_parts: list[Structure] = []
    seen_keys: set[str | int] = set()
    for struct in parts:
        # Determine a stable key: prefer component_id when present, otherwise
        # fall back to a case‑insensitive name.  Use the structure id to
        # avoid collisions when both are missing, although that scenario is
        # unlikely given the constraints in the data model.
        if struct.component_id:
            key: str | int = struct.component_id
        elif struct.name:
            key = struct.name.strip().lower()
        else:
            key = struct.id
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_parts.append(struct)
    # Attach a display_image_filename attribute to each unique part based on
    # the first ProductComponent that references this structure and has a
    # non‑null image_filename.  Without filtering, `.first()` could return
    # a component without an image even if another component defines one,
    # causing the listing to show a placeholder.  By selecting the first
    # component with a defined image we ensure that uploaded pictures
    # surface in the inventory views.
    for struct in unique_parts:
        try:
            image_filename: str | None = _lookup_structure_image(struct)
        except Exception:
            image_filename = None
        setattr(struct, 'display_image_filename', image_filename)

    return render_template(
        'inventory/parts.html',
        parts=unique_parts,
        active_tab='parts'
    )


@inventory_bp.route('/commercial')
@login_required
def list_commercial():
    """Display all commercial components in the warehouse.

    Commercial components are flagged via ``flag_commercial``.  Assemblies
    are excluded because commercial parts cannot themselves be assemblies.
    The listing mirrors the parts view and provides a link to the
    production page for stock replenishment.
    """
    _recalculate_assemblies()
    parts: List[Structure] = (
        Structure.query
        .filter_by(flag_commercial=True)
        .filter_by(flag_assembly=False)
        .order_by(Structure.name.asc())
        .all()
    )
    # Deduplicate commercial components: treat items with the same
    # component master or, when missing, the same name as a single
    # commercial part.  Retain the first occurrence in the sorted list.
    unique_parts: list[Structure] = []
    seen_keys: set[str | int] = set()
    for struct in parts:
        if struct.component_id:
            key: str | int = struct.component_id
        elif struct.name:
            key = struct.name.strip().lower()
        else:
            key = struct.id
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_parts.append(struct)
    # Attach display_image_filename to commercial parts.  As with the parts
    # listing above, choose the first component with a non‑null image so that
    # uploaded images are displayed instead of placeholders.
    for struct in unique_parts:
        try:
            image_filename: str | None = _lookup_structure_image(struct)
        except Exception:
            image_filename = None
        setattr(struct, 'display_image_filename', image_filename)
    return render_template(
        'inventory/commercial.html',
        parts=unique_parts,
        active_tab='commercial'
    )


@inventory_bp.route('/archive')
@login_required
def archive() -> Any:
    """
    Render the warehouse production history (storico produzione).

    This view lists **only completed builds** recorded in the ``ProductBuild``
    table.  Each build corresponds to one row in the history and is
    identified by the build timestamp, the DataMatrix code of the
    finished product (taken from the associated production box when
    available) and the operator who performed the build.  Expanding a
    build reveals all of its consumed components and subcomponents,
    derived from ``ProductBuildItem`` records; when no explicit
    consumption records exist the current bill of materials (BOM) is
    used as a fallback to populate the tree.  Component scan events are
    **not** displayed in this global history; they appear only in the
    per‑product component archive.

    Returns:
        Rendered HTML page displaying the production history by build.
    """
    from typing import Any, Dict, List
    from ...models import (
        Product, ProductComponent, ProductBuild, ProductBuildItem,
        Structure, BOMLine, StockItem, User, ScanEvent
    )
    # Pre‑load all products into a map keyed by id to minimise DB hits
    product_map: Dict[int, Product] = {}
    try:
        for prod in Product.query.all():
            product_map[prod.id] = prod
    except Exception:
        # On failure leave product_map empty; lookups will fall back to direct queries
        pass

    # Helper: determine a human readable type label for a product based on its
    # root structure.  Assemblies return 'ASSIEME', commercial parts return
    # 'COMMERCIALE' and all others return 'PARTE'.
    def _type_label_for_product(prod: Product) -> str:
        if not prod:
            return ''
        try:
            pc = ProductComponent.query.filter_by(product_id=prod.id).order_by(ProductComponent.id.asc()).first()
        except Exception:
            pc = None
        if pc:
            try:
                struct = Structure.query.get(pc.structure_id)
            except Exception:
                struct = None
            if struct:
                if getattr(struct, 'flag_assembly', False):
                    return 'ASSIEME'
                if getattr(struct, 'flag_commercial', False):
                    return 'COMMERCIALE'
        return 'PARTE'

    # Helper: build a nested component tree for a given ProductBuild.  When
    # explicit ``ProductBuildItem`` records are available they are used;
    # otherwise we fall back to exploding the BOM.  Recursion depth is
    # limited to avoid deep cycles.
    def _component_tree_for_build(pb: ProductBuild, depth: int = 50) -> List[Dict[str, Any]]:
        """
        Build a nested component tree for a given ProductBuild.

        Prefer scanned components (stock items linked via ``parent_code``) when
        available.  When no scanned components exist for the build, fall back
        to explicit ``ProductBuildItem`` records and finally the BOM definition.
        ``depth`` limits the recursion depth when traversing nested assemblies.

        This function has been extended to preserve assembly build history.
        When building a finished product, sub‑assembly ``ProductBuild``
        records are no longer deleted.  To reflect these builds in the
        production history, this helper constructs a tree where each
        assembly node includes its own build timestamp and user.  Nested
        components of an assembly are recursively gathered from
        ``StockItem.parent_code`` relationships.  For parts and commercial
        components the earliest ``ScanEvent`` timestamp is used as before.
        """
        if depth <= 0 or not pb:
            return []
        # Determine the assembly code for this build to look up scanned items
        prod_obj = None
        try:
            prod_obj = product_map.get(pb.product_id)
            if not prod_obj:
                prod_obj = Product.query.get(pb.product_id)
        except Exception:
            prod_obj = None
        asm_code = _assembly_dm(pb, prod_obj) if prod_obj else None
        # If an assembly code exists, gather scanned stock items associated via parent_code
        if asm_code:
            try:
                # Gather stock items associated via parent_code matching the exact
                # DataMatrix code for this build.  Additionally look up items
                # whose parent_code matches a simplified alias formed from the
                # P= and T= segments of the DataMatrix.  When operators scan
                # components using a shortened code (e.g. ``P=<name>|T=PRODOTTO``)
                # instead of the full barcode, their parent_code will not
                # include the DMV1 prefix or serial number.  To ensure these
                # items still appear under the finished product in the
                # production history, search for both variants.  Duplicate
                # entries are removed by deduplicating on stock item id.
                assoc_items: list[StockItem] = []  # initialise list
                # First, try the exact parent_code match
                try:
                    assoc_items = StockItem.query.filter_by(parent_code=asm_code).all()
                except Exception:
                    assoc_items = []
                # Derive an alias code comprised of the P= and T= segments of
                # asm_code.  The DataMatrix format follows ``[prefix|]P=<name>|...|T=<type>[|...]``.
                # Split by '|' and extract the P= and T= parts to form a
                # simplified code.  Use this alias to find items that were
                # associated using a shortened parent_code.  Only perform
                # this secondary lookup when the alias differs from the full
                # asm_code to avoid duplicate queries.
                try:
                    name_seg = None
                    type_seg = None
                    for seg in asm_code.split('|'):
                        seg_stripped = seg.strip()
                        if seg_stripped.upper().startswith('P=') and not name_seg:
                            name_seg = seg_stripped.split('=', 1)[1]
                        if seg_stripped.upper().startswith('T=') and not type_seg:
                            type_seg = seg_stripped.split('=', 1)[1]
                        if name_seg and type_seg:
                            break
                    alias_code = None
                    if name_seg and type_seg:
                        alias_code = f"P={name_seg}|T={type_seg}"
                    if alias_code and alias_code != asm_code:
                        try:
                            alias_items = StockItem.query.filter_by(parent_code=alias_code).all()
                        except Exception:
                            alias_items = []
                        if alias_items:
                            existing_ids = {getattr(si, 'id', None) for si in (assoc_items or [])}
                            for si in alias_items:
                                sid = getattr(si, 'id', None)
                                if sid is not None and sid not in existing_ids:
                                    assoc_items.append(si)
                except Exception:
                    pass
            except Exception:
                # If the lookup fails entirely, treat as no associated items
                assoc_items = []
            # -----------------------------------------------------------------
            # New helper to build a nested tree preserving assembly build info.
            # This helper walks the StockItem.parent_code hierarchy and
            # constructs nodes.  Assembly nodes include the timestamp and
            # operator from the most recent ProductBuild prior to the parent
            # build.  Non‑assembly nodes use the earliest ScanEvent timestamp.
            # ``final_ts`` is the timestamp of the parent build (i.e. the
            # finished product build).  Assemblies built after this timestamp
            # are ignored when searching for the associated ProductBuild.
            def _build_tree_from_parent_code(parent_code: str, max_depth: int, final_ts: Any) -> List[Dict[str, Any]]:
                if max_depth <= 0:
                    return []
                nodes: List[Dict[str, Any]] = []
                try:
                    child_items: list[StockItem] = []
                    try:
                        child_items = StockItem.query.filter_by(parent_code=parent_code).all()
                    except Exception:
                        child_items = []
                    # Derive alias for parent_code to include items associated via simplified codes
                    try:
                        name_seg = None
                        type_seg = None
                        for seg in parent_code.split('|'):
                            seg_stripped = seg.strip()
                            if seg_stripped.upper().startswith('P=') and not name_seg:
                                name_seg = seg_stripped.split('=', 1)[1]
                            if seg_stripped.upper().startswith('T=') and not type_seg:
                                type_seg = seg_stripped.split('=', 1)[1]
                            if name_seg and type_seg:
                                break
                        alias_code = None
                        if name_seg and type_seg:
                            alias_code = 'P=' + name_seg + '|T=' + type_seg
                        if alias_code and alias_code != parent_code:
                            try:
                                alias_items = StockItem.query.filter_by(parent_code=alias_code).all()
                            except Exception:
                                alias_items = []
                            if alias_items:
                                existing_ids = {getattr(si, 'id', None) for si in (child_items or [])}
                                for si in alias_items:
                                    sid = getattr(si, 'id', None)
                                    if sid is not None and sid not in existing_ids:
                                        child_items.append(si)
                    except Exception:
                        pass
                except Exception:
                    child_items = []
                # Compute parent component and type segments once for the current parent_code
                parent_comp: str | None = None
                parent_type: str | None = None
                try:
                    for seg in (parent_code or '').split('|'):
                        seg_clean = seg.strip()
                        if seg_clean.upper().startswith('P=') and parent_comp is None:
                            parent_comp = seg_clean.split('=', 1)[1]
                            continue
                        if seg_clean.upper().startswith('T=') and parent_type is None:
                            parent_type = seg_clean.split('=', 1)[1]
                            continue
                        if parent_comp is not None and parent_type is not None:
                            break
                except Exception:
                    parent_comp = None
                    parent_type = None
                # Derive a simplified alias of the parent code using only the P= and T= segments.
                alias_parent: str | None = None
                if parent_comp is not None and parent_type is not None:
                    alias_parent = f"P={parent_comp}|T={parent_type}"
                for si in child_items or []:
                    # Extract the child's DataMatrix code
                    try:
                        dm_current = getattr(si, 'datamatrix_code', '') or ''
                    except Exception:
                        dm_current = ''
                    # Extract the child's P= and T= segments
                    child_comp: str | None = None
                    child_type: str | None = None
                    try:
                        for seg_c in (dm_current or '').split('|'):
                            seg_clean_c = seg_c.strip()
                            if seg_clean_c.upper().startswith('P=') and child_comp is None:
                                child_comp = seg_clean_c.split('=', 1)[1]
                                continue
                            if seg_clean_c.upper().startswith('T=') and child_type is None:
                                child_type = seg_clean_c.split('=', 1)[1]
                                continue
                            if child_comp is not None and child_type is not None:
                                break
                    except Exception:
                        child_comp = None
                        child_type = None
                    # Skip when the child refers to the same component name as the parent.  Use
                    # case-insensitive comparison to handle mismatched casing.  Matching only on
                    # the ``P`` segment ensures that any stock item representing the finished
                    # product (even with a different type such as ASSIEME/PRODOTTO) is not
                    # included as a child, preventing repeated product names in the tree.
                    if (parent_comp and child_comp and
                        child_comp.strip().upper() == parent_comp.strip().upper()):
                        continue
                    # Also skip when the child's full DataMatrix code exactly matches the parent_code
                    # or its simplified alias.  This covers cases where the association was recorded
                    # using either the complete or shortened code but lacks distinct P/T segments.
                    if dm_current and (dm_current == parent_code or (alias_parent and dm_current == alias_parent)):
                        continue
                    # Resolve product.  Stock items are linked to the parent product via
                    # ``product_id`` but this does not reflect the actual component name.
                    # Retrieve the product record when possible for description purposes,
                    # but derive the display name and type from the DataMatrix code.
                    child_prod = None
                    cid = None
                    try:
                        cid = getattr(si, 'product_id', None)
                    except Exception:
                        cid = None
                    if cid:
                        child_prod = product_map.get(cid)
                        if not child_prod:
                            try:
                                child_prod = Product.query.get(cid)
                            except Exception:
                                child_prod = None
                    # Determine the display name (component) and type label from the
                    # DataMatrix payload.  When the DataMatrix is missing or does not
                    # include P/T segments, fall back to the product name and type.
                    name_val = child_comp if child_comp else (getattr(child_prod, 'name', '') if child_prod else '')
                    tlabel = None
                    if child_type:
                        tlabel = child_type.upper()
                    else:
                        # Fallback to type label based on the product's root structure
                        tlabel = _type_label_for_product(child_prod) if child_prod else 'PARTE'
                    # Determine DataMatrix code.  Prefer the stock item's own code; if
                    # missing, construct one from the derived name and type.  This
                    # synthetic code retains the association to the child component
                    # without referencing the parent product name.
                    dm_code = None
                    try:
                        dm_code = getattr(si, 'datamatrix_code', None)
                    except Exception:
                        dm_code = None
                    if not dm_code:
                        dm_code = f"P={name_val}|T={tlabel}"
                    # Initialise timestamp and user
                    timestamp_val = None
                    user_val = ''
                    # Decide whether this child is an assembly based on the type label.
                    # Use the derived label rather than the product-based type to ensure
                    # assemblies identified via DataMatrix are exploded correctly.
                    if tlabel and tlabel.upper() == 'ASSIEME':
                        # For assemblies, prefer ProductBuild record for this subassembly
                        # that occurred prior to the parent build timestamp.  Use the
                        # most recent such build.
                        try:
                            sub_pb = (
                                ProductBuild.query
                                .filter_by(product_id=cid)
                                .filter(ProductBuild.created_at <= final_ts)
                                .order_by(ProductBuild.created_at.desc())
                                .first()
                            )
                        except Exception:
                            sub_pb = None
                        if sub_pb:
                            try:
                                timestamp_val = getattr(sub_pb, 'created_at', None)
                            except Exception:
                                timestamp_val = None
                            try:
                                uid_val = getattr(sub_pb, 'user_id', None)
                            except Exception:
                                uid_val = None
                            if uid_val:
                                user_val = _user_display(uid_val)
                        # Build subcomponents recursively
                        sub_nodes = _build_tree_from_parent_code(dm_code, max_depth - 1, final_ts)
                    else:
                        # Non‑assembly: use earliest ScanEvent for timestamp and user
                        try:
                            se = (
                                ScanEvent.query
                                .filter_by(datamatrix_code=dm_code)
                                .order_by(ScanEvent.created_at.asc())
                                .first()
                            )
                        except Exception:
                            se = None
                        if se:
                            try:
                                timestamp_val = getattr(se, 'created_at', None)
                            except Exception:
                                timestamp_val = None
                            try:
                                import json as _json  # local import
                                meta_dict = _json.loads(se.meta) if se.meta else {}
                            except Exception:
                                meta_dict = {}
                            uid = None
                            try:
                                uid_candidates = (
                                    meta_dict.get('user_id'),
                                    meta_dict.get('user'),
                                    meta_dict.get('user_username'),
                                    meta_dict.get('username'),
                                    meta_dict.get('operator'),
                                )
                                for candidate in uid_candidates:
                                    if candidate:
                                        uid = candidate
                                        break
                            except Exception:
                                uid = None

                            if uid:
                                try:
                                    uid_int = int(uid)
                                except Exception:
                                    uid_int = None

                                try:
                                    user_val = _user_display(uid_int if uid_int is not None else uid)
                                except Exception:
                                    user_val = ''
                        sub_nodes = []
                    nodes.append({
                        'timestamp': timestamp_val,
                        'datamatrix': dm_code,
                        'name': name_val,
                        'description': getattr(child_prod, 'description', None) or '' if child_prod else '',
                        'user': user_val or None,
                        # Include the product identifier when known.  This
                        # facilitates construction of document links in the
                        # global production history.  When the associated
                        # product cannot be resolved, set to None.
                        'product_id': cid if cid is not None else None,
                        'docs': [],
                        'components': sub_nodes
                    })
                return nodes
            # If any associated items exist, build the tree using the new helper
            if assoc_items:
                return _build_tree_from_parent_code(asm_code, depth, getattr(pb, 'created_at', None))
        # When no scanned items exist, fall back to explicit ProductBuildItem records
        nodes: List[Dict[str, Any]] = []
        try:
            build_items = ProductBuildItem.query.filter_by(build_id=pb.id).all()
        except Exception:
            build_items = []
        if build_items:
            for bitem in build_items:
                # Skip self-referential entries.  A product build item
                # referencing the same product as the parent build would
                # erroneously display the product as its own component.  Such
                # entries can occur when fallback logic derives children
                # from structures rather than products.  Only include
                # build items whose product_id differs from the parent.
                try:
                    cid = getattr(bitem, 'product_id', None)
                    if cid is not None and cid == getattr(pb, 'product_id', None):
                        continue
                except Exception:
                    pass
                child_prod = None
                try:
                    cid_val = getattr(bitem, 'product_id', None)
                except Exception:
                    cid_val = None
                if cid_val:
                    child_prod = product_map.get(cid_val)
                    if not child_prod:
                        try:
                            child_prod = Product.query.get(cid_val)
                        except Exception:
                            child_prod = None
                if not child_prod:
                    continue
                tlabel = _type_label_for_product(child_prod)
                dm_code = f"P={child_prod.name}|T={tlabel}"
                sub_nodes: List[Dict[str, Any]] = []
                # Only explode BOM for assemblies
                if tlabel == 'ASSIEME':
                    sub_nodes = _bom_tree(child_prod.id, depth - 1)
                nodes.append({
                    'timestamp': None,
                    'datamatrix': dm_code,
                    'name': child_prod.name,
                    'description': getattr(child_prod, 'description', None) or '',
                    'user': None,
                    # Persist the product id for this component.  This aids in
                    # constructing document links in the global history view.
                    'product_id': cid_val if cid_val is not None else None,
                    'docs': [],
                    'components': sub_nodes
                })
            return nodes
        # Final fallback: derive components from BOM when no build items recorded
        return _bom_tree(getattr(pb, 'product_id', None), depth)

    # Helper: explode BOM into a tree for a given product id.  Returns a list
    # of component nodes with synthetic DataMatrix codes.  Recursion depth
    # prevents infinite cycles.
    def _bom_tree(prod_id: int | None, depth: int = 50) -> List[Dict[str, Any]]:
        if not prod_id or depth <= 0:
            return []
        children: List[Dict[str, Any]] = []
        lines = []
        try:
            lines = BOMLine.query.filter_by(padre_id=prod_id).all()
        except Exception:
            lines = []
        for line in lines or []:
            child_id = None
            try:
                child_id = getattr(line, 'figlio_id', None)
            except Exception:
                child_id = None
            if not child_id:
                continue
            child_prod = product_map.get(child_id)
            if not child_prod:
                try:
                    child_prod = Product.query.get(child_id)
                except Exception:
                    child_prod = None
            if not child_prod:
                continue
            tlabel = _type_label_for_product(child_prod)
            dm_code = f"P={child_prod.name}|T={tlabel}"
            sub: List[Dict[str, Any]] = []
            if tlabel == 'ASSIEME':
                sub = _bom_tree(child_prod.id, depth - 1)
            children.append({
                'timestamp': None,
                'datamatrix': dm_code,
                'name': child_prod.name,
                'description': getattr(child_prod, 'description', None) or '',
                'user': None,
                # Expose the child product id so that document links can be constructed
                'product_id': getattr(child_prod, 'id', None),
                'docs': [],
                'components': sub
            })
        return children

    # Helper: build DataMatrix code for a ProductBuild.  Prefer the code of
    # a stock item in the associated box that is labelled as an assembly or
    # product.  Otherwise synthesise a code using the product name and
    # 'PRODOTTO'.
    def _assembly_dm(pb: ProductBuild, prod: Product) -> str:
        # Prefer a DataMatrix code labelled as a finished product (T=PRODOTTO).
        # Fallback to an assembly code (T=ASSIEME) only when no product code
        # exists.  When neither code is present in the production box,
        # synthesise a product code using the product name.  This ensures
        # that finished products built without a scanned DataMatrix are
        # labelled consistently and avoids accidentally selecting a sub‑assembly
        # code for the parent build.  Assemblies built via the assembly
        # workflow will still return their scanned assembly code since no
        # product code will be present in their production box.
        asm_code = f"P={prod.name}|T=PRODOTTO"
        if not pb or not prod:
            return asm_code
        try:
            # Use the production box id attached to this build to look up scanned
            # stock items.  Prefer a DataMatrix code that corresponds to the
            # same product as the build and contains ``T=PRODOTTO``.  When
            # such a code cannot be found, avoid selecting a code from a
            # different product (which could happen if multiple finished
            # products were loaded into the same box).  Only fall back to
            # an assembly code for this product when no finished product code
            # exists.  Finally, when neither code is present, return a
            # synthetic ``P=<product.name>|T=PRODOTTO`` payload to clearly
            # identify the build as a finished product.
            box_id = getattr(pb, 'production_box_id', None)
            items: List[StockItem] = []
            if box_id:
                items = StockItem.query.filter_by(production_box_id=box_id).all()
            # Search for a finished product code matching this product
            for si in items or []:
                try:
                    dm = getattr(si, 'datamatrix_code', '') or ''
                except Exception:
                    dm = ''
                if not dm:
                    continue
                try:
                    pid = getattr(si, 'product_id', None)
                except Exception:
                    pid = None
                if pid and prod and pid == prod.id and 'T=PRODOTTO' in dm.upper():
                    asm_code = dm
                    break
            else:
                # No matching finished product code found; look for an assembly code
                for si in items or []:
                    try:
                        dm = getattr(si, 'datamatrix_code', '') or ''
                    except Exception:
                        dm = ''
                    if not dm:
                        continue
                    try:
                        pid = getattr(si, 'product_id', None)
                    except Exception:
                        pid = None
                    if pid and prod and pid == prod.id and 'T=ASSIEME' in dm.upper():
                        asm_code = dm
                        break
        except Exception:
            # Leave asm_code unchanged on failure; fallback to synthetic code below
            pass
        return asm_code

    # Helper: resolve user display string from a user_id.  Prefers username and
    # falls back to legacy email, id or empty string.  When uid is falsy the
    # result is empty.
    def _user_display(uid: Any) -> str:
        if not uid:
            return ''
        try:
            usr = User.query.get(uid)
            if usr:
                username = getattr(usr, 'username', None)
                if username:
                    return username
                legacy_email = getattr(usr, 'email', None)
                if legacy_email:
                    return legacy_email
                return str(usr.id)
        except Exception:
            pass
        return ''

    # Helper: parse a DataMatrix code into component and type segments.
    def _parse_dm(dm: str) -> Dict[str, str]:
        result: Dict[str, str] = {}
        if not dm:
            return result
        for segment in dm.split('|'):
            if '=' not in segment:
                continue
            key, val = segment.split('=', 1)
            if key == 'P':
                result['component'] = val
            elif key == 'T':
                result['type'] = val
        return result

    # Build rows: each build (ProductBuild) becomes a top-level node.  Only
    # completed builds are displayed; component scan events are excluded.  Rows
    # are sorted in descending chronological order.  Each node includes
    # its consumed components (and subcomponents) retrieved from
    # ProductBuildItem records or, when absent, from the BOM.
    build_nodes: List[Dict[str, Any]] = []
    # Identify assemblies that have been consumed as components of higher‑level
    # builds.  When a stock item has a non‑null parent_code the corresponding
    # assembly should no longer appear as a top‑level row in the global
    # archive.  Record both the full DataMatrix and a simplified P/T
    # representation for robust matching.
    consumed_codes: set[str] = set()
    try:
        assoc_items = StockItem.query.filter(StockItem.parent_code.isnot(None)).all()
    except Exception:
        assoc_items = []
    for si in assoc_items:
        try:
            dm = si.datamatrix_code or ''
        except Exception:
            dm = ''
        if not dm:
            continue
        consumed_codes.add(dm)
        # Also record simplified P/T variants to match synthetic codes
        try:
            comp_val = None
            typ_val = None
            for seg in dm.split('|'):
                if seg.startswith('P='):
                    comp_val = seg.split('=', 1)[1]
                elif seg.startswith('T='):
                    typ_val = seg.split('=', 1)[1]
            if comp_val and typ_val:
                consumed_codes.add(f"P={comp_val}|T={typ_val}")
        except Exception:
            pass
    # Fetch all builds sorted by timestamp descending
    all_builds: List[ProductBuild] = []
    try:
        all_builds = (
            ProductBuild.query
            .order_by(ProductBuild.created_at.desc())
            .all()
        )
    except Exception:
        all_builds = []
    # Build a set of product ids that appear as children in any BOM line.  These
    # represent sub‑assemblies or intermediate products that are consumed
    # when building a finished product.  We exclude builds where the
    # product belongs to this set so that only final product builds
    # appear in the production history.  See issue: assemblies should not
    # create top‑level entries in storico produzione.
    child_product_ids: set[int] = set()
    try:
        child_lines = BOMLine.query.all()
    except Exception:
        child_lines = []
    for bl in child_lines or []:
        try:
            cid = getattr(bl, 'figlio_id', None)
        except Exception:
            cid = None
        if cid:
            try:
                child_product_ids.add(int(cid))
            except Exception:
                try:
                    child_product_ids.add(cid)
                except Exception:
                    pass

    for pb in all_builds:
        # Resolve product for this build
        prod_obj = None
        if pb and pb.product_id:
            prod_obj = product_map.get(pb.product_id)
            if not prod_obj:
                try:
                    prod_obj = Product.query.get(pb.product_id)
                except Exception:
                    prod_obj = None
        if not prod_obj:
            continue
        # Determine the DataMatrix code for this build early.  When a finished
        # product is constructed via the ``build_product`` route, its
        # DataMatrix will contain ``T=PRODOTTO``.  Assemblies built via
        # sub‑assembly workflows typically have ``T=ASSIEME`` or may omit
        # the type entirely.  By computing the DataMatrix ahead of time we
        # can decide whether to treat this build as a final product even
        # when the underlying root structure is flagged as an assembly or
        # appears in the BOM as a child.  This allows finished products to
        # appear in the global production history while still filtering
        # out intermediate assemblies.
        try:
            dm_code = _assembly_dm(pb, prod_obj)
        except Exception:
            dm_code = None
        dm_upper = dm_code.upper() if dm_code else ''
        is_final_product = 'T=PRODOTTO' in dm_upper
        # Determine the root structure associated with this product.  When
        # flag_assembly is True the product is treated as an assembly unless
        # the computed DataMatrix indicates a finished product (T=PRODOTTO).
        try:
            pc_root = ProductComponent.query.filter_by(product_id=prod_obj.id).order_by(ProductComponent.id.asc()).first()
        except Exception:
            pc_root = None
        root_struct = None
        if pc_root:
            try:
                root_struct = Structure.query.get(pc_root.structure_id)
            except Exception:
                root_struct = None
        try:
            if root_struct and getattr(root_struct, 'flag_assembly', False) and not is_final_product:
                # This build represents a sub‑assembly and no override was
                # provided by the DataMatrix.  Skip it to avoid cluttering
                # the production history with intermediate assemblies.
                continue
        except Exception:
            pass
        # Skip builds for products that are children in any BOM line.  These
        # correspond to assemblies used within other products and should not
        # appear as top‑level entries in the global production history.
        # However, when the DataMatrix specifies a finished product (T=PRODOTTO)
        # we override this rule to include the build.
        try:
            if prod_obj.id in child_product_ids and not is_final_product:
                continue
        except Exception:
            # Fallback: attempt to cast product id to int and compare
            try:
                if int(getattr(prod_obj, 'id', -1)) in child_product_ids and not is_final_product:
                    continue
            except Exception:
                pass
        # Skip builds whose assembly code appears in the consumed set.  When
        # an assembly has been linked to a parent via ``parent_code`` it
        # should not appear as a top‑level entry in the global archive.
        if dm_code and dm_code in consumed_codes:
            continue
        # Build component tree (explicit consumption preferred)
        # Increase the recursion depth to allow unlimited nesting of components.
        # A high value (e.g. 50) ensures that deeply nested assemblies are fully
        # expanded in the production history without cutting off at three levels.
        comps = _component_tree_for_build(pb, depth=50)
        user_disp = _user_display(getattr(pb, 'user_id', None))
        build_nodes.append({
            'timestamp': getattr(pb, 'created_at', None),
            'datamatrix': dm_code,
            'name': prod_obj.name,
            'description': getattr(prod_obj, 'description', None) or '',
            'user': user_disp,
            # Persist the product id for downstream docs lookup.  This allows
            # the template to construct document links without needing to
            # reverse‑engineer the product from the DataMatrix payload.  When
            # this key is missing the docs link may be disabled.
            'product_id': getattr(pb, 'product_id', None),
            # Include the production box id to scope document retrieval to this build
            'box_id': getattr(pb, 'production_box_id', None),
            'docs': [],
            'components': comps
        })
    # -----------------------------------------------------------------
    # Augment each build and its component nodes with revision labels.  The
    # global production history draws component names from ProductBuild and
    # ProductBuildItem records, which do not themselves persist revision
    # information.  To present the revision of each component we look up the
    # current Structure by name.  When revisions were stored at the time of
    # the build via ScanEvent meta, those values should be used instead.
    from ...models import Structure, ScanEvent  # ensure Structure and ScanEvent are available
    import json as _json
    def _add_revision_nodes(nodes: list[dict[str, Any]]) -> None:
        """
        Attach a revision label to each node in the production history.

        Prefer the revision stored in the earliest ScanEvent meta for the
        component's DataMatrix code when available.  When no such event
        exists or lacks revision data, fall back to the current Structure
        revision.  Recursively update nested component nodes.
        """
        for node in nodes or []:
            revision_val = ''
            # Determine the DataMatrix code associated with this node
            dm_code = None
            try:
                dm_code = node.get('datamatrix')
            except Exception:
                dm_code = None
            # Attempt to fetch revision from earliest ScanEvent meta
            if dm_code:
                try:
                    se = (
                        ScanEvent.query
                        .filter_by(datamatrix_code=dm_code)
                        .order_by(ScanEvent.id.asc())
                        .first()
                    )
                except Exception:
                    se = None
                if se:
                    try:
                        meta_val = se.meta or ''
                        meta_dict = _json.loads(meta_val) if meta_val else {}
                        rev_from_meta = meta_dict.get('revision_label') or ''
                        if rev_from_meta:
                            revision_val = rev_from_meta
                    except Exception:
                        revision_val = ''
            # Fallback: derive revision from current Structure when no meta revision found
            if not revision_val:
                try:
                    nm = node.get('name')
                except Exception:
                    nm = None
                if nm:
                    try:
                        s_obj = Structure.query.filter_by(name=nm).first()
                    except Exception:
                        s_obj = None
                    if s_obj:
                        try:
                            revision_val = s_obj.revision_label or ''
                        except Exception:
                            revision_val = ''
            # Assign revision to node
            try:
                node['revision'] = revision_val or ''
            except Exception:
                pass
            # Recurse into nested components
            try:
                children = node.get('components')
            except Exception:
                children = None
            if children:
                _add_revision_nodes(children)
    try:
        _add_revision_nodes(build_nodes)
    except Exception:
        pass
    # Render the history.  The template will iterate through each build node
    # as a top‑level entry.  When no builds exist the history will show
    # a placeholder message defined in the template.
    return render_template('inventory/archive.html', assemblies=build_nodes, active_tab='archive')


@inventory_bp.route('/archive/download')
@login_required
def download_archive():
    """Generate and return a CSV containing the complete build archive.

    This endpoint exports every recorded assembly build along with its
    associated components.  When no build data exists the CSV will
    include only a header row.  Each row in the CSV represents one
    component consumed in a build and includes the build identifier,
    product name, build timestamp, quantity built, the DataMatrix code
    assigned to the assembly and the component details (product name,
    DataMatrix code and quantity).

    Returns:
        A Flask response with ``text/csv`` content type and an
        appropriate ``Content-Disposition`` header to prompt download.
    """
    import csv
    import io
    from datetime import datetime
    from ...models import ProductBuild, ProductBuildItem, BOMLine, Product, StockItem
    # Prepare an in-memory buffer for the CSV
    output = io.StringIO()
    writer = csv.writer(output)
    # Write header
    writer.writerow([
        'build_id', 'product_id', 'product_name', 'build_timestamp', 'quantity_built',
        'assembly_datamatrix', 'component_product_id', 'component_product_name',
        'component_datamatrix', 'component_quantity'
    ])
    # Fetch all builds
    try:
        builds = ProductBuild.query.order_by(ProductBuild.created_at.asc()).all()
    except Exception:
        builds = []
    # Pre-cache product names
    prod_map: dict[int, str] = {}
    try:
        for p in Product.query.all():
            prod_map[p.id] = p.name
    except Exception:
        pass
    # Iterate through each build and generate rows
    for pb in builds:
        # Determine assembly DataMatrix
        asm_code = f"P={prod_map.get(pb.product_id, '')}|T=ASSIEME"
        try:
            if getattr(pb, 'production_box_id', None):
                items = StockItem.query.filter_by(production_box_id=pb.production_box_id).all()
                for si in items:
                    dm = si.datamatrix_code or ''
                    if 'T=ASSIEME' in dm.upper():
                        asm_code = dm
                        break
        except Exception:
            pass
        # Gather associated component stock items via parent_code
        try:
            assoc_items = StockItem.query.filter_by(parent_code=asm_code).all()
        except Exception:
            assoc_items = []
        rows_added = False
        if assoc_items:
            # Group by product_id and DataMatrix to count duplicates
            from collections import defaultdict
            grouping: dict[tuple[int, str], int] = defaultdict(int)
            for si in assoc_items:
                if not si or not si.product_id:
                    continue
                key = (si.product_id, si.datamatrix_code or '')
                grouping[key] += 1
            for (cid, dm_val), qty in grouping.items():
                writer.writerow([
                    pb.id,
                    pb.product_id,
                    prod_map.get(pb.product_id, ''),
                    pb.created_at.strftime('%Y-%m-%d %H:%M:%S') if pb.created_at else '',
                    pb.qty,
                    asm_code,
                    cid,
                    prod_map.get(cid, ''),
                    dm_val,
                    qty
                ])
                rows_added = True
        else:
            # Fallback to ProductBuildItem
            try:
                items = ProductBuildItem.query.filter_by(build_id=pb.id).all()
            except Exception:
                items = []
            if items:
                for it in items:
                    cid = it.product_id
                    qty_required = it.quantity_required
                    comp_name = prod_map.get(cid, '')
                    comp_dm = f"P={comp_name}|T=PART"
                    writer.writerow([
                        pb.id,
                        pb.product_id,
                        prod_map.get(pb.product_id, ''),
                        pb.created_at.strftime('%Y-%m-%d %H:%M:%S') if pb.created_at else '',
                        pb.qty,
                        asm_code,
                        cid,
                        comp_name,
                        comp_dm,
                        qty_required
                    ])
                    rows_added = True
            else:
                # Fallback to BOM definition
                try:
                    bom_lines = BOMLine.query.filter_by(padre_id=pb.product_id).all()
                except Exception:
                    bom_lines = []
                for line in bom_lines:
                    cid = line.figlio_id
                    qty_required = line.quantita or 1
                    comp_name = prod_map.get(cid, '')
                    comp_dm = f"P={comp_name}|T=PART"
                    writer.writerow([
                        pb.id,
                        pb.product_id,
                        prod_map.get(pb.product_id, ''),
                        pb.created_at.strftime('%Y-%m-%d %H:%M:%S') if pb.created_at else '',
                        pb.qty,
                        asm_code,
                        cid,
                        comp_name,
                        comp_dm,
                        qty_required
                    ])
                    rows_added = True
        if not rows_added:
            # Still write a row representing the build with no components
            writer.writerow([
                pb.id,
                pb.product_id,
                prod_map.get(pb.product_id, ''),
                pb.created_at.strftime('%Y-%m-%d %H:%M:%S') if pb.created_at else '',
                pb.qty,
                asm_code,
                '', '', '', ''
            ])
    # Create response
    csv_data = output.getvalue()
    output.close()
    return current_app.response_class(
        csv_data,
        mimetype='text/csv',
        headers={
            'Content-Disposition': 'attachment; filename=archive_completo.csv'
        }
    )


# -----------------------------------------------------------------------------
# Component load (parts and commercial components)
#
# When an operator clicks "Carica" on a part or commercial component in the
# warehouse listing, they are taken to this view.  The page lists any
# documents that have been flagged in the checklist for that structure and
# allows the operator to upload compiled versions before increasing the
# stock quantity.  The workflow mirrors the build assembly interface but
# operates on a single component.  After completion the user is redirected
# back to the originating list with the updated row highlighted.

@inventory_bp.route('/load/<int:part_id>', methods=['GET', 'POST'])
@login_required
def load_component(part_id: int):
    """Display and process the load component form for a single part/commercial.

    The handler shows any flagged documents for the given structure and
    requires the operator to upload compiled versions of those documents.
    Upon successful submission the specified quantity is added to the
    ``quantity_in_stock`` for all matching structures (same component_id or
    name) and the compiled documents are copied into the appropriate
    directories.  A log entry is recorded and the user is redirected
    back to the previous page with the updated component highlighted.
    """
    part = Structure.query.get_or_404(part_id)
    # ---------------------------------------------------------------------
    # Attach an image to the part for the load component popup.
    #
    # Several listing views (parts, commercial, product parts) attach a
    # ``display_image_filename`` attribute to each Structure instance
    # using a helper function to locate the most appropriate uploaded
    # image.  The load component view originally omitted this step,
    # resulting in a missing or incorrect image when the form was
    # rendered in a modal.  To provide a consistent experience and to
    # satisfy the requirement of displaying the component image in the
    # load popup, compute the best available image here and attach it to
    # the ``part`` object.  If no image is found the attribute will be
    # left as ``None`` and the template will display a placeholder icon.
    try:
        image_filename: str | None = _lookup_structure_image(part)
    except Exception:
        image_filename = None
    # Set the attribute on the instance to enable Jinja to resolve it.
    setattr(part, 'display_image_filename', image_filename)
    # Determine whether the form should be rendered inside an embedded modal.
    # When the 'embedded' query parameter is present the template hides the
    # application header, navigation and footer so that only the form contents
    # appear within the popup.  A 'box_id' query parameter may also be provided
    # when loading a component from a production box; this identifier is passed
    # through to the template to enable client‑side API calls after upload.
    embedded_flag = bool(request.args.get('embedded'))
    box_id_param = request.args.get('box_id')
    # When loading from a production box, a stock item identifier may
    # be provided to indicate that a single item (rather than the entire
    # box) should be marked as loaded.  Pass this through to the
    # template to adjust behaviour.
    item_id_param = request.args.get('item_id')
    # Determine where to return after completion.  Use query parameter, then
    # referrer, otherwise fall back to the appropriate list page based on
    # component type.
    back_url = request.args.get('back_url') or request.referrer
    if not back_url:
        # Choose default list based on flags
        if getattr(part, 'flag_part', False):
            back_url = url_for('inventory.list_parts')
        elif getattr(part, 'flag_commercial', False):
            back_url = url_for('inventory.list_commercial')
        else:
            back_url = url_for('inventory.index')
    # Load flagged documents for this structure
    try:
        checklist_data = load_checklist()
    except Exception:
        checklist_data = {}
    flagged_paths: list[str] = checklist_data.get(str(part.id), []) if isinstance(checklist_data, dict) else []
    # Keep only string paths.  Include any prefix (e.g. documents or tmp_components)
    # so that all flagged documents appear in the load interface.  The
    # `products.download_file` endpoint will enforce that the resolved path
    # remains within the static directory, preventing traversal outside
    # of permitted locations.
    flagged_paths = [p for p in flagged_paths if isinstance(p, str)]
    # Mapping of document types to human-readable labels
    doc_label_map: dict[str, str] = {
        'qualita': 'Modulo Cert. qualità',
        '3_1_materiale': '3.1 Materiale',
        'step_tavole': 'Step/tavola',
        'funzionamento': 'Verifica funzionamento',
        'istruzioni': 'Montaggio istruzioni',
        'ddt_fornitore': 'DDT fornitore',
        'altro': 'Altro'
    }
    # Build document definitions from flagged paths.  Group by type and
    # include multiple entries when different documents belong to the same
    # category.  Each definition includes a unique form field name and the
    # original relative path used for downloading.
    doc_defs: list[dict[str, str]] = []
    idx_counter = 1
    for path in flagged_paths:
        # Determine document type from path segment
        doc_type = 'altro'
        norm = path.replace('\\', '/').lower()
        for key in doc_label_map.keys():
            if f"/{key}/" in norm:
                doc_type = key
                break
        # Field name must be unique for each doc
        field_name = f"{doc_type}_{part.id}_{idx_counter}"
        idx_counter += 1
        # Build a display label including the file name to help users
        file_name = os.path.basename(path)
        display_name = f"{doc_label_map.get(doc_type, doc_type)} ({file_name})"
        doc_defs.append({
            'type': doc_type,
            'field_name': field_name,
            'original_path': path,
            'display_name': display_name
        })

    # -------------------------------------------------------------------------
    # When no documents are flagged for this component, attempt to surface
    # previously uploaded documents stored under the component's directory in
    # the static ``documents`` folder.  This ensures that parts without
    # checklist entries still present a "carica/scarica" interface similar to
    # commercial components.  We scan ``static/documents/<component>/<type>``
    # and create a doc_def entry for each file discovered.  Only execute this
    # logic when no flagged docs exist to avoid duplicating checklist items.
    # Each resulting doc_def uses a unique field name and references the
    # existing file for the download link.  If multiple files are present
    # within the same doc_type folder, individual entries are created for
    # each file.
    if not doc_defs:
        try:
            safe_name = secure_filename(part.name) or f"id_{part.id}"
            doc_root = os.path.join(current_app.static_folder, 'documents', safe_name)
            if os.path.isdir(doc_root):
                # Iterate over document type folders
                for dt_name in sorted(os.listdir(doc_root)):
                    dt_path = os.path.join(doc_root, dt_name)
                    if not os.path.isdir(dt_path):
                        continue
                    for fname in sorted(os.listdir(dt_path)):
                        file_path = os.path.join(dt_path, fname)
                        if not os.path.isfile(file_path):
                            continue
                        # Build relative path for download (relative to static folder)
                        try:
                            rel_path = os.path.relpath(file_path, current_app.static_folder)
                        except Exception:
                            # Fallback: use filename only
                            rel_path = os.path.join('documents', safe_name, dt_name, fname)
                        # Determine human readable label and unique field name
                        doc_type = dt_name.lower()
                        field_name = f"{doc_type}_{part.id}_{idx_counter}"
                        idx_counter += 1
                        display_name = f"{doc_label_map.get(doc_type, doc_type)} ({fname})"
                        doc_defs.append({
                            'type': doc_type,
                            'field_name': field_name,
                            'original_path': rel_path,
                            'display_name': display_name
                        })
        except Exception:
            # If scanning fails, leave doc_defs unchanged
            pass
    if request.method == 'POST':
        # Parse quantity; default to 1 if missing or invalid
        qty_str = request.form.get('quantity', '1')
        try:
            qty = float(qty_str)
            if qty <= 0:
                raise ValueError
        except Exception:
            flash('La quantità deve essere un numero positivo.', 'warning')
            return render_template(
                'inventory/load_component.html',
                part=part,
                doc_defs=doc_defs,
                doc_label_map=doc_label_map,
                back_url=back_url,
                embedded=embedded_flag,
                box_id=box_id_param,
                docs_ready=len(doc_defs) == 0
            )
        # Validate that all required documents are uploaded
        missing = []
        for doc_def in doc_defs:
            field = doc_def['field_name']
            file_obj = request.files.get(field)
            if not file_obj or not file_obj.filename:
                missing.append(doc_def['display_name'])
        if missing:
            flash('Carica tutti i documenti richiesti prima di procedere:\n' + '\n'.join(missing), 'danger')
            return render_template(
                'inventory/load_component.html',
                part=part,
                doc_defs=doc_defs,
                doc_label_map=doc_label_map,
                back_url=back_url,
                embedded=embedded_flag,
                box_id=box_id_param,
                docs_ready=len(doc_defs) == 0
            )
        # Base directory under static for documents
        doc_base_dir = os.path.join(current_app.static_folder, 'documents')
        safe_name = secure_filename(part.name) or f"id_{part.id}"
        # Save each uploaded file into the component directory, copy to product directories and
        # replicate it in the "Produzione/archivio" hierarchy.  The production archive
        # mirrors the upload location so that documents can later be associated with
        # assemblies during the build process.  A unique subfolder is created for
        # each load operation to avoid name clashes.  The folder name follows
        # the pattern ``<component>_<timestamp>`` where the component name has
        # been sanitised via ``secure_filename``.  All document types are
        # preserved within the subfolder.
        # Compute a single timestamp for this load operation to build a unique
        # identifier for the component directory in the Produzione archive.  Using a
        # consistent timestamp across documents ensures they all reside under
        # the same folder rather than each file creating a separate directory.
        production_timestamp = int(time.time())
        produzione_component_id = f"{safe_name}_{production_timestamp}"
        produzione_base = None  # lazily initialised when needed
        for doc_def in doc_defs:
            field = doc_def['field_name']
            file_obj = request.files.get(field)
            if not file_obj or not file_obj.filename:
                continue
            doc_type = doc_def['type']
            # Destination directory for the compiled document under static/documents
            dest_dir = os.path.join(doc_base_dir, safe_name, doc_type)
            try:
                os.makedirs(dest_dir, exist_ok=True)
            except Exception:
                pass
            # Generate a unique filename for compiled documents using the current
            # date and time and the uploaded filename.  This prevents collisions
            # while preserving the original filename.  Format:
            # YYYYMMDD_HHMMSS_<original_name>
            upload_name = secure_filename(file_obj.filename)
            timestamp_str = time.strftime('%Y%m%d_%H%M%S')
            unique_name = f"{timestamp_str}_{upload_name}"
            dest_path = os.path.join(dest_dir, unique_name)
            try:
                file_obj.save(dest_path)
            except Exception:
                # Skip saving this file if an error occurs
                continue
            # NOTE: Do not replicate compiled documents into product-specific directories.
            # Historically the upload logic copied compiled documents into both the
            # component-level folder (static/documents/<component>/<type>) and into each
            # product hierarchy (static/documents/<product>/<component>/<type>).  This
            # behaviour caused compiled documents to appear under product folders such as
            # ``static/documents/DQS100``, which should be reserved for product-level
            # documentation defined in the anagrafiche.  To meet the new requirement,
            # compiled documents are no longer copied into product-specific directories.
            # The component-level copy above remains so that future builds can access
            # previously uploaded documents for this component.  Compiled documents are
            # also stored in the ``Produzione/archivio`` hierarchy (see below) for
            # traceability and to provide a source when rendering the archive pages.
            # If additional replication is desired in the future, implement it here.
            # ------------------------------------------------------------------
            # Copy each compiled document into the Produzione archive.  The
            # production archive is located in the application root (one level
            # above ``app/``) under the folder ``Produzione/archivio``.  A unique
            # subdirectory is created for this component load (e.g.
            # ``Produzione/archivio/Pompa_ABC123_1698571234``).  Documents are
            # organised by their type inside this folder.  The directory is
            # lazily created on the first file copy to avoid unnecessary
            # overhead when no documents are present.
            try:
                if produzione_base is None:
                    # Determine the absolute path to the Produzione archive
                    app_root = current_app.root_path
                    produzione_root = os.path.join(app_root, 'Produzione')
                    # Ensure base directories exist
                    os.makedirs(os.path.join(produzione_root, 'archivio'), exist_ok=True)
                    produzione_base = os.path.join(produzione_root, 'archivio', produzione_component_id)
                # Ensure the directory for this document type exists
                prod_doc_dir = os.path.join(produzione_base, doc_type)
                os.makedirs(prod_doc_dir, exist_ok=True)
                dest_prod_copy = os.path.join(prod_doc_dir, unique_name)
                shutil.copyfile(dest_path, dest_prod_copy)
            except Exception:
                # Ignore errors copying to Produzione archive
                pass
            # ------------------------------------------------------------------
            # Create a Document record for this upload when loading into a production box.
            # By storing a relative URL to the static folder we ensure that the
            # document can be downloaded from the archive popup.  Only create
            # records when a production box id is provided.  Non‑box loads do not
            # generate stock items, so a Document entry cannot be assigned to a
            # specific owner in that case.
            if box_id_param:
                try:
                    box_id_int = int(box_id_param)
                except Exception:
                    box_id_int = None
                if box_id_int:
                    # Compute a relative path for the Document.url.  Fall back to
                    # the expected documents folder structure when relative
                    # computation fails (e.g. due to unexpected path structure).
                    try:
                        rel_url = os.path.relpath(dest_path, current_app.static_folder)
                    except Exception:
                        rel_url = os.path.join('documents', safe_name, doc_type, unique_name)
                    try:
                        doc_record = Document(owner_type='BOX', owner_id=box_id_int,
                                              doc_type=doc_type, url=rel_url, status='CARICATO')
                        db.session.add(doc_record)
                    except Exception:
                        # Ignore database errors when adding the document
                        pass
        # When loading components outside of a production box, update the
        # stock quantity across all matching structures and record an
        # inventory log.  When ``box_id_param`` is defined this call
        # originates from a production box and the stock update is
        # performed via the API endpoint instead.  Skipping the update
        # here prevents double counting quantities when loading from
        # production boxes.  When a production box id is provided, we
        # perform the stock update directly below using the same
        # logic implemented in the API endpoint.  This ensures that
        # each stock item loaded from a production box is marked as
        # completed and the box status reflects the partial progress.
        if box_id_param:
            # -----------------------------------------------------------------
            # Update stock quantities and statuses for a production box.
            # Replicates the logic of the `/api/production-box/<id>/load` endpoint
            # for the case where a single item is being loaded from within the
            # embedded modal.  This avoids relying on a separate API call
            # triggered from the client and ensures the row disappears as soon
            # as the form is submitted.
            try:
                box_id_int = int(box_id_param)
            except Exception:
                box_id_int = None
            if box_id_int:
                box = ProductionBox.query.get(box_id_int)
            else:
                box = None
            if box:
                # Determine selected items: when an item_id is provided, load only that item.
                selected_items: list[StockItem] = []
                if item_id_param:
                    try:
                        target_id = int(item_id_param)
                    except Exception:
                        target_id = None
                    # Find the matching stock item in this box
                    match_item = None
                    for si in box.stock_items:
                        if si.id == target_id:
                            match_item = si
                            break
                    if match_item:
                        selected_items = [match_item]
                    else:
                        selected_items = []
                else:
                    # No item id: load all items in the box
                    selected_items = list(box.stock_items)
                # Collect increments per product and per structure.  Import model
                # classes under local aliases to avoid shadowing the global
                # ``Structure`` symbol (importing a name inside a function makes it
                # a local variable throughout the function, which prevents
                # references earlier in the function from resolving the module
                # level ``Structure``).  Use these aliases for queries below.
                from collections import defaultdict
                from ...models import (
                    Product as _Prod,
                    ProductComponent as _ProdComp,
                    Structure as _Struct,
                    ScanEvent as _ScanEvent
                )  # Local import to avoid circular refs and shadowing
                import json as _json
                product_increments: defaultdict[int, int] = defaultdict(int)
                structure_increments: dict[tuple[str | int, str], dict[str, Any]] = {}
                # Record a scan event for each stock item.  Even when lot management
                # is enabled and multiple items share the same DataMatrix, a
                # separate event is recorded for each to accurately reflect
                # the quantity loaded in the archive.
                for si in selected_items:
                    # Skip already completed items
                    if si.status == 'COMPLETATO':
                        continue
                    # Mark as completed
                    si.status = 'COMPLETATO'
                    # Increment product count
                    prod = si.product
                    if prod:
                        product_increments[prod.id] += 1
                    # Build metadata for the scan event.  Include the box id and user info.
                    try:
                        meta_dict = {'box_id': box.id}
                        if current_user and hasattr(current_user, 'is_authenticated') and current_user.is_authenticated:
                            meta_dict['user_id'] = current_user.id
                            try:
                                meta_dict['user_username'] = current_user.username
                            except Exception:
                                pass
                        dm_code_meta: str = getattr(si, 'datamatrix_code', '') or ''
                        comp_code_meta: str | None = None
                        try:
                            for seg in (dm_code_meta or '').split('|'):
                                seg_str = seg.strip()
                                if seg_str.upper().startswith('P='):
                                    comp_code_meta = seg_str.split('=', 1)[1]
                                    break
                        except Exception:
                            comp_code_meta = None
                        struct_meta = None
                        if comp_code_meta:
                            try:
                                struct_meta = Structure.query.filter_by(name=comp_code_meta).first()
                            except Exception:
                                struct_meta = None
                        if not struct_meta:
                            try:
                                prod_tmp = getattr(si, 'product', None)
                                if prod_tmp:
                                    root_comp_tmp = (
                                        ProductComponent.query
                                        .filter_by(product_id=prod_tmp.id)
                                        .order_by(ProductComponent.id.asc())
                                        .first()
                                    )
                                    if root_comp_tmp:
                                        struct_meta = Structure.query.get(root_comp_tmp.structure_id)
                            except Exception:
                                struct_meta = None
                        if struct_meta:
                            try:
                                meta_dict['structure_name'] = getattr(struct_meta, 'name', '') or ''
                            except Exception:
                                meta_dict['structure_name'] = ''
                            try:
                                meta_dict['structure_description'] = getattr(struct_meta, 'description', '') or ''
                            except Exception:
                                meta_dict['structure_description'] = ''
                            try:
                                rev_lbl = struct_meta.revision_label
                            except Exception:
                                rev_lbl = ''
                            if rev_lbl:
                                meta_dict['revision_label'] = rev_lbl
                            try:
                                rev_idx = getattr(struct_meta, 'revision', None)
                                if rev_idx is not None:
                                    meta_dict['revision_index'] = int(rev_idx)
                            except Exception:
                                pass
                        try:
                            meta_json = _json.dumps(meta_dict)
                        except Exception:
                            meta_json = _json.dumps({'box_id': box.id})
                    except Exception:
                        meta_json = _json.dumps({'box_id': box.id})
                    # Always create a scan event for this stock item.  Duplicate
                    # events with the same DataMatrix code are allowed; this
                    # ensures that each unit appears as a separate row in the
                    # archive while still sharing the same documents when lot
                    # management is enabled.
                    try:
                        event = _ScanEvent(
                            datamatrix_code=(si.datamatrix_code or ''),
                            action='CARICA',
                            meta=meta_json
                        )
                        db.session.add(event)
                    except Exception:
                        pass
                    # Determine structure via datamatrix (P=component) to accumulate increments per structure.
                    struct = None
                    component_code = None
                    dm = si.datamatrix_code or ''
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
                            struct = _Struct.query.filter_by(name=component_code).first()
                        except Exception:
                            struct = None
                    if not struct and prod:
                        try:
                            root_comp = (
                                _ProdComp.query
                                .filter_by(product_id=prod.id)
                                .order_by(_ProdComp.id.asc())
                                .first()
                            )
                            if root_comp:
                                struct = _Struct.query.get(root_comp.structure_id)
                        except Exception:
                            struct = None
                    if struct:
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
                # Update product quantities
                for prod_id, inc in product_increments.items():
                    try:
                        prod = _Prod.query.get(prod_id)
                        if prod:
                            current_qty = prod.quantity_in_stock or 0
                            prod.quantity_in_stock = current_qty + inc
                    except Exception:
                        pass
                # Update structure quantities
                for key, entry in structure_increments.items():
                    struct = entry['struct']
                    inc = entry['count']
                    # Determine all matching structures (name and component_id)
                    matches_dict: dict[int, _Struct] = {}
                    try:
                        # Include name-based matches
                        if struct.name:
                            try:
                                for m in _Struct.query.filter(_Struct.name == struct.name).all():
                                    matches_dict[m.id] = m
                            except Exception:
                                pass
                        # Include component_id matches
                        if struct.component_id:
                            try:
                                for m in _Struct.query.filter(_Struct.component_id == struct.component_id).all():
                                    matches_dict[m.id] = m
                            except Exception:
                                pass
                    except Exception:
                        matches_dict = {}
                    matches = list(matches_dict.values())
                    # Compute current maximum quantity across matches
                    quantities: list[float] = []
                    for m in matches:
                        try:
                            quantities.append(float(m.quantity_in_stock or 0))
                        except Exception:
                            pass
                    if not quantities:
                        try:
                            quantities.append(float(struct.quantity_in_stock or 0))
                        except Exception:
                            quantities.append(0)
                    current_qty = max(quantities) if quantities else 0.0
                    new_qty = current_qty + inc
                    if new_qty < 0:
                        new_qty = 0.0
                    # Apply new quantity to representative and matches
                    struct.quantity_in_stock = new_qty
                    for m in matches:
                        m.quantity_in_stock = new_qty
                # Update box status
                if item_id_param:
                    # Determine if any item remains incomplete
                    incomplete = False
                    for si in box.stock_items:
                        if si.status != 'COMPLETATO':
                            incomplete = True
                            break
                    box.status = 'IN_CARICO' if incomplete else 'COMPLETATO'
                else:
                    box.status = 'COMPLETATO'
                try:
                    db.session.commit()
                except Exception:
                    # On failure, rollback to maintain consistency
                    db.session.rollback()
        else:
            # Update stock quantity across all matching structures (same component_id or name).
            # When multiple Structure rows represent the same physical component (via
            # shared component_id or name), they may have diverging quantities.  Using
            # the quantity from the currently loaded part can incorrectly reduce
            # stock when another matching structure holds a higher quantity.  To
            # ensure stock is always increased relative to the global on‑hand
            # quantity, determine the highest quantity among all matching structures
            # before applying the increment.  Then propagate the new quantity to
            # every match.
            matches_dict: dict[int, Structure] = {}
            try:
                # Always include name-based matches
                try:
                    for s in Structure.query.filter(Structure.name == part.name).all():
                        matches_dict[s.id] = s
                except Exception:
                    pass
                # Include component_id matches when present
                if part.component_id:
                    try:
                        for s in Structure.query.filter(Structure.component_id == part.component_id).all():
                            matches_dict[s.id] = s
                    except Exception:
                        pass
            except Exception:
                matches_dict = {}
            matches = list(matches_dict.values())
            quantities: list[float] = []
            for s in matches:
                try:
                    quantities.append(float(s.quantity_in_stock or 0))
                except Exception:
                    pass
            if not quantities:
                try:
                    quantities.append(float(part.quantity_in_stock or 0))
                except Exception:
                    quantities.append(0)
            current_qty: float = max(quantities) if quantities else 0.0
            new_qty: float = current_qty + qty
            part.quantity_in_stock = new_qty
            for s in matches:
                s.quantity_in_stock = new_qty
            db.session.commit()
            # Record inventory log
            try:
                if getattr(part, 'flag_part', False):
                    cat = 'parts'
                elif getattr(part, 'flag_commercial', False):
                    cat = 'commercial'
                else:
                    cat = 'unknown'
                log = InventoryLog(
                    user_id=current_user.id,
                    structure_id=part.id,
                    category=cat,
                    action=f'Caricati {qty:.0f}',
                    quantity=qty
                )
                db.session.add(log)
                db.session.commit()
            except Exception:
                pass
        # Create a printable summary of this load operation.  Generate a
        # snapshot file under ``static/prints`` containing the component
        # identifier, quantity loaded and the names of uploaded documents.  The
        # summary persists independently of future modifications to the part.
        try:
            print_dir = os.path.join(current_app.static_folder, 'prints')
            os.makedirs(print_dir, exist_ok=True)
            summary_name = f"load_component_{part.id}_{int(time.time())}.txt"
            summary_path = os.path.join(print_dir, summary_name)
            with open(summary_path, 'w', encoding='utf-8') as summary:
                summary.write(f"Componente: {part.name} (ID: {part.id})\n")
                summary.write(f"Quantità caricata: {qty}\n")
                if doc_defs:
                    summary.write("Documenti caricati:\n")
                    for doc_def in doc_defs:
                        try:
                            summary.write(f" - {doc_def.get('display_name', '')}\n")
                        except Exception:
                            pass
        except Exception:
            # Ignore errors during summary creation
            pass
        # When loading from a production box inside an embedded modal, do not
        # perform a normal redirect back to the box view.  Instead return a
        # minimal success page that instructs the parent window to close the
        # modal and reload.  This ensures the updated item list is visible
        # immediately without requiring a manual refresh.  Only invoke this
        # behaviour when both a box id is provided and the embedded flag is set.
        if box_id_param and embedded_flag:
            return render_template(
                'inventory/load_component_success.html',
                back_url=back_url
            )
        # Otherwise redirect back to previous page with anchor to highlight the updated row
        base_ref = back_url.split('#', 1)[0] if back_url else ''
        anchor = f"#part-{part.id}"
        return redirect(f"{base_ref}{anchor}")
    # GET request: show form
    return render_template(
        'inventory/load_component.html',
        part=part,
        doc_defs=doc_defs,
        doc_label_map=doc_label_map,
        back_url=back_url,
        embedded=embedded_flag,
        box_id=box_id_param,
        item_id=item_id_param,
        docs_ready=len(doc_defs) == 0
    )


@inventory_bp.route('/product/<int:product_id>/loaded')
@login_required
def product_loaded(product_id: int):
    """View all stock items that have been loaded for a product.

    This view renders a simple table listing the DataMatrix codes and
    associated production boxes for each stock item whose status is
    ``CARICATO`` or ``COMPLETATO``.  During loading the API marks
    completed items with ``COMPLETATO``; therefore we include both
    statuses when querying the database.  Operators can use this
    page to audit what has been physically loaded for a given product.
    The data is retrieved from the ``stock_items`` table introduced
    alongside the reservation functionality.
    """
    product = Product.query.get_or_404(product_id)
    items = StockItem.query.filter(
        StockItem.product_id == product.id,
        StockItem.status.in_(['CARICATO', 'COMPLETATO'])
    ).all()
    return render_template('inventory/product_loaded.html', product=product, items=items)


@inventory_bp.route('/product/<int:product_id>/archive')
@login_required
def product_archive_view(product_id: int):
    """Display the audit history for a product.

    This route collects scan events associated with all stock items
    belonging to the specified product and renders them in reverse
    chronological order.  Each event shows the DataMatrix code, the
    recorded action and the timestamp.  The underlying data is
    provided by the ``scan_events`` table populated when loading
    production boxes.
    """
    product = Product.query.get_or_404(product_id)
    # Gather all DataMatrix codes for stock items belonging to this product
    items = StockItem.query.filter_by(product_id=product.id).all()
    codes = [item.datamatrix_code for item in items if item.datamatrix_code]
    events: list[ScanEvent] = []
    if codes:
        events = (
            ScanEvent.query
            .filter(ScanEvent.datamatrix_code.in_(codes))
            .order_by(ScanEvent.created_at.desc())
            .all()
        ) or []
        # Do not exclude events for consumed stock items; keep all events
    # Build a rich representation for each event.  Only include events
    # corresponding to non-assembly stock items (parts and commercial components).
    import json as _json
    from ...models import Structure, Document, StockItem as _StockItem, User  # type: ignore
    events_data: list[dict[str, Any]] = []
    # ---------------------------------------------------------------------
    # For each scan event assign documents by DataMatrix code.  When multiple
    # events exist for the same DataMatrix, the same set of documents should
    # appear on each event row.  Rather than assigning documents only to the
    # earliest event, collect documents for each event independently by
    # querying all stock items that share the same DataMatrix code and the
    # corresponding production box referenced in the event metadata.  Only
    # documents with status ``CARICATO`` or ``APPROVATO`` are included.
    #
    # Build a mapping from event id to its documents.  Each entry is a list
    # of (filename, URL) tuples ready for display.  Deduplication of
    # documents is performed per event based on the document id.
    event_docs_map: dict[int, list[tuple[str, str]]] = {}
    # Cache document lists per DataMatrix code.  When multiple events share
    # the same DataMatrix (as in lot management), compute the documents
    # once and reuse the result for all events.  This ensures that all
    # events for a batch display the same document list without
    # re-querying the database.
    dm_docs_cache: dict[str, list[tuple[str, str]]] = {}
    for ev in events:
        dm_code = ev.datamatrix_code or ''
        # If we have already computed the documents for this DataMatrix
        # code, reuse the cached list.  Otherwise compute from scratch.
        if dm_code in dm_docs_cache:
            event_docs_map[ev.id] = dm_docs_cache[dm_code]
            continue
        # Build a list of Document objects attached to all stock items
        # that share this DataMatrix code as well as the associated
        # production box referenced in the scan event's meta.  Only
        # include documents with status CARICATO or APPROVATO.
        doc_objs: list[Document] = []
        # Gather documents from stock items with the same DM
        try:
            stock_items_same_dm = _StockItem.query.filter_by(datamatrix_code=dm_code).all()
        except Exception:
            stock_items_same_dm = []
        for si in stock_items_same_dm or []:
            try:
                si_docs = Document.query.filter_by(owner_type='STOCK', owner_id=si.id).all()
            except Exception:
                si_docs = []
            doc_objs.extend(si_docs or [])
        # Also collect documents attached to the production box for this event
        box_id_val = None
        try:
            meta_dict = _json.loads(ev.meta) if ev.meta else {}
        except Exception:
            meta_dict = {}
        try:
            box_id_val = meta_dict.get('box_id')
        except Exception:
            box_id_val = None
        if box_id_val is not None:
            try:
                # Convert to int if possible
                try:
                    box_id_int = int(box_id_val)
                except Exception:
                    box_id_int = box_id_val
                box_docs = Document.query.filter_by(owner_type='BOX', owner_id=box_id_int).all()
            except Exception:
                box_docs = []
            else:
                doc_objs.extend(box_docs or [])
        # Filter documents by status and deduplicate by id
        seen_doc_ids: set[int] = set()
        docs_final: list[tuple[str, str]] = []
        for doc in doc_objs:
            try:
                doc_id = getattr(doc, 'id', None)
            except Exception:
                doc_id = None
            if not doc_id or doc_id in seen_doc_ids:
                continue
            try:
                status = getattr(doc, 'status', '').upper() if doc else ''
            except Exception:
                status = ''
            if status not in ('CARICATO', 'APPROVATO'):
                continue
            seen_doc_ids.add(doc_id)
            # Build URL to download the document.  Prefer the products
            # download endpoint; fall back to static serving when necessary.
            doc_url = ''
            try:
                doc_url = url_for('products.download_file', filename=doc.url)
            except Exception:
                try:
                    doc_url = url_for('static', filename=doc.url)
                except Exception:
                    doc_url = doc.url
            fname = os.path.basename(doc.url or '')
            docs_final.append((fname, doc_url))
        # Cache the final list for this DataMatrix code and assign to the event
        dm_docs_cache[dm_code] = docs_final
        event_docs_map[ev.id] = docs_final
    def _parse_dm(dm: str) -> dict[str, str]:
        """Parse a DataMatrix code into its component and type fields."""
        result: dict[str, str] = {}
        for segment in (dm or '').split('|'):
            if '=' in segment:
                key, val = segment.split('=', 1)
                if key == 'P':
                    result['component'] = val
                elif key == 'T':
                    result['type'] = val
        return result
    for ev in events:
        dm_code: str = ev.datamatrix_code or ''
        parsed = _parse_dm(dm_code)
        if not parsed:
            continue
        dm_type = parsed.get('type') or ''
        # Skip assembly and association events; these belong to the assemblies archive
        # Assemblies are identified by T=ASSIEME in the DataMatrix.  Association
        # events (action "ASSOCIA") represent linking a component to an assembly and
        # should not appear in the component archive.  They will instead be
        # nested under the relevant assembly in the assemblies archive view.
        if dm_type.upper() == 'ASSIEME':
            continue
        if ev.action and ev.action.upper() == 'ASSOCIA':
            continue
        comp_name = parsed.get('component') or ''
        # Look up structure to obtain description.  If not found, leave blank.
        struct = None
        try:
            struct = Structure.query.filter_by(name=comp_name).first()
        except Exception:
            struct = None
        description = ''
        if struct and getattr(struct, 'description', None):
            description = struct.description
        # Determine the user who performed the action from the event meta
        user_display = ''
        try:
            meta_dict = _json.loads(ev.meta) if ev.meta else {}
        except Exception:
            meta_dict = {}
        # Prefer a stored username in meta when present.  This preserves the
        # operator's identifier at the time of the event even if the User record
        # is later modified.  Fall back to the legacy ``user_email`` key for
        # archives generated with older versions.
        stored_username = None
        try:
            stored_username = (
                meta_dict.get('user_username')
                or meta_dict.get('username')
                or meta_dict.get('user_email')
            )
        except Exception:
            stored_username = None
        if stored_username:
            user_display = stored_username
        else:
            # Attempt to resolve user from meta fields
            # meta may include the id of the user who performed the scan.  Attempt
            # to resolve the user id (integer) to the username.  Accept numeric
            # ids or strings directly.  When no user information is present the
            # display will remain empty (rendered as a dash in the template).
            uid = (
                meta_dict.get('user_id')
                or meta_dict.get('user')
                or meta_dict.get('user_username')
                or meta_dict.get('username')
                or meta_dict.get('operator')
                or None
            )
            if uid:
                # Resolve the user identifier into a readable string.  If the
                # underlying User model defines a username field.  Fall back to the
                # legacy email field only when the username is unavailable to
                # preserve backwards compatibility with older databases.
                try:
                    uid_int = int(uid)
                    usr = User.query.get(uid_int)
                    if usr:
                        username = getattr(usr, 'username', None)
                        if username:
                            user_display = username
                        else:
                            legacy_email = getattr(usr, 'email', None)
                            if legacy_email:
                                user_display = legacy_email
                            else:
                                user_display = str(uid)
                    else:
                        user_display = str(uid)
                except Exception:
                    user_display = str(uid)
        # ---------------------------------------------------------------
        # Use the precomputed document assignments for this event.
        # The ``event_docs_map`` populated above maps each scan event to
        # a list of (filename, url) tuples.  Documents are computed once
        # per DataMatrix code and shared across all events that share
        # that code.  This ensures that all events representing the
        # same lot display the same documents while avoiding
        # redundant database queries.
        docs: list[tuple[str, str]] = event_docs_map.get(ev.id, [])

        # Skip legacy production/archive scanning and box-level fallback
        # for this view.  The unique assignment in ``event_docs_map``
        # already captures documents uploaded during production.  Any
        # additional manual uploads via the UI should be tied to the
        # relevant stock item or box and will therefore be present in
        # ``event_docs_map``.  Removing the fallback prevents the same
        # file from appearing under multiple events.
        #
        # If desired, you could reintroduce scanning of extra folders
        # here for events that have no documents assigned.  However,
        # this would break the one-to-one mapping of documents to
        # production events and reintroduce duplicates when the same
        # component is reloaded.
        # Remove duplicate document entries before adding this event.  Without
        # deduplication the same file may appear multiple times when it is
        # present both in the database and in legacy production folders.
        docs = _dedupe_docs(docs)
        # Extract static component information from the event meta when available.  This
        # ensures that the archive remains a historical snapshot by displaying
        # the name, description and revision recorded at the time of the scan.  When
        # the meta lacks these fields (e.g. legacy events), fall back to the
        # current structure description and revision.
        ev_meta: dict[str, Any] = {}
        try:
            ev_meta = _json.loads(ev.meta) if ev.meta else {}
        except Exception:
            ev_meta = {}
        # Determine the display name: prefer the structure_name stored in meta.
        ev_name = ev_meta.get('structure_name') or comp_name
        # Determine description: use stored description or fallback to current structure
        ev_desc = ev_meta.get('structure_description') or description
        # Determine revision: use the stored revision_label when present.
        ev_rev = ''
        try:
            ev_rev = ev_meta.get('revision_label') or ''
        except Exception:
            ev_rev = ''
        # Do not derive the revision from the current Structure when the meta
        # lacks a revision_label.  Falling back to the Structure would
        # inadvertently update the archive when the underlying anagrafica is
        # revised.  Instead leave the revision blank for legacy events.
        events_data.append({
            # Include the scan event primary key so that downstream views can
            # identify this specific event.  The event id is exposed via the
            # ``id`` key.  This value is used in the archive template to
            # construct document links that refer back to the originating
            # event rather than the generic DataMatrix code.  Without the id
            # the docs view cannot distinguish between multiple scan events
            # for the same component.
            'id': ev.id,
            'timestamp': ev.created_at,
            'datamatrix': dm_code,
            'name': ev_name,
            'description': ev_desc,
            'revision': ev_rev,
            'user': user_display,
            'action': ev.action,
            'docs': docs,
            # Precompute a simple image for the DataMatrix code.  See
            # _generate_dm_image for implementation details.
            'image_data': _generate_dm_image(dm_code),
        })
    # ------------------------------------------------------------------
    # Filter out component events that belong to assemblies
    #
    # Parts and commercial components used in assembly builds should not
    # appear in the standalone component archive.  To achieve this,
    # exclude any DataMatrix codes that appear as the parent_code for
    # another StockItem.  When a component is associated with an
    # assembly its DataMatrix code becomes the parent_code of the
    # stock items representing its own children.  Removing such events
    # from the component archive consolidates all components under the
    # corresponding assembly entry in the assemblies archive.
    # ------------------------------------------------------------------
    # Remove events corresponding to components that have been used in
    # assembly builds of this product.  To avoid removing unrelated
    # components, restrict the search to assemblies built from this
    # product.  For each ProductBuild of the current product, determine
    # its assembly DataMatrix code and gather all stock items whose
    # parent_code matches that assembly code.  The datamatrix codes of
    # these stock items correspond to components consumed during the
    # assembly.  Exclude such component codes from the component
    # archive so that they appear only in the assemblies archive.
    try:
        # Import ProductBuild and StockItem with aliases.  Avoid importing Product
        # here to prevent masking the outer Product variable, which leads to
        # UnboundLocalError when referenced before assignment at the top of
        # this view.  ``Product`` is already available from the outer scope.
        from ...models import ProductBuild as _ProductBuild, StockItem as _StockAlias
        #-----------------------------------------------------------------
        # Build a set of DataMatrix codes representing assemblies for this
        # product.  Assemblies may originate from stock items loaded into
        # production boxes as well as synthetic codes when no stock item
        # exists.  We include any code whose payload contains "T=ASSIEME"
        # and belongs to the current product's builds.  This set is used
        # to identify which stock items represent assemblies for this
        # product so that their component events can be removed from the
        # component archive.
        #-----------------------------------------------------------------
        assembly_codes: set[str] = set()
        # First gather DataMatrix codes from stock items of this product
        try:
            prod_items = _StockAlias.query.filter_by(product_id=product.id).all()
        except Exception:
            prod_items = []
        for si in prod_items:
            dm = si.datamatrix_code or ''
            if 'T=ASSIEME' in dm.upper():
                assembly_codes.add(dm)
        # Also inspect builds for this product to find assembly codes via their
        # production box.  If no stock item with an ASSIEME code is found,
        # generate a synthetic code using the product name.
        try:
            builds_for_product = _ProductBuild.query.filter_by(product_id=product.id).all()
        except Exception:
            builds_for_product = []
        for pb in builds_for_product:
            found = None
            try:
                if getattr(pb, 'production_box_id', None):
                    # Search for stock items in the same production box containing an assembly code
                    try:
                        cand_items = _StockAlias.query.filter_by(production_box_id=pb.production_box_id).all()
                    except Exception:
                        cand_items = []
                    for si in cand_items:
                        dm_val = si.datamatrix_code or ''
                        if 'T=ASSIEME' in dm_val.upper():
                            found = dm_val
                            break
            except Exception:
                pass
            if found:
                assembly_codes.add(found)
            else:
                # Use a fallback synthetic code based on product name
                assembly_codes.add(f"P={product.name}|T=ASSIEME")
        #-----------------------------------------------------------------
        # Collect all DataMatrix codes of components consumed in any of the
        # assemblies above.  Use a recursive helper to traverse nested
        # assemblies so that components used in subassemblies are also
        # excluded from the standalone component archive.
        #-----------------------------------------------------------------
        consumed_codes: set[str] = set()
        visited_assemblies: set[str] = set()
        def _gather_children(parent_code: str) -> None:
            """Recursively collect DataMatrix codes of stock items whose
            parent_code matches the given assembly code.  When a child is
            itself an assembly, the recursion continues on that child to
            collect its descendants.

            Args:
                parent_code: DataMatrix code representing the assembly whose
                             children should be collected.
            """
            # Avoid infinite recursion
            if parent_code in visited_assemblies:
                return
            visited_assemblies.add(parent_code)
            try:
                children = _StockAlias.query.filter_by(parent_code=parent_code).all()
            except Exception:
                children = []
            for child in children:
                if not child or not child.datamatrix_code:
                    continue
                c_code = child.datamatrix_code
                consumed_codes.add(c_code)
                # If the child is itself an assembly, recurse
                if 'T=ASSIEME' in c_code.upper():
                    _gather_children(c_code)
        # Initiate recursion from all known assembly codes
        for asm_code in assembly_codes:
            _gather_children(asm_code)
        # Filter events_data: exclude events whose datamatrix belongs to a
        # consumed component.  This ensures components used in assembly
        # builds are shown only in the assemblies archive and not in the
        # standalone component archive.
        filtered_events: list[dict[str, Any]] = []
        for ev in events_data:
            dm_code = ev.get('datamatrix') or ''
            if dm_code in consumed_codes:
                continue
            filtered_events.append(ev)
        events_data = filtered_events
    except Exception:
        # On any error, leave events_data unchanged
        pass

    return render_template(
        'inventory/product_archive.html',
        product=product,
        events_data=events_data,
        active_tab='archive'
    )


@inventory_bp.route('/product/<int:product_id>/archive/docs')
@login_required
def product_docs_view(product_id: int):
    """Display a page listing all documents associated with a DataMatrix code.

    This route is used by both the component and assembly archives to show
    detailed document information for a specific event.  It accepts
    query parameters:

      code: The full DataMatrix payload (e.g. "DMV1|P=ABC|S=123|T=PARTE").
      src:  The source view ("components" or "assemblies") used to
            determine the back button destination.

    The page displays the DataMatrix code as an image (if possible) and
    provides download links for each associated document.  When no
    documents are found the list remains empty.

    Args:
        product_id: Identifier of the product whose archive is being viewed.

    Returns:
        Rendered HTML page showing the documents and a back button.
    """
    from flask import abort
    # Extract query parameters
    dm_code = request.args.get('code', '')
    src = request.args.get('src', 'components')
    if not dm_code:
        return abort(404)
    # Determine whether this code refers to an assembly or a component
    def _parse_dm(code: str) -> dict[str, str]:
        result: dict[str, str] = {}
        for segment in (code or '').split('|'):
            if '=' in segment:
                key, val = segment.split('=', 1)
                if key == 'P':
                    result['component'] = val
                elif key == 'T':
                    result['type'] = val
        return result
    parsed = _parse_dm(dm_code)
    dm_type = (parsed.get('type') or '').upper()
    comp_code = parsed.get('component') or ''
    # Box context: optional production box id passed from the archive view.  When
    # provided, document results will be restricted to the specified box.
    box_id_param = request.args.get('box_id')
    try:
        box_id_int: int | None = int(box_id_param) if box_id_param else None
    except (TypeError, ValueError):
        box_id_int = None
    # Prepare a list to collect document details.  Each entry is a
    # dictionary with keys: ``name`` (the display filename), ``url`` (the
    # download link) and ``category`` (derived from the folder name or
    # inferred).  This replaces the previous simple list of (filename,
    # url) tuples to allow grouping by category and display of the
    # original filename.
    docs_detail: list[dict[str, Any]] = []
    # Helper to derive the original filename from a compiled filename.
    # Compiled files have the form ``<base>_compiled_<timestamp>_<uuid>.ext``.
    # This function strips the compiled portion to recover ``<base>.ext``.
    def _original_filename(fname: str) -> str:
        """
        Return the stored filename without attempting to derive an original name.

        Args:
            fname: Filename including the extension.

        Returns:
            The filename as stored (compiled or otherwise).
        """
        return fname
    # Determine a human‑friendly component name for the page title.  If a
    # ComponentMaster exists for the component code, prefer its description
    # when available; otherwise fall back to the component code itself.
    component_name: str = comp_code
    if comp_code:
        try:
            cm = ComponentMaster.query.filter_by(code=comp_code).first()
            if cm:
                # Use description if defined; if not, the code serves as name
                if getattr(cm, 'description', None):
                    component_name = cm.description
                elif getattr(cm, 'code', None):
                    component_name = cm.code
        except Exception:
            # Leave component_name unchanged on any error
            pass
    # Helper to build static URL if possible
    def _static_url(rel_path: str) -> str:
        """
        Resolve a relative path within the static folder into a URL.

        When ``url_for`` succeeds the returned value points to the
        appropriate Flask static endpoint (e.g. ``/static/documents/foo.pdf``).
        On failure, a best‑effort fallback is returned by prefixing
        ``/static/`` to the relative path so that the file can still be
        requested directly.  Absolute paths are returned unchanged.

        :param rel_path: A path relative to the ``static`` directory.  This
            should not begin with a leading slash.  If the path is empty
            or an exception occurs a fallback path is returned.
        :return: A URL string usable as the ``href`` for document links.
        """
        try:
            # Use Flask's static endpoint when possible
            return url_for('static', filename=rel_path)
        except Exception:
            # Fallback: ensure the path begins with /static/ when it is
            # relative.  When ``rel_path`` already starts with a slash
            # return it unchanged.  This avoids generating malformed
            # URLs like ``/static//static/foo``.
            if rel_path and not rel_path.startswith('/'):
                return '/static/' + rel_path.lstrip('/')
            return rel_path
    # Note: the global import for ComponentMaster was moved to the module
    # level.  Avoid re-importing inside the function.
    # Retrieve documents depending on whether this is an assembly or component
    try:
        # First, look up the stock item associated with this DataMatrix code
        stock_item = _StockItem.query.filter_by(datamatrix_code=dm_code).first()
    except Exception:
        stock_item = None
    if dm_type == 'ASSIEME':
        # Assembly: collect documents linked to the assembly stock item and its production box
        if stock_item:
            # Stock-level documents for assemblies.  If a box context is provided,
            # only include documents when the stock item belongs to the same
            # production box.  Without a box context, include all stock-level
            # documents regardless of box.
            include_stock = False
            try:
                if box_id_int is None:
                    include_stock = True
                else:
                    include_stock = getattr(stock_item, 'production_box_id', None) == box_id_int
            except Exception:
                include_stock = False
            if include_stock:
                try:
                    doc_records = Document.query.filter_by(owner_type='STOCK', owner_id=stock_item.id).all()
                except Exception:
                    doc_records = []
                for doc in doc_records:
                    # Derive category from the path of the document URL (folder name)
                    try:
                        raw_path = doc.url or ''
                        category = os.path.basename(os.path.dirname(raw_path)) or 'varie'
                    except Exception:
                        category = 'varie'
                    fname = os.path.basename(doc.url or '')
                    # Use the products blueprint to serve documents as attachments.  When
                    # ``url_for`` fails (e.g. outside application context) fall back
                    # to resolving a static URL via the helper.  The download route
                    # ensures the file is downloaded rather than opened in the browser.
                    doc_url = ''
                    try:
                        doc_url = url_for('products.download_file', filename=doc.url)
                    except Exception:
                        doc_url = _static_url(doc.url or '')
                    docs_detail.append({
                        'name': _original_filename(fname),
                        'url': doc_url,
                        'category': category,
                        'real_name': fname,
                    })
            # Include box-level documents.  When a box context is provided,
            # restrict to that specific box id; otherwise include the current
            # stock item's box.
            box_id = None
            try:
                box_id = getattr(stock_item, 'production_box_id', None)
            except Exception:
                box_id = None
            # Determine which box id(s) to search for document attachments
            box_ids_to_search: list[int] = []
            if box_id_int is not None:
                # When a box id is provided, use only that one
                box_ids_to_search = [box_id_int]
            else:
                # Without a specified context, use the stock item's box id if present
                if box_id:
                    box_ids_to_search = [box_id]
            for b_id in box_ids_to_search:
                try:
                    box_docs = Document.query.filter_by(owner_type='BOX', owner_id=b_id).all()
                except Exception:
                    box_docs = []
                for doc in box_docs:
                    try:
                        raw_path = doc.url or ''
                        category = os.path.basename(os.path.dirname(raw_path)) or 'varie'
                    except Exception:
                        category = 'varie'
                    fname = os.path.basename(doc.url or '')
                    doc_url = ''
                    try:
                        doc_url = url_for('products.download_file', filename=doc.url)
                    except Exception:
                        doc_url = _static_url(doc.url or '')
                    docs_detail.append({
                        'name': _original_filename(fname),
                        'url': doc_url,
                        'category': category,
                        'real_name': fname,
                    })
        # Attempt to locate additional documents on disk for this assembly.  This mirrors
        # the legacy behaviour in the assembly archive view and ensures that assemblies
        # without database-linked documents still show available production or compiled
        # documentation.  The scan is performed regardless of whether a box_id context
        # is provided; duplicate names will be removed later.  We do not restrict
        # scanning by box id because compiled documentation is organised by component name.
        if True:
            try:
                # Use the component code (comp_code) to build candidate directory names.
                comp_name = comp_code
                produzione_root = os.path.join(current_app.root_path, 'Produzione')
                archivio_root = os.path.join(produzione_root, 'archivio')
                extra_docs: list[dict[str, Any]] = []
                candidates: list[str] = []
                if comp_name:
                    safe_name = secure_filename(comp_name)
                    raw_name = comp_name.strip()
                    if safe_name:
                        candidates.append(safe_name)
                    if raw_name and raw_name != safe_name:
                        candidates.append(raw_name)
                    lower_raw = raw_name.lower() if raw_name else ''
                    if lower_raw and lower_raw not in [c.lower() for c in candidates]:
                        candidates.append(lower_raw)
                if os.path.isdir(archivio_root) and comp_name:
                    candidate_lowers = [c.lower() for c in candidates]
                    latest_mtime: float | None = None
                    latest_dir: str | None = None
                    for dir_name in os.listdir(archivio_root):
                        name_lower = dir_name.lower()
                        matched = False
                        for prefix in candidate_lowers:
                            if (name_lower.startswith(prefix + '_')
                                or name_lower.startswith(prefix)
                                or prefix in name_lower):
                                matched = True
                                break
                        if not matched:
                            continue
                        dir_path = os.path.join(archivio_root, dir_name)
                        if not os.path.isdir(dir_path):
                            continue
                        try:
                            mtime = os.path.getmtime(dir_path)
                        except Exception:
                            mtime = None
                        if mtime is None:
                            continue
                        if latest_mtime is None or mtime > latest_mtime:
                            latest_mtime = mtime
                            latest_dir = dir_name
                    if latest_dir:
                        comp_arch_path = os.path.join(archivio_root, latest_dir)
                        for dt_name in os.listdir(comp_arch_path):
                            dt_path = os.path.join(comp_arch_path, dt_name)
                            if not os.path.isdir(dt_path):
                                continue
                            for fname in os.listdir(dt_path):
                                file_path = os.path.join(dt_path, fname)
                                if os.path.isfile(file_path):
                                    try:
                                        rel = os.path.relpath(file_path, produzione_root)
                                        url = url_for('inventory.download_production_file', filepath=rel)
                                    except Exception:
                                        url = f"/inventory/download_production_file/{rel}"
                                    extra_docs.append({
                                        'name': _original_filename(fname),
                                        'url': url,
                                        'category': dt_name or 'varie',
                                        'real_name': fname,
                                    })
                # Fallback: compiled static documents when docs_detail is empty
                if not docs_detail and not extra_docs:
                    static_root = os.path.join(current_app.static_folder, 'documents')
                    for cname in candidates:
                        comp_static_path = os.path.join(static_root, cname)
                        if not os.path.isdir(comp_static_path):
                            continue
                        for doc_folder in os.listdir(comp_static_path):
                            doc_path = os.path.join(comp_static_path, doc_folder)
                            if not os.path.isdir(doc_path):
                                continue
                            for fname in os.listdir(doc_path):
                                fpath = os.path.join(doc_path, fname)
                                if os.path.isfile(fpath):
                                    try:
                                        rel_static = os.path.relpath(fpath, current_app.static_folder)
                                        static_url = url_for('static', filename=rel_static)
                                    except Exception:
                                        static_url = _static_url(rel_static)
                                    extra_docs.append({
                                        'name': _original_filename(fname),
                                        'url': static_url,
                                        'category': doc_folder or 'varie',
                                        'real_name': fname,
                                    })
                        if extra_docs:
                            break
                # Append extra docs and deduplicate names
                if extra_docs:
                    # Avoid duplicating names that already exist in docs_detail
                    existing_names = {d.get('name') for d in docs_detail}
                    for ed in extra_docs:
                        if ed.get('name') not in existing_names:
                            docs_detail.append(ed)
                            existing_names.add(ed.get('name'))
            except Exception:
                # Ignore any errors during assembly-level file scanning
                pass
    else:
        # Component or commercial part: gather stock and box documents
        if stock_item:
            # Stock-level documents for components.  When a box context is provided,
            # only include documents belonging to stock items from that box.  Without
            # a box context include all stock docs.
            include_stock = False
            try:
                if box_id_int is None:
                    include_stock = True
                else:
                    include_stock = getattr(stock_item, 'production_box_id', None) == box_id_int
            except Exception:
                include_stock = False
            if include_stock:
                try:
                    doc_records = Document.query.filter_by(owner_type='STOCK', owner_id=stock_item.id).all()
                except Exception:
                    doc_records = []
                for doc in doc_records:
                    try:
                        raw_path = doc.url or ''
                        category = os.path.basename(os.path.dirname(raw_path)) or 'varie'
                    except Exception:
                        category = 'varie'
                    fname = os.path.basename(doc.url or '')
                    doc_url = ''
                    try:
                        doc_url = url_for('products.download_file', filename=doc.url)
                    except Exception:
                        doc_url = _static_url(doc.url or '')
                    docs_detail.append({
                        'name': _original_filename(fname),
                        'url': doc_url,
                        'category': category,
                        'real_name': fname,
                    })
            # Box-level documents: restrict to the specified box id if provided; otherwise
            # use the stock item's box id.
            box_id = None
            try:
                box_id = getattr(stock_item, 'production_box_id', None)
            except Exception:
                box_id = None
            box_ids_to_search: list[int] = []
            if box_id_int is not None:
                box_ids_to_search = [box_id_int]
            else:
                if box_id:
                    box_ids_to_search = [box_id]
            for b_id in box_ids_to_search:
                try:
                    box_docs = Document.query.filter_by(owner_type='BOX', owner_id=b_id).all()
                except Exception:
                    box_docs = []
                for doc in box_docs:
                    try:
                        raw_path = doc.url or ''
                        category = os.path.basename(os.path.dirname(raw_path)) or 'varie'
                    except Exception:
                        category = 'varie'
                    fname = os.path.basename(doc.url or '')
                    doc_url = ''
                    try:
                        doc_url = url_for('products.download_file', filename=doc.url)
                    except Exception:
                        doc_url = _static_url(doc.url or '')
                    docs_detail.append({
                        'name': _original_filename(fname),
                        'url': doc_url,
                        'category': category,
                        'real_name': fname,
                    })
        # Append documents stored in the production archive (Produzione/archivio)
        # and static folders.  When displaying a finished product (dm_type == PRODOTTO)
        # we always scan the archive/static documents regardless of the box context.
        # For parts and commercial items the scan is skipped when a box id is
        # provided to avoid mixing files from previous productions.
        if box_id_int is None or (dm_type and dm_type.upper() == 'PRODOTTO'):
            try:
                produzione_root = os.path.join(current_app.root_path, 'Produzione')
                archivio_root = os.path.join(produzione_root, 'archivio')
                if comp_code and os.path.isdir(archivio_root):
                    candidates: list[str] = []
                    safe_name = secure_filename(comp_code)
                    if safe_name:
                        candidates.append(safe_name)
                    raw_name = comp_code.strip()
                    if raw_name and raw_name != safe_name:
                        candidates.append(raw_name)
                    lower_raw = raw_name.lower() if raw_name else ''
                    if lower_raw and lower_raw not in candidates:
                        candidates.append(lower_raw)
                    candidate_lowers = [c.lower() for c in candidates]
                    latest_mtime: float | None = None
                    latest_dir: str | None = None
                    for dir_name in os.listdir(archivio_root):
                        name_lower = dir_name.lower()
                        matched_prefix = None
                        for prefix in candidate_lowers:
                            if (name_lower.startswith(prefix + '_') or
                                name_lower.startswith(prefix) or
                                prefix in name_lower):
                                matched_prefix = prefix
                                break
                        if not matched_prefix:
                            continue
                        dir_path = os.path.join(archivio_root, dir_name)
                        if not os.path.isdir(dir_path):
                            continue
                        try:
                            mtime = os.path.getmtime(dir_path)
                        except Exception:
                            mtime = None
                        if mtime is None:
                            continue
                        if latest_mtime is None or mtime > latest_mtime:
                            latest_mtime = mtime
                            latest_dir = dir_name
                    if latest_dir:
                        comp_arch_path = os.path.join(archivio_root, latest_dir)
                        for dt_name in os.listdir(comp_arch_path):
                            dt_path = os.path.join(comp_arch_path, dt_name)
                            if not os.path.isdir(dt_path):
                                continue
                            for fname in os.listdir(dt_path):
                                file_path = os.path.join(dt_path, fname)
                                if os.path.isfile(file_path):
                                    try:
                                        rel = os.path.relpath(file_path, produzione_root)
                                        url = url_for('inventory.download_production_file', filepath=rel)
                                    except Exception:
                                        url = f"/inventory/download_production_file/{rel}"
                                    category = dt_name or 'varie'
                                    docs_detail.append({
                                        'name': _original_filename(fname),
                                        'url': url,
                                        'category': category,
                                        'real_name': fname,
                                    })
                # Include any static compiled documents only when no documents have been found
                if not docs_detail:
                    static_root = os.path.join(current_app.static_folder, 'documents')
                    for cname in candidates:
                        comp_static_path = os.path.join(static_root, cname)
                        if not os.path.isdir(comp_static_path):
                            continue
                        for doc_folder in os.listdir(comp_static_path):
                            doc_path = os.path.join(comp_static_path, doc_folder)
                            if not os.path.isdir(doc_path):
                                continue
                            for fname in os.listdir(doc_path):
                                fpath = os.path.join(doc_path, fname)
                                if os.path.isfile(fpath):
                                    try:
                                        rel_static = os.path.relpath(fpath, current_app.static_folder)
                                        static_url = url_for('static', filename=rel_static)
                                    except Exception:
                                        static_url = _static_url(rel_static)
                                    docs_detail.append({
                                        'name': _original_filename(fname),
                                        'url': static_url,
                                        'category': doc_folder or 'varie',
                                        'real_name': fname,
                                    })
                        if docs_detail:
                            break
            except Exception:
                # ignore errors during production/static scanning
                pass
    # Deduplicate document entries by display name while preserving order.
    seen_names: set[str] = set()
    unique_details: list[dict[str, Any]] = []
    for d in docs_detail:
        name = d.get('name')
        if name in seen_names:
            continue
        seen_names.add(name)
        unique_details.append(d)
    docs_detail = unique_details
    # Validate each document URL and apply a fallback when necessary.  If the
    # resolved path does not exist or the URL is empty, fall back to the
    # download-by-name endpoint.  This maintains accessibility for
    # documents whose paths may be flattened or compiled.
    for d in docs_detail:
        url = d.get('url')
        missing_or_invalid = False
        try:
            if url and isinstance(url, str):
                if url.startswith('/static/'):
                    rel = url[len('/static/'):]
                    fpath = os.path.join(current_app.static_folder, rel)
                    missing_or_invalid = not os.path.isfile(fpath)
                elif url.startswith('/inventory/download_production_file/'):
                    rel = url[len('/inventory/download_production_file/'):]
                    prod_root = os.path.join(current_app.root_path, 'Produzione')
                    fpath = os.path.join(prod_root, rel)
                    missing_or_invalid = not os.path.isfile(fpath)
        except Exception:
            missing_or_invalid = True
        if not url or missing_or_invalid:
            # Use the real filename when falling back to the search endpoint; the
            # real_name corresponds to the actual file on disk rather than
            # the display name.
            real = d.get('real_name') or d.get('name')
            try:
                d['url'] = url_for('inventory.download_document_by_name', filename=real)
            except Exception:
                d['url'] = f"/inventory/document_by_name/{real}"
    # Group documents by category using human‑friendly labels.  Certain folder names
    # are mapped to more descriptive headings (e.g. "qualita" → "Cert Qualita",
    # "step" and "tavola" are combined under "Step/Tavola").  Unknown
    # categories fall back to the raw folder name.  After grouping, sort
    # categories alphabetically to provide a stable order in the UI.
    CATEGORY_LABELS: dict[str, str] = {
        'qualita': 'Cert Qualita',
        'disegni': 'Disegni',
        'manuale': 'Manuale',
        'step': 'Step/Tavola',
        'tavola': 'Step/Tavola',
        'varie': 'Altri documenti',
        'db': 'Altri documenti',
    }
    docs_by_category: dict[str, list[dict[str, Any]]] = {}
    for d in docs_detail:
        raw_cat = (d.get('category') or 'varie').lower()
        label = CATEGORY_LABELS.get(raw_cat, raw_cat.capitalize())
        docs_by_category.setdefault(label, []).append(d)
    docs_by_category = dict(sorted(docs_by_category.items(), key=lambda kv: kv[0].lower()))
    # Generate a DataMatrix image for display and download
    image_data = _generate_dm_image(dm_code)
    # Increase resolution of the DataMatrix image for higher‑quality download.  When
    # Pillow is available decode the base64 image and rescale it using nearest
    # neighbour interpolation.  If scaling fails, fall back to the original
    # image_data.  The scaling factor (e.g. 4×) can be adjusted to control
    # resolution; 4× yields a 480×480 image from a 120×120 source.
    image_data_hr = image_data
    if image_data:
        try:
            import base64 as _b64
            buf = _b64.b64decode(image_data)
            if Image is not None:
                im = Image.open(io.BytesIO(buf))
                factor = 4
                new_size = (im.width * factor, im.height * factor)
                im = im.resize(new_size, Image.NEAREST)
                out = io.BytesIO()
                im.save(out, format='PNG')
                image_data_hr = _b64.b64encode(out.getvalue()).decode('ascii')
        except Exception:
            image_data_hr = image_data
    return render_template(
        'inventory/docs_page.html',
        product_id=product_id,
        dm_code=dm_code,
        docs_by_category=docs_by_category,
        component_name=component_name,
        image_data=image_data_hr,
        from_page=src,
    )


# -----------------------------------------------------------------------------
# Event-specific document view
#
# Some archive views assign documents uniquely to the earliest scan event
# associated with a DataMatrix code.  To display the documents attached
# exclusively to a particular event, a separate route is provided.  This
# route accepts both the DataMatrix code and the scan event id and
# reconstructs the mapping used in the archive: documents are assigned
# sequentially in chronological order and will only appear on the event
# where they were first uploaded.  Later events for the same code will
# not show previously assigned documents.  The resulting documents are
# grouped by category and presented using the same template as the
# standard document view.

@inventory_bp.route('/product/<int:product_id>/archive/event_docs')
@login_required
def product_event_docs_view(product_id: int):
    """Display documents for a specific scan event of a DataMatrix code.

    Query parameters:
      code:  The DataMatrix payload (e.g. "DMV1|P=ABC|S=001|T=PARTE").
      ev_id: The primary key of the ScanEvent representing the production
             event to display.  Documents uploaded during earlier events
             will not be included.
      src:   The source view ("components" or "assemblies") used to
             determine the back button destination.

    When the code or event id is missing, a 404 is returned.  When no
    documents are associated with the specified event, an empty list is
    displayed.

    :param product_id: The identifier of the product whose archive is being viewed.
    :returns: Rendered HTML showing event-specific documents.
    """
    from flask import abort
    import json as _json
    # Extract query parameters
    dm_code = request.args.get('code', '')
    ev_id_raw = request.args.get('ev_id')
    src = request.args.get('src', 'components')
    if not dm_code or not ev_id_raw:
        return abort(404)
    try:
        ev_id = int(ev_id_raw)
    except Exception:
        return abort(404)
    # Parse component/type from DataMatrix to derive a component name
    def _parse_dm(code: str) -> dict[str, str]:
        res: dict[str, str] = {}
        for seg in (code or '').split('|'):
            if '=' in seg:
                k, v = seg.split('=', 1)
                if k == 'P':
                    res['component'] = v
                elif k == 'T':
                    res['type'] = v
        return res
    parsed = _parse_dm(dm_code)
    comp_code = parsed.get('component') or ''
    # Determine a human-friendly name for the component.  Use the ComponentMaster
    # description when available; fall back to the code itself.
    component_name: str = comp_code
    if comp_code:
        try:
            cm = ComponentMaster.query.filter_by(code=comp_code).first()
            if cm:
                if getattr(cm, 'description', None):
                    component_name = cm.description
                elif getattr(cm, 'code', None):
                    component_name = cm.code
        except Exception:
            pass
    # Helper to derive the original filename from compiled names
    def _original_filename(fname: str) -> str:
        """Return the stored filename unchanged for document display."""
        return fname
    # Build a documents list for this DataMatrix code.  Collect all
    # documents attached to any stock item with the given code and to
    # any production box referenced in its scan events.  Deduplicate by
    # document id and include only documents with status CARICATO or
    # APPROVATO.  Use this same list for every event so that all
    # events for a lot display the same documents.
    try:
        events_for_code = (
            ScanEvent.query
            .filter_by(datamatrix_code=dm_code)
            .order_by(ScanEvent.created_at.asc())
            .all()
        )
    except Exception:
        events_for_code = []
    # Collect documents from all stock items with this DM code
    doc_objs: list[Document] = []
    try:
        stock_items_same_dm = StockItem.query.filter_by(datamatrix_code=dm_code).all()
    except Exception:
        stock_items_same_dm = []
    for si in stock_items_same_dm or []:
        try:
            si_docs = Document.query.filter_by(owner_type='STOCK', owner_id=si.id).all()
        except Exception:
            si_docs = []
        doc_objs.extend(si_docs or [])
    # Collect documents from boxes referenced in all events for this code
    for ev in events_for_code or []:
        box_id_val = None
        try:
            meta_dict = _json.loads(ev.meta) if ev.meta else {}
        except Exception:
            meta_dict = {}
        box_id_val = meta_dict.get('box_id')
        if box_id_val is None:
            # Fallback to the stock item's production box
            try:
                si = StockItem.query.filter_by(datamatrix_code=ev.datamatrix_code).first()
            except Exception:
                si = None
            if si:
                try:
                    box_id_val = getattr(si, 'production_box_id', None)
                except Exception:
                    box_id_val = None
        if box_id_val is not None:
            try:
                # Convert to int when possible
                try:
                    box_id_int = int(box_id_val)
                except Exception:
                    box_id_int = box_id_val
                box_docs = Document.query.filter_by(owner_type='BOX', owner_id=box_id_int).all()
            except Exception:
                box_docs = []
            else:
                doc_objs.extend(box_docs or [])
    # Filter and deduplicate documents
    seen_doc_ids: set[int] = set()
    final_docs: list[Document] = []
    for doc in doc_objs:
        try:
            status = getattr(doc, 'status', '').upper() if doc else ''
        except Exception:
            status = ''
        if status not in ('CARICATO', 'APPROVATO'):
            continue
        try:
            doc_id = getattr(doc, 'id', None)
        except Exception:
            doc_id = None
        if not doc_id or doc_id in seen_doc_ids:
            continue
        seen_doc_ids.add(doc_id)
        final_docs.append(doc)
    # Build a map assigning the same list to every event id
    docs_map: dict[int, list[Document]] = {}
    for ev in events_for_code or []:
        docs_map[ev.id] = final_docs
    # Select documents for the requested event id
    event_docs: list[Document] = docs_map.get(ev_id, [])
    # Convert Document objects into display dictionaries
    docs_detail: list[dict[str, Any]] = []
    for doc in event_docs:
        try:
            raw_path = doc.url or ''
            category = os.path.basename(os.path.dirname(raw_path)) or 'varie'
        except Exception:
            category = 'varie'
        fname = os.path.basename(doc.url or '')
        # Build a download URL using the products blueprint so that files are
        # served as attachments.  Fall back to a static URL if this fails.
        url = ''
        try:
            url = url_for('products.download_file', filename=doc.url)
        except Exception:
            try:
                url = url_for('static', filename=doc.url)
            except Exception:
                url = doc.url or ''
        docs_detail.append({
            'name': _original_filename(fname),
            'url': url,
            'category': category,
            'real_name': fname,
        })
    # Deduplicate by display name while preserving order
    seen_names: set[str] = set()
    unique_details: list[dict[str, Any]] = []
    for d in docs_detail:
        n = d.get('name')
        if n in seen_names:
            continue
        seen_names.add(n)
        unique_details.append(d)
    docs_detail = unique_details

    # Validate each document URL and apply a fallback when necessary.
    # When a document path points to a non-existent file (either in
    # static or production folders) or the URL is empty, fall back to
    # using the download-by-name endpoint.  This ensures that users
    # can still download documents even when the stored path cannot be
    # resolved.  The real filename (``real_name``) is used in the
    # fallback because it corresponds to the actual file on disk.
    for d in docs_detail:
        url = d.get('url')
        missing_or_invalid = False
        try:
            if url and isinstance(url, str):
                # Check static files
                if url.startswith('/static/'):
                    rel = url[len('/static/'):]
                    fpath = os.path.join(current_app.static_folder, rel)
                    missing_or_invalid = not os.path.isfile(fpath)
                # Check production files served via inventory endpoint
                elif url.startswith('/inventory/download_production_file/'):
                    rel = url[len('/inventory/download_production_file/'):]
                    prod_root = os.path.join(current_app.root_path, 'Produzione')
                    fpath = os.path.join(prod_root, rel)
                    missing_or_invalid = not os.path.isfile(fpath)
        except Exception:
            missing_or_invalid = True
        if not url or missing_or_invalid:
            real = d.get('real_name') or d.get('name')
            try:
                d['url'] = url_for('inventory.download_document_by_name', filename=real)
            except Exception:
                d['url'] = f"/inventory/document_by_name/{real}"

    # Group documents by category using the same labels as the standard docs view
    CATEGORY_LABELS: dict[str, str] = {
        'qualita': 'Cert Qualita',
        'disegni': 'Disegni',
        'manuale': 'Manuale',
        'step': 'Step/Tavola',
        'tavola': 'Step/Tavola',
        'varie': 'Altri documenti',
        'db': 'Altri documenti',
    }
    docs_by_category: dict[str, list[dict[str, Any]]] = {}
    for d in docs_detail:
        raw_cat = (d.get('category') or 'varie').lower()
        label = CATEGORY_LABELS.get(raw_cat, raw_cat.capitalize())
        docs_by_category.setdefault(label, []).append(d)
    docs_by_category = dict(sorted(docs_by_category.items(), key=lambda kv: kv[0].lower()))
    # Generate DataMatrix image for display
    image_data = _generate_dm_image(dm_code)
    image_data_hr = image_data
    if image_data:
        try:
            import base64 as _b64
            buf = _b64.b64decode(image_data)
            if Image is not None:
                im = Image.open(io.BytesIO(buf))
                factor = 4
                new_size = (im.width * factor, im.height * factor)
                im = im.resize(new_size, Image.NEAREST)
                out = io.BytesIO()
                im.save(out, format='PNG')
                image_data_hr = _b64.b64encode(out.getvalue()).decode('ascii')
        except Exception:
            image_data_hr = image_data
    return render_template(
        'inventory/docs_page.html',
        product_id=product_id,
        dm_code=dm_code,
        docs_by_category=docs_by_category,
        component_name=component_name,
        image_data=image_data_hr,
        from_page=src,
    )


# -----------------------------------------------------------------------------
# Assemblies archive view
#
# This route displays the audit history for assembly events only.  Each
# assembly entry groups together the primary scan event for the assembly
# itself and any subsequent association events that link component
# stock items to the assembly.  The table includes columns mirroring
# the component archive but nests component rows under each assembly.

@inventory_bp.route('/product/<int:product_id>/archive/assemblies')
@login_required
def product_archive_assemblies_view(product_id: int):
    """Display the archive of assemblies built for a specific product.

    This view has been rewritten to remove reliance on DataMatrix scan events
    and instead derives assembly information directly from the filesystem.
    Every completed assembly build in the ``Produzione/Assiemi_completati``
    directory is represented as a row in the archive.  Rows are grouped by
    assembly name (product or sub‑assembly) and sorted by build timestamp.
    Each row can be expanded to reveal the components and their documents used
    during the build.  For each assembly and component we display the
    timestamp, DataMatrix image (synthetic QR code), name, description,
    user (from metadata) and a fixed action ("COSTRUZIONE" or "COMPONENTE").

    Args:
        product_id: Identifier of the product whose assemblies archive is
                    requested.

    Returns:
        Rendered HTML template displaying the assemblies archive.
    """
    # Retrieve the product or return 404 if not found
    product = Product.query.get_or_404(product_id)
    #
    # DB-based assembly archive.  Use ProductBuild and ProductBuildItem
    # tables to list historical builds for this product.  Each build
    # event produces a parent row and nested component rows.  A build
    # that lacks explicit ProductBuildItem entries falls back to the
    # current BOM definition to derive its components.
    from typing import Any
    # Import Document to gather component-level documents for stock items.  Use
    # aliases to avoid masking outer variables.
    from ...models import ProductBuild, ProductBuildItem, BOMLine, Structure, User, StockItem, Document, Product as _ProductModel
    # Retrieve all build records for this product, newest first.  If the
    # query fails (e.g., due to missing table) fall back to an empty
    # collection.  We defer filtering of consumed sub‑assemblies until
    # after the DataMatrix codes have been derived for each build.
    try:
        # Retrieve all build records for this product.  These include
        # finished product builds (where build.product_id == product.id)
        # as well as builds for any sub‑assemblies that belong to this
        # product's BOM.  We order builds newest first.
        builds = (
            ProductBuild.query
            .filter_by(product_id=product.id)
            .order_by(ProductBuild.created_at.desc())
            .all()
        )
    except Exception:
        builds = []

    # -------------------------------------------------------------------
    # NOTE: Do not exclude final product builds from the assembly archive.
    # Previous attempts to filter final builds caused legitimate assembly
    # histories to disappear when the assembly build was recorded against
    # the parent product.  Because ``ProductBuild`` records created via
    # ``build_assembly`` may use the parent product_id, we cannot reliably
    # distinguish between an assembly build and a finished product build
    # without additional metadata.  As such, we retain all builds here
    # and perform any necessary filtering only in the production history
    # view.  This ensures that assembly builds remain visible in the
    # ``Archivio assiemi`` section of the product page.

    # -------------------------------------------------------------------
    # Filter out builds that correspond to assemblies which have been
    # consumed as components of a larger assembly.  When a previously
    # built assembly is associated to a new parent (via an ASSOCIA scan),
    # its stock item will have a non‑null parent_code.  In that case we
    # exclude the build from the top‑level archive and instead display
    # the assembly nested under its parent.  The consumed assemblies are
    # identified by matching their DataMatrix codes against stock items
    # whose parent_code is populated and whose datamatrix indicates an
    # assembly (T=ASSIEME).  Filtering at this stage prevents the
    # consumed assemblies from appearing as standalone rows.
    # Build a set of DataMatrix codes that correspond to assemblies which have
    # been linked to a parent assembly.  When an assembly is consumed by a
    # higher‑level assembly (via the ASSOCIA scan), its stock item will
    # have a non‑null ``parent_code``.  We include all such codes here
    # regardless of the T= label so that assemblies built using an
    # incorrect or missing type (e.g., ``PARTE`` instead of ``ASSIEME``)
    # are still excluded from the top‑level archive.  Both the full
    # DataMatrix and a simplified component/type code are added to
    # ensure matches against synthetic codes derived later.  Parsing is
    # performed inline to avoid relying on helpers defined further
    # below.
    consumed_codes: set[str] = set()
    try:
        assoc_items = StockItem.query.filter(
            StockItem.parent_code.isnot(None)
        ).all()
    except Exception:
        assoc_items = []
    for si in assoc_items:
        dm = si.datamatrix_code or ''
        if not dm:
            continue
        # Add the full DataMatrix code to the consumed set
        consumed_codes.add(dm)
        # Also add a simplified P/T variant when possible.  This helps
        # match synthetic codes that omit extra segments such as S= or
        # DMV version prefixes.  The parsing looks for pipe‑delimited
        # segments beginning with ``P=`` and ``T=`` and uses those to
        # construct a simplified code.
        comp_val = None
        typ_val = None
        try:
            for seg in dm.split('|'):
                if seg.startswith('P='):
                    comp_val = seg.split('=', 1)[1]
                elif seg.startswith('T='):
                    typ_val = seg.split('=', 1)[1]
            if comp_val and typ_val:
                consumed_codes.add(f"P={comp_val}|T={typ_val}")
        except Exception:
            # Ignore parsing errors; fallback to only full code
            pass

    # Now filter the list of builds by skipping those whose assembly
    # datamatrix appears in the consumed_codes set.  Because the
    # datamatrix for a build may not be stored directly on the build, we
    # derive it using the same logic as below (checking stock items in
    # the production_box first and then falling back to a synthetic code).
    filtered_builds: list[ProductBuild] = []
    for b in builds:
        # Determine the DataMatrix code for this build.  When a build
        # originates from a production box, inspect all stock items in
        # that box and select the first DataMatrix code available.  Do
        # not restrict by T=ASSIEME so that assemblies created with
        # incorrect or missing type labels are still matched.  When
        # no stock item exists (e.g., builds recorded via BOM
        # without a production box), synthesise a code using the
        # product name and the ASSIEME label to maintain backward
        # compatibility.
        code = None
        try:
            box_id = getattr(b, 'production_box_id', None)
            if box_id:
                try:
                    cand_items = StockItem.query.filter_by(production_box_id=box_id).all()
                except Exception:
                    cand_items = []
                for si in cand_items or []:
                    dmcode = getattr(si, 'datamatrix_code', '') or ''
                    if dmcode:
                        code = dmcode
                        break
        except Exception:
            code = None
        if not code:
            # Fallback to a synthetic code using the product name
            code = f"P={product.name}|T=ASSIEME"
        # Determine the type encoded within the DataMatrix payload.  A
        # valid code contains pipe‑delimited segments such as ``P=<name>``
        # and ``T=<type>``.  When the type is ``PRODOTTO`` the build
        # corresponds to a finished product and should not be listed in
        # the assemblies archive.  Conversely, when the type is
        # ``ASSIEME`` or other values (including missing or malformed
        # segments), the build is considered an assembly and is kept.
        dm_type = None
        try:
            for seg in (code or '').split('|'):
                if seg.startswith('T='):
                    dm_type = seg.split('=', 1)[1]
                    break
        except Exception:
            dm_type = None
        # Exclude finished product builds from the assemblies archive
        if dm_type and dm_type.upper() == 'PRODOTTO':
            continue
        # Skip builds whose assembly code appears in the consumed set.
        # This check covers both full DataMatrix codes and simplified
        # P/T codes due to the way ``consumed_codes`` was populated.
        if code in consumed_codes:
            continue
        filtered_builds.append(b)

    # Use the filtered list for the remainder of the view.  This
    # prevents consumed sub‑assemblies from appearing as separate
    # entries in the archive.
    builds = filtered_builds
    assemblies: list[dict[str, Any]] = []
    def _desc(name: str) -> str:
        try:
            st = Structure.query.filter_by(name=name).first()
            return (st.description or '') if st else ''
        except Exception:
            return ''
    def _user_for(pb: ProductBuild) -> str:
        try:
            uid = getattr(pb, 'user_id', None)
            if uid:
                u = User.query.get(uid)
                if u:
                    username = getattr(u, 'username', None)
                    if username:
                        return username
                    legacy_email = getattr(u, 'email', None)
                    if legacy_email:
                        return legacy_email
                    return str(u.id)
        except Exception:
            pass
        return '—'
    # Helper to parse a DataMatrix payload into its component and type fields.
    def _parse_dm(code: str) -> dict[str, str]:
        result: dict[str, str] = {}
        for seg in (code or '').split('|'):
            if '=' in seg:
                k, v = seg.split('=', 1)
                if k == 'P':
                    result['component'] = v
                elif k == 'T':
                    result['type'] = v
        return result

    # ------------------------------------------------------------------
    # Helper to return static fields (name, description, revision) for a
    # given DataMatrix code.  When components are loaded or associated
    # via ScanEvent, a copy of the structure name, description and
    # revision is persisted in the event meta.  To prevent archives
    # from being influenced by subsequent changes to the anagrafiche,
    # this helper scans all events for the provided code in ascending
    # order and returns the first meta that contains any of these
    # fields.  If no such meta exists the returned dict is empty.
    def _static_fields_for_dm(dm_code: str) -> dict[str, Any]:
        """Retrieve persisted static name, description and revision for a DataMatrix code.

        Args:
            dm_code: The full DataMatrix payload (e.g. "P=ABC|T=PART").

        Returns:
            A dictionary with optional keys ``name``, ``description`` and
            ``revision`` when these values are stored in any ScanEvent
            meta for the provided DataMatrix.  If no event meta contains
            static fields, an empty dict is returned.
        """
        if not dm_code:
            return {}
        try:
            events_for_code = (
                ScanEvent.query
                .filter_by(datamatrix_code=dm_code)
                .order_by(ScanEvent.created_at.asc())
                .all()
            )
        except Exception:
            events_for_code = []
        import json as _json  # local import to parse meta
        for _evt in events_for_code or []:
            # Skip association events when deriving static fields.  ASSOCIA
            # events record the state of the anagrafica at the time of
            # association rather than at load, and should not alter the
            # historical snapshot for a component.
            try:
                act = getattr(_evt, 'action', None) or ''
                if act.upper() == 'ASSOCIA':
                    continue
            except Exception:
                pass
            meta_val = getattr(_evt, 'meta', None) or ''
            try:
                meta_dict = _json.loads(meta_val) if meta_val else {}
            except Exception:
                meta_dict = {}
            # Check for any static fields in this meta
            has_name = 'structure_name' in meta_dict
            has_desc = 'structure_description' in meta_dict
            has_rev = 'revision_label' in meta_dict
            if has_name or has_desc or has_rev:
                return {
                    'name': meta_dict.get('structure_name'),
                    'description': meta_dict.get('structure_description'),
                    'revision': meta_dict.get('revision_label'),
                }
        return {}

    # Recursively build component rows for a given ProductBuild.  This function
    # returns a list of dictionaries describing each immediate component and
    # attaches a nested ``components`` list to any entry that represents
    # another assembly.  Recursion is limited by ``max_depth`` to avoid
    # infinite loops in cyclic BOMs.
    def _build_component_rows(pb: ProductBuild, assembly_code: str, user_display: str, max_depth: int = 50) -> list[dict[str, Any]]:
        comp_rows: list[dict[str, Any]] = []
        # Determine which component products were expected for this build.  When
        # ProductBuildItem records exist for the current build, they list
        # the product ids of components consumed to assemble the parent product.
        # Use this to filter out erroneously associated stock items belonging
        # to other products (e.g. scanning a VITE while linking an ORING).  When
        # no build items exist, this set will remain empty until the
        # subsequent fallback to BOM definition populates it.
        # Build a set of expected product ids for this assembly.  When a build
        # record is available use its ProductBuildItem entries to determine
        # which component products belong to this assembly.  If no items are
        # recorded fall back to the BOM definition for the parent product.
        allowed_product_ids: set[int] = set()
        if pb is not None:
            # Try ProductBuildItem entries first
            try:
                build_items = ProductBuildItem.query.filter_by(build_id=pb.id).all()
            except Exception:
                build_items = []
            for bitem in build_items or []:
                # Each item defines a component product id via figlio_id or product_id
                # Use getattr fallback for compatibility across versions
                pid = None
                try:
                    pid = getattr(bitem, 'product_id', None)
                except Exception:
                    pid = None
                if pid:
                    allowed_product_ids.add(pid)
            if not allowed_product_ids:
                # Fall back to BOM definition when no build items exist
                try:
                    bom_lines = BOMLine.query.filter_by(padre_id=pb.product_id).all()
                except Exception:
                    bom_lines = []
                for bl in bom_lines:
                    try:
                        fid = getattr(bl, 'figlio_id', None)
                    except Exception:
                        fid = None
                    if fid:
                        allowed_product_ids.add(fid)
        # First attempt to gather associated stock items via parent_code
        # Retrieve stock items whose parent_code matches the current assembly code.  In some
        # cases the parent_code stored in the database may omit extra DataMatrix segments
        # (e.g. the S= serial component) and contain only the component/type portion (e.g.
        # "P=ASSY|T=ASSIEME").  Attempt an alternative lookup when an exact match
        # yields no results.
        try:
            associated_items = StockItem.query.filter_by(parent_code=assembly_code).all()
        except Exception:
            associated_items = []
        # Filter associated items to only those whose product_id is expected for this assembly
        if associated_items and allowed_product_ids:
            try:
                associated_items = [si for si in associated_items if si and getattr(si, 'product_id', None) in allowed_product_ids]
            except Exception:
                # On error do not filter
                pass
        # Fallback: when no associated items are found with the full assembly_code, try
        # matching a simplified parent code composed of the component and type fields.
        # Some older association records store the parent_code without the DataMatrix
        # version or serial segments (e.g. "P=ASSY|T=ASSIEME").  When no matches are
        # found for the full code, derive the component/type only code and search
        # again.  Filter the alternative matches by allowed_product_ids to avoid
        # unrelated components.  This fallback covers both legacy data and
        # synthetic assembly codes used when a physical DataMatrix does not exist.
        if not associated_items:
            try:
                parsed_ac = _parse_dm(assembly_code)
                comp_c = parsed_ac.get('component')
                typ_c = parsed_ac.get('type')
                if comp_c and typ_c:
                    alt_code = f"P={comp_c}|T={typ_c}"
                    if alt_code != assembly_code:
                        try:
                            associated_items_alt = StockItem.query.filter_by(parent_code=alt_code).all()
                        except Exception:
                            associated_items_alt = []
                        if associated_items_alt:
                            if allowed_product_ids:
                                try:
                                    associated_items_alt = [si for si in associated_items_alt if si and getattr(si, 'product_id', None) in allowed_product_ids]
                                except Exception:
                                    pass
                            associated_items = associated_items_alt
            except Exception:
                pass
        # Additional fallback: attempt to match parent_code with alternate forms
        # When associating components via DataMatrix scanning, the recorded parent_code
        # may omit the DMV prefix, the G= guiding segment or the serial (S=) segment.
        # To increase robustness, try matching these variants before falling back to
        # production box heuristics.  Variants are constructed by removing the first
        # segment when it starts with ``DM`` (e.g. DMV1), by excluding the ``G=`` segment
        # and by retaining only the component (P=) and type (T=) segments.  The first
        # variant that yields matches will be used.
        if not associated_items:
            try:
                segments = (assembly_code or '').split('|')
                if segments:
                    # Remove DMV prefix if present (e.g. DMV1)
                    seg_no_dm = segments[1:] if segments and segments[0].upper().startswith('DM') else segments[:]
                    # Remove guiding code segment (G=...) when present
                    seg_no_g = [s for s in seg_no_dm if not (s.startswith('G=') or s.startswith('g='))]
                    # Candidate variants to try
                    candidates: list[str] = []
                    # Variant with no DMV prefix
                    cand1 = '|'.join(seg_no_dm)
                    if cand1 and cand1 != assembly_code:
                        candidates.append(cand1)
                    # Variant with no G= segment
                    cand2 = '|'.join(seg_no_g)
                    if cand2 and cand2 != assembly_code and cand2 not in candidates:
                        candidates.append(cand2)
                    # Variant with only P= and T= segments
                    comp_seg = None
                    typ_seg = None
                    for s in seg_no_g:
                        if s.startswith('P='):
                            comp_seg = s
                        elif s.startswith('T='):
                            typ_seg = s
                    if comp_seg and typ_seg:
                        cand3 = '|'.join([comp_seg, typ_seg])
                        if cand3 and cand3 != assembly_code and cand3 not in candidates:
                            candidates.append(cand3)
                    # Attempt to fetch matches for each candidate
                    for cand in candidates:
                        try:
                            items_candidate = StockItem.query.filter_by(parent_code=cand).all()
                        except Exception:
                            items_candidate = []
                        if items_candidate:
                            # Apply allowed_product_ids filtering when defined
                            if allowed_product_ids:
                                try:
                                    items_filtered = [si for si in items_candidate if si and getattr(si, 'product_id', None) in allowed_product_ids]
                                except Exception:
                                    items_filtered = items_candidate
                            else:
                                items_filtered = items_candidate
                            if items_filtered:
                                associated_items = items_filtered
                                break
            except Exception:
                # ignore errors constructing candidate codes
                pass
        # Additional fallback: derive children from the same production box only
        # for true assemblies.  When no associated items are found via parent_code
        # lookup, the system would previously gather all other items in the same
        # production box as potential children.  However, this behaviour can
        # incorrectly treat parts as assemblies when they are built in a box with
        # unrelated items.  Restrict this heuristic to DataMatrix codes whose
        # type is 'ASSIEME' to avoid exploding parts.  This respects the
        # explicit associations the user has made.
        if not associated_items:
            try:
                parsed_box = _parse_dm(assembly_code)
                dm_type_box = (parsed_box.get('type') or '').upper()
                if dm_type_box == 'ASSIEME':
                    # pb may be None when called from BOM-only helper
                    if pb and getattr(pb, 'production_box_id', None):
                        box_id = pb.production_box_id
                        try:
                            box_items = StockItem.query.filter_by(production_box_id=box_id).all()
                        except Exception:
                            box_items = []
                        derived_children: list[StockItem] = []
                        for si in box_items or []:
                            # Skip the stock item representing this assembly itself
                            try:
                                if si.datamatrix_code and si.datamatrix_code == assembly_code:
                                    continue
                            except Exception:
                                pass
                            # Apply allowed_product_ids filtering when defined
                            try:
                                if allowed_product_ids and getattr(si, 'product_id', None) not in allowed_product_ids:
                                    continue
                            except Exception:
                                pass
                            # Accept valid stock items with a defined product
                            if si and getattr(si, 'product_id', None):
                                derived_children.append(si)
                        if derived_children:
                            associated_items = derived_children
            except Exception:
                pass
        if associated_items:
            # List each stock item separately rather than grouping by product id
            for si in associated_items:
                # Skip items that do not belong to the expected components for this build.
                # When allowed_product_ids is empty (no build items recorded), accept all.
                try:
                    if allowed_product_ids and si.product_id not in allowed_product_ids:
                        continue
                except Exception:
                    pass
                # Skip invalid or uninitialised stock items
                if not si or not si.product_id:
                    continue
                # Resolve the product and description for this stock item
                try:
                    comp_prod = _ProductModel.query.get(si.product_id)
                except Exception:
                    comp_prod = None
                if not comp_prod:
                    continue
                # Determine the DataMatrix code for the component.  When no explicit
                # code exists, synthesise one using the product name with a PART
                # type.  The DataMatrix is used both for rendering and for
                # matching nested component associations.
                dm_val = si.datamatrix_code or f"P={comp_prod.name}|T=PART"
                # Parse the DataMatrix payload into its segments.  Prefer the
                # encoded component name (P=) over the product name to ensure
                # nested assemblies display the correct identifier.
                parsed_dm = _parse_dm(dm_val)
                comp_code = parsed_dm.get('component') or (comp_prod.name if comp_prod else '')
                name_val = comp_code
                desc_val = _desc(name_val)
                # Override fallback name/description with static values from scan metadata.
                static_info = _static_fields_for_dm(dm_val)
                if static_info:
                    try:
                        s_name = static_info.get('name')
                        if s_name:
                            name_val = s_name
                    except Exception:
                        pass
                    try:
                        s_desc = static_info.get('description')
                        if s_desc:
                            desc_val = s_desc
                    except Exception:
                        pass
                # Preserve historical component identity: override name and
                # description with static values stored in ScanEvent meta when
                # available.  This prevents archive entries from changing when
                # the anagrafica is updated.  A helper defined in this view
                # returns the first persisted fields for the DataMatrix.
                static_info = _static_fields_for_dm(dm_val)
                if static_info:
                    try:
                        static_name = static_info.get('name')
                        if static_name:
                            name_val = static_name
                    except Exception:
                        pass
                    try:
                        static_desc = static_info.get('description')
                        if static_desc:
                            desc_val = static_desc
                    except Exception:
                        pass
                # Collect any documents uploaded for this stock item.  We render
                # the filenames as links when a static URL can be resolved; when
                # the file is stored outside of the Flask static folder the URL
                # may be unavailable (None), and the UI displays a disabled
                # placeholder instead.
                docs: list[tuple[str, str]] = []
                try:
                    doc_records = Document.query.filter_by(owner_type='STOCK', owner_id=si.id).all()
                except Exception:
                    doc_records = []
                for doc in doc_records:
                    url = ''
                    try:
                        url = url_for('static', filename=doc.url)
                    except Exception:
                        url = doc.url
                    fname_compiled = os.path.basename(doc.url or '')
                    docs.append((fname_compiled, url))
                # Include any documents associated with the production box of this stock item.
                # A component may have documents uploaded at the box level (owner_type='BOX').
                box_id = getattr(si, 'production_box_id', None)
                if box_id:
                    try:
                        box_docs = Document.query.filter_by(owner_type='BOX', owner_id=box_id).all()
                    except Exception:
                        box_docs = []
                    for doc in box_docs:
                        url = ''
                        try:
                            url = url_for('static', filename=doc.url)
                        except Exception:
                            url = doc.url
                        fname_compiled = os.path.basename(doc.url or '')
                        docs.append((fname_compiled, url))
                # Save base docs after retrieving from Document for this component and its box.
                # The legacy scanning below (which searches production and static folders)
                # will be ignored by resetting docs to this list before constructing the entry.
                base_docs = list(docs)
                # Append extra documents from the Produzione archivio.  This mirrors
                # the legacy behaviour of grouping quality/manual docs with
                # assembled components.  We locate the most recent archived
                # directory matching the sanitized component name and add all
                # files found within as documents without a resolvable URL.
                # Ensure that ``candidates`` is always defined for the fallback lookup below.  When
                # the archive directory does not exist or no component code is provided,
                # the ``candidates`` variable defined inside the try block may never be
                # assigned.  By initialising it here, we prevent a NameError when
                # referencing it in the static fallback.
                candidates: list[str] = []
                try:
                    # Gather documents from the Produzione archivio for this component.  Archive
                    # directory names may derive from secure_filename(comp_code), the raw component
                    # name, or lowercase variants; match prefixes case‑insensitively and select
                    # the most recent timestamp.
                    produzione_root = os.path.join(current_app.root_path, 'Produzione')
                    archivio_root = os.path.join(produzione_root, 'archivio')
                    extra_docs: list[tuple[str, str | None]] = []
                    if os.path.isdir(archivio_root) and comp_code:
                        candidates: list[str] = []
                        safe_name = secure_filename(comp_code)
                        if safe_name:
                            candidates.append(safe_name)
                        raw_name = comp_code.strip()
                        if raw_name and raw_name != safe_name:
                            candidates.append(raw_name)
                        lower_raw = raw_name.lower() if raw_name else ''
                        if lower_raw and lower_raw not in candidates:
                            candidates.append(lower_raw)
                        candidate_lowers = [c.lower() for c in candidates]
                        # Choose the most recently modified matching directory
                        latest_mtime: float | None = None
                        latest_dir: str | None = None
                        for dir_name in os.listdir(archivio_root):
                            name_lower = dir_name.lower()
                            matched_prefix: str | None = None
                            # Allow matching the prefix anywhere in the directory name, not only at
                            # the beginning.  Archive directories may include product codes or
                            # descriptors before or after the component code, so a strict
                            # startswith check would miss valid matches.
                            for prefix in candidate_lowers:
                                if (
                                    name_lower.startswith(prefix + '_')
                                    or name_lower.startswith(prefix)
                                    or prefix in name_lower
                                ):
                                    matched_prefix = prefix
                                    break
                            if not matched_prefix:
                                continue
                            dir_path = os.path.join(archivio_root, dir_name)
                            if not os.path.isdir(dir_path):
                                continue
                            try:
                                mtime = os.path.getmtime(dir_path)
                            except Exception:
                                mtime = None
                            if mtime is None:
                                continue
                            if latest_mtime is None or mtime > latest_mtime:
                                latest_mtime = mtime
                                latest_dir = dir_name
                        if latest_dir:
                            comp_arch_path = os.path.join(archivio_root, latest_dir)
                            for dt_name in os.listdir(comp_arch_path):
                                dt_path = os.path.join(comp_arch_path, dt_name)
                                if not os.path.isdir(dt_path):
                                    continue
                                for fname in os.listdir(dt_path):
                                    file_path = os.path.join(dt_path, fname)
                                    if os.path.isfile(file_path):
                                        try:
                                            rel = os.path.relpath(file_path, produzione_root)
                                            url = url_for('inventory.download_production_file', filepath=rel)
                                        except Exception:
                                            url = None
                                        extra_docs.append((fname, url))
                    if extra_docs:
                        docs.extend(extra_docs)
                    else:
                        # Fallback: if no production archive is found, search for compiled documents under static/documents
                        try:
                            static_root = os.path.join(current_app.static_folder, 'documents')
                            static_docs: list[tuple[str, str | None]] = []
                            for cname in candidates:
                                comp_static_path = os.path.join(static_root, cname)
                                if not os.path.isdir(comp_static_path):
                                    continue
                                for doc_folder in os.listdir(comp_static_path):
                                    doc_path = os.path.join(comp_static_path, doc_folder)
                                    if not os.path.isdir(doc_path):
                                        continue
                                    for fname in os.listdir(doc_path):
                                        fpath = os.path.join(doc_path, fname)
                                        if os.path.isfile(fpath):
                                            try:
                                                rel_static = os.path.relpath(fpath, current_app.static_folder)
                                                static_url = url_for('static', filename=rel_static)
                                            except Exception:
                                                static_url = None
                                            static_docs.append((fname, static_url))
                                # Stop after processing the first candidate that yields docs
                                if static_docs:
                                    break
                            if static_docs:
                                docs.extend(static_docs)
                        except Exception:
                            # Ignore any errors during static docs scanning
                            pass
                except Exception:
                    pass
                # Do not revert docs to base_docs here.  Keep any additional
                # documents discovered in production and static folders.  The
                # resulting list may contain duplicates which we remove below.
                # Deduplicate documents to avoid showing the same file twice when
                # it exists both in the database and in the production archives.
                docs = _dedupe_docs(docs)
                # Construct the base entry for this component.  By default the
                # action label is 'COMPONENTE'.  Nested assemblies override the
                # action implicitly by adding nested components.
                entry: dict[str, Any] = {
                    'timestamp': pb.created_at,
                    'datamatrix': dm_val,
                    'name': name_val,
                    'description': desc_val,
                    'user': user_display,
                    'action': 'COMPONENTE',
                    'docs': docs,
                    'image_data': _generate_dm_image(dm_val),
                }
                # Determine whether this component should be treated as an assembly.
                # It can be considered an assembly in three situations:
                #   1. The DataMatrix payload explicitly identifies it as an assembly (T=ASSIEME).
                #   2. There exist stock items whose parent_code equals this stock item's datamatrix_code,
                #      meaning other components were associated to this instance.
                #   3. A ProductBuild record exists for the component's product, indicating it has been
                #      built as an assembly at least once.  This covers cases where datamatrix codes
                #      are missing or use the PART type but the subassembly still has an internal BOM.
                parsed = _parse_dm(dm_val)
                is_assy_type = parsed.get('type', '').upper() == 'ASSIEME'
                has_children = False
                if si.datamatrix_code:
                    try:
                        # Check if any child stock items reference this assembly's datamatrix code
                        has_children = StockItem.query.filter_by(parent_code=si.datamatrix_code).first() is not None
                    except Exception:
                        has_children = False
                has_build = False
                try:
                    has_build = ProductBuild.query.filter_by(product_id=comp_prod.id).first() is not None
                except Exception:
                    has_build = False
                # Determine if a BOM exists for this component.  Even when a product has never
                # been built, a defined BOM indicates it acts as an assembly with children.
                has_bom = False
                try:
                    has_bom = BOMLine.query.filter_by(padre_id=comp_prod.id).first() is not None
                except Exception:
                    has_bom = False
                # Determine if this component should be treated as an assembly.  Assemblies
                # have their DataMatrix type equal to ASSIEME, or have explicit
                # children recorded via parent_code, or have a defined BOM.  A prior
                # version considered any product with a ProductBuild record (has_build)
                # as an assembly candidate, but this caused normal parts that are
                # manufactured to be exploded as if they contained sub‑components.
                # To respect the user’s explicit associations, drop the ``has_build``
                # check so only true assemblies are exploded.
                is_assembly_candidate = max_depth > 0 and (is_assy_type or has_children or has_bom)
                if is_assembly_candidate:
                    # Attempt to resolve the subassembly's product by name.  Use the component
                    # code from the DataMatrix when available, falling back to the product name.
                    # Use the product of the current stock item as the subassembly product.  In
                    # earlier versions the product was looked up by the component name parsed
                    # from the DataMatrix code.  However this can fail when naming discrepancies
                    # exist between the DataMatrix payload and the product record.  Since the
                    # ``StockItem`` already references its product via ``product_id``, use that
                    # directly to resolve the subassembly rather than performing a name‑based
                    # lookup.  Fall back to the name lookup only when the product cannot be
                    # resolved from the stock item.
                    sub_name = parsed.get('component') or (comp_prod.name if comp_prod else '')
                    sub_build: ProductBuild | None = None
                    # Attempt to derive the subassembly product directly from the stock item.
                    sub_prod = comp_prod
                    # If comp_prod is missing (unlikely), fall back to querying by the component name
                    if not sub_prod and sub_name:
                        try:
                            sub_prod = _ProductModel.query.filter_by(name=sub_name).first()
                        except Exception:
                            sub_prod = None
                    if sub_prod:
                        # Prefer to match the build that created this specific stock item via production_box_id
                        try:
                            pb_id = getattr(si, 'production_box_id', None)
                            if pb_id:
                                sub_build = (ProductBuild.query
                                             .filter_by(production_box_id=pb_id)
                                             .filter_by(product_id=sub_prod.id)
                                             .first())
                        except Exception:
                            sub_build = None
                        # Fallback to the most recent build for the subassembly product
                        if not sub_build:
                            try:
                                sub_build = (ProductBuild.query
                                             .filter_by(product_id=sub_prod.id)
                                             .order_by(ProductBuild.created_at.desc())
                                             .first())
                            except Exception:
                                sub_build = None
                    if sub_build:
                        # Recursively gather nested components for this subassembly.  Use the
                        # stock item's datamatrix_code when available; when it is empty,
                        # synthesise a deterministic code from the subassembly name.  Passing a
                        # NULL value as ``assembly_code`` would cause the helper to match all
                        # children with a NULL parent_code, hiding the real structure.  A
                        # synthetic code of the form ``P=<name>|T=ASSIEME`` prevents this and
                        # also enables matching against simplified parent codes.
                        code_for_child = si.datamatrix_code if si.datamatrix_code else f"P={sub_name}|T=ASSIEME"
                        nested_components = _build_component_rows(sub_build, code_for_child, _user_for(sub_build), max_depth=max_depth-1)
                        if nested_components:
                            entry['components'] = nested_components
                    elif sub_prod:
                        # When no ProductBuild exists for the subassembly, derive its children
                        # using the BOM definition.  This leverages the helper to build nested
                        # structures so that BOM-only assemblies and their descendants explode
                        # correctly in the archive.
                        nested_components = _build_bom_only_component_rows(sub_prod, user_display, pb.created_at, max_depth-1)
                        if nested_components:
                            entry['components'] = nested_components
                comp_rows.append(entry)
        else:
            # Fallback to ProductBuildItem entries when no stock items were associated
            try:
                items = ProductBuildItem.query.filter_by(build_id=pb.id).all()
            except Exception:
                items = []
            if not items:
                # Final fallback: derive components from the BOM definition of the parent product
                try:
                    bom_lines = BOMLine.query.filter_by(padre_id=pb.product_id).all()
                except Exception:
                    bom_lines = []
                # Construct lightweight objects with product_id and quantity_required attributes
                items = [type('X', (), {
                    'product_id': line.figlio_id,
                    'quantity_required': (line.quantita or 1)
                }) for line in bom_lines]
            for it in items:
                try:
                    comp_prod = _ProductModel.query.get(it.product_id)
                except Exception:
                    comp_prod = None
                if not comp_prod:
                    continue
                dm_val = f"P={comp_prod.name}|T=PART"
                qty_required = getattr(it, 'quantity_required', 1)
                # Build a quantity label appended to the action when quantity > 1
                try:
                    q_float = float(qty_required)
                    q_int = int(q_float)
                    qty_label = '' if (abs(q_float - q_int) < 1e-6 and q_int == 1) else f" × {q_float if abs(q_float - q_int) >= 1e-6 else q_int}"
                except Exception:
                    qty_label = '' if qty_required == 1 else f" × {qty_required}"
                # Parse the DataMatrix to extract the component name for display
                parsed_dm = _parse_dm(dm_val)
                comp_code = parsed_dm.get('component') or comp_prod.name
                name_val = comp_code
                desc_val = _desc(name_val)
                entry: dict[str, Any] = {
                    'timestamp': pb.created_at,
                    'datamatrix': dm_val,
                    'name': name_val,
                    'description': desc_val,
                    'user': user_display,
                    'action': f"COMPONENTE{qty_label}",
                    'docs': [],
                    'image_data': _generate_dm_image(dm_val),
                }
                # Determine if this fallback component should be treated as an assembly.  We
                # consider it an assembly either when the DataMatrix marks it as
                # such or when there exists at least one ProductBuild for this
                # product (indicating it has been assembled previously).  The
                # additional depth check prevents infinite recursion.
                parsed = _parse_dm(dm_val)
                is_assy_type = parsed.get('type', '').upper() == 'ASSIEME'
                has_build = False
                try:
                    has_build = ProductBuild.query.filter_by(product_id=comp_prod.id).first() is not None
                except Exception:
                    has_build = False
                # Also treat the fallback component as an assembly when it has a BOM defined.
                has_bom = False
                try:
                    has_bom = BOMLine.query.filter_by(padre_id=comp_prod.id).first() is not None
                except Exception:
                    has_bom = False
                if (is_assy_type or has_build or has_bom) and max_depth > 0:
                    try:
                        sub_prod = _ProductModel.query.get(comp_prod.id)
                    except Exception:
                        sub_prod = None
                    if sub_prod:
                        sub_build: ProductBuild | None = None
                        try:
                            sub_build = (ProductBuild.query
                                         .filter_by(product_id=sub_prod.id)
                                         .order_by(ProductBuild.created_at.desc())
                                         .first())
                        except Exception:
                            sub_build = None
                        if sub_build:
                            # Use a synthetic DataMatrix code for the subassembly
                            sub_code = f"P={sub_prod.name}|T=ASSIEME"
                            nested_components = _build_component_rows(sub_build, sub_code, _user_for(sub_build), max_depth=max_depth-1)
                            if nested_components:
                                entry['components'] = nested_components
                        elif has_bom and sub_prod:
                            # No build exists for the subassembly but a BOM is defined.  Use the
                            # helper to derive its children from the BOM recursively.  This allows
                            # nested BOM-only assemblies to explode correctly in the archive.
                            nested_components = _build_bom_only_component_rows(sub_prod, user_display, pb.created_at, max_depth-1)
                            if nested_components:
                                entry['components'] = nested_components
                comp_rows.append(entry)
        return comp_rows

    def _build_bom_only_component_rows(prod: _ProductModel, user_display: str, timestamp, max_depth: int) -> list[dict[str, Any]]:
        """
        Recursively build component rows for a product that has not been built (no ProductBuild)
        but has a defined BOM.  This helper mirrors _build_component_rows but derives
        children solely from BOM definitions.  It will mark child components as assemblies
        when they themselves have a build record or further BOM entries.

        Args:
            prod: Product to explode via its BOM structure.
            user_display: String identifying the user responsible for the parent assembly.
            timestamp: Timestamp associated with the parent assembly/build event.
            max_depth: Remaining recursion depth.  A value <= 0 stops recursion.

        Returns:
            A list of component dictionaries compatible with those returned by
            _build_component_rows, including nested 'components' lists when deeper
            structures exist.
        """
        from datetime import datetime as _dt  # local import to avoid circular issues
        # Base cases: no product provided or depth exhausted
        if max_depth <= 0 or not prod:
            return []
        # Retrieve BOM lines for the provided product
        try:
            bom_lines = BOMLine.query.filter_by(padre_id=prod.id).all()
        except Exception:
            bom_lines = []
        result: list[dict[str, Any]] = []
        for line in bom_lines:
            # Resolve the child product from the BOM line
            try:
                child_prod = _ProductModel.query.get(line.figlio_id)
            except Exception:
                child_prod = None
            if not child_prod:
                continue
            # Determine whether the child is an assembly via either a build or its own BOM
            has_child_build = False
            try:
                has_child_build = ProductBuild.query.filter_by(product_id=child_prod.id).first() is not None
            except Exception:
                has_child_build = False
            has_child_bom = False
            try:
                has_child_bom = BOMLine.query.filter_by(padre_id=child_prod.id).first() is not None
            except Exception:
                has_child_bom = False
            # Compose a DataMatrix code for the child: use ASSIEME type when it appears to be an assembly
            dm_type = 'ASSIEME' if (has_child_build or has_child_bom) else 'PART'
            child_dm = f"P={child_prod.name}|T={dm_type}"
            # Compute the quantity label, omitting multiplier when quantity is effectively one
            qty_required = line.quantita or 1
            try:
                qf = float(qty_required)
                qi = int(qf)
                qty_label = '' if (abs(qf - qi) < 1e-6 and qi == 1) else f" × {qf if abs(qf - qi) >= 1e-6 else qi}"
            except Exception:
                qty_label = '' if qty_required == 1 else f" × {qty_required}"
            # Derive display fields for the child
            parsed_child = _parse_dm(child_dm)
            child_code = parsed_child.get('component') or child_prod.name
            child_name = child_code
            child_desc = _desc(child_name)
            # Override child name and description with static values when
            # available from scan metadata.  This ensures nested BOM-only
            # components display their historical name and description if
            # previously loaded or associated in a ScanEvent.
            static_info = _static_fields_for_dm(child_dm)
            if static_info:
                try:
                    s_name = static_info.get('name')
                    if s_name:
                        child_name = s_name
                except Exception:
                    pass
                try:
                    s_desc = static_info.get('description')
                    if s_desc:
                        child_desc = s_desc
                except Exception:
                    pass
            child_entry: dict[str, Any] = {
                'timestamp': timestamp,
                'datamatrix': child_dm,
                'name': child_name,
                'description': child_desc,
                'user': user_display,
                'action': f"COMPONENTE{qty_label}",
                'docs': [],
                'image_data': _generate_dm_image(child_dm),
            }
            # Determine nested children if recursion depth permits
            if max_depth > 1:
                nested_components: list[dict[str, Any]] = []
                if has_child_build:
                    # Use the most recent build to derive nested structure
                    try:
                        sub_child_build = (ProductBuild.query
                                           .filter_by(product_id=child_prod.id)
                                           .order_by(ProductBuild.created_at.desc())
                                           .first())
                    except Exception:
                        sub_child_build = None
                    if sub_child_build:
                        nested_components = _build_component_rows(sub_child_build, child_dm, user_display, max_depth=max_depth-1)
                elif has_child_bom:
                    # Recursively derive children from the BOM for the subassembly
                    nested_components = _build_bom_only_component_rows(child_prod, user_display, timestamp, max_depth-1)
                if nested_components:
                    child_entry['components'] = nested_components
            result.append(child_entry)
        return result

    def _build_filesystem_component_rows(dir_path: str, parent_name: str, timestamp, user_display: str, depth: int = 50) -> list[dict[str, Any]]:
        """
        Recursively derive component rows from the filesystem for assemblies that have
        been archived without corresponding build records or BOM definitions.  When a
        component folder contains its own ``componenti`` subfolder, this helper
        traverses it to build a nested component hierarchy.  Names are resolved
        using the BOM of the parent product when available; otherwise the safe
        prefix is used.  Documentation links are generated similar to the
        top-level archive parser.

        Args:
            dir_path: Filesystem path to the archived assembly's root directory.
            parent_name: Name of the product/assembly corresponding to ``dir_path``.
            timestamp: Timestamp to assign to all component rows (fallback when no
                       individual timestamps are recorded).
            user_display: Name of the user associated with the parent assembly.
            depth: Remaining recursion depth to avoid infinite loops.  A depth <= 0
                   stops recursion.

        Returns:
            A list of component dictionaries compatible with the archive view,
            including nested ``components`` lists when deeper hierarchies exist.
        """
        from typing import Any as _Any
        result: list[dict[str, _Any]] = []
        if depth <= 0 or not dir_path:
            return result
        # Locate the 'componenti' subfolder for this archived assembly.  If absent
        # there are no nested components to derive.
        compi_dir = os.path.join(dir_path, 'componenti')
        if not os.path.isdir(compi_dir):
            return result
        # Attempt to build a map of safe prefixes to human-readable component names
        # using the BOM definition of the parent product.  When the BOM cannot be
        # resolved, the safe prefix will be used as the component name.
        comp_map: dict[str, str] = {}
        try:
            parent_prod = _ProductModel.query.filter_by(name=parent_name).first()
            if parent_prod:
                child_lines = BOMLine.query.filter_by(padre_id=parent_prod.id).all()
                for line in child_lines:
                    c_prod = _ProductModel.query.get(line.figlio_id)
                    if not c_prod:
                        continue
                    c_safe = secure_filename(c_prod.name) or f"id_{c_prod.id}"
                    comp_map[c_safe.lower()] = c_prod.name
                    comp_map[c_safe] = c_prod.name
                    raw_c = (c_prod.name or '').strip()
                    if raw_c:
                        comp_map[raw_c.lower()] = c_prod.name
        except Exception:
            pass
        # Determine the base path for relative URLs when generating document links
        try:
            production_base = os.path.join(current_app.root_path, 'Produzione')
        except Exception:
            production_base = ''
        # Iterate over each component directory in the 'componenti' folder
        for sub_dir_name in os.listdir(compi_dir):
            sub_path = os.path.join(compi_dir, sub_dir_name)
            if not os.path.isdir(sub_path):
                continue
            # Extract the safe prefix from '<safe>_<timestamp>' naming scheme
            sub_parts = sub_dir_name.rsplit('_', 1)
            sub_safe = sub_parts[0] if sub_parts else sub_dir_name
            # Resolve the human-friendly name via comp_map; fallback to the safe prefix
            sub_name = comp_map.get(sub_safe.lower(), comp_map.get(sub_safe, sub_safe))
            # Lookup description via Structure when available.  Then override
            # name and description with static values from scan metadata when
            # available to preserve historical data.  A synthetic DataMatrix
            # code is constructed below, so we cannot query static info yet.
            sub_desc = ''
            try:
                s_struct = Structure.query.filter_by(name=sub_name).first()
                if s_struct and getattr(s_struct, 'description', None):
                    sub_desc = s_struct.description
            except Exception:
                sub_desc = ''
            # Gather all document files under this component directory.  Each subfolder
            # (e.g. 'qualita', 'manuali') is traversed and files are appended as
            # (filename, url) tuples.  When a static URL cannot be resolved the
            # second element is None.
            sub_docs: list[tuple[str, str | None]] = []
            try:
                for dt_sub in os.listdir(sub_path):
                    dt_path = os.path.join(sub_path, dt_sub)
                    if not os.path.isdir(dt_path):
                        continue
                    for f in os.listdir(dt_path):
                        fpath = os.path.join(dt_path, f)
                        if not os.path.isfile(fpath):
                            continue
                        try:
                            rel = os.path.relpath(fpath, production_base)
                            url = url_for('inventory.download_production_file', filepath=rel)
                        except Exception:
                            url = None
                        sub_docs.append((f, url))
            except Exception:
                pass
            # Determine whether this component is itself an assembly.  We consider it an
            # assembly when it has an associated build record, its own BOM definition,
            # or a nested 'componenti' folder in the filesystem.
            has_sub_build = False
            has_sub_bom = False
            try:
                c_prod = _ProductModel.query.filter_by(name=sub_name).first()
            except Exception:
                c_prod = None
            if c_prod:
                try:
                    has_sub_build = ProductBuild.query.filter_by(product_id=c_prod.id).first() is not None
                except Exception:
                    has_sub_build = False
                try:
                    has_sub_bom = BOMLine.query.filter_by(padre_id=c_prod.id).first() is not None
                except Exception:
                    has_sub_bom = False
            fs_has_children = os.path.isdir(os.path.join(sub_path, 'componenti'))
            sub_dm_type = 'ASSIEME' if (has_sub_build or has_sub_bom or fs_has_children) else 'PART'
            sub_dm = f"P={sub_name}|T={sub_dm_type}"
            # Override the subassembly/component name and description with static
            # values from ScanEvent metadata when available.  This ensures
            # filesystem-derived components honour the historical data
            # captured during previous loads/associations.  Only update
            # sub_name and sub_desc if static values are provided.
            static_info = _static_fields_for_dm(sub_dm)
            if static_info:
                try:
                    s_name = static_info.get('name')
                    if s_name:
                        sub_name = s_name
                except Exception:
                    pass
                try:
                    s_desc = static_info.get('description')
                    if s_desc:
                        sub_desc = s_desc
                except Exception:
                    pass
            # Derive nested components: prefer database/build paths, otherwise fall back to filesystem recursion
            nested: list[dict[str, _Any]] = []
            if depth > 1:
                try:
                    if has_sub_build:
                        # Use the most recent build to derive children
                        sub_build = (ProductBuild.query
                                     .filter_by(product_id=c_prod.id)
                                     .order_by(ProductBuild.created_at.desc())
                                     .first()) if c_prod else None
                        if sub_build:
                            nested = _build_component_rows(sub_build, sub_dm, user_display, max_depth=depth-1)
                    if (not nested) and has_sub_bom:
                        # Derive children from BOM definitions
                        nested = _build_bom_only_component_rows(c_prod, user_display, timestamp, depth-1) if c_prod else []
                    if (not nested) and fs_has_children:
                        # Recursively derive children from the filesystem
                        nested = _build_filesystem_component_rows(sub_path, sub_name, timestamp, user_display, depth-1)
                except Exception:
                    nested = []
            # Compose the component entry
            entry: dict[str, _Any] = {
                'timestamp': timestamp,
                'datamatrix': sub_dm,
                'name': sub_name,
                'description': sub_desc,
                'user': '',
                'action': 'COMPONENTE',
                'docs': sub_docs,
                'image_data': _generate_dm_image(sub_dm),
            }
            if nested:
                entry['components'] = nested
            result.append(entry)
        return result

    # -------------------------------------------------------------------------------------------
    # Build assemblies archive with unique document assignments per event.
    # To ensure that documents uploaded during earlier builds remain associated with their
    # original events and are not replaced by newer uploads, we assign documents to the
    # earliest build where they appear.  Subsequent builds for the same DataMatrix
    # (assembly or component) will not display documents that were already shown on a
    # previous event.  This mirrors the logic used for the component archive and treats
    # the assemblies archive as an immutable history.
    from datetime import datetime as _dt
    # Track assigned document keys globally (across all assemblies and components).
    # Each document is represented by a key "<filename>|<url>".  Once a key has
    # been assigned to an event it will not appear again in subsequent events
    # regardless of the DataMatrix code.  This ensures historical builds remain
    # immutable and that documents do not reappear on later rows.
    # NOTE: Do not assign documents uniquely across all builds.  Each assembly
    # build should display its own documents independently rather than
    # suppressing duplicates across events.  Removing the global sets
    # ensures that documents uploaded for one build remain visible on
    # subsequent builds.
    assigned_global_asm_doc_keys: set[str] = set()
    assigned_global_comp_doc_keys: set[str] = set()
    def _filter_component_docs(rows: list[dict[str, Any]]) -> None:
        """
        Recursively filter document lists for component rows so that each document
        appears only on the earliest event across all components.  A global set
        of assigned document keys ensures that once a file has been displayed
        it is not shown again in later events.  This preserves the historical
        integrity of the archive and prevents documents from reappearing when
        the same component or another assembly is loaded subsequently.
        """
        for row in rows or []:
            filtered_docs: list[tuple[str, str]] = []
            for name, url in row.get('docs') or []:
                key = f"{name}|{url or ''}"
                if key not in assigned_global_comp_doc_keys:
                    assigned_global_comp_doc_keys.add(key)
                    filtered_docs.append((name, url))
            # Remove duplicate filenames while preserving order
            row['docs'] = _dedupe_docs(filtered_docs)
            # Recurse into nested components
            if row.get('components'):
                _filter_component_docs(row['components'])
    # Sort builds in chronological order (oldest first).  Assigning documents
    # to the earliest build ensures that each historical assembly record
    # retains the documents that were uploaded at that time.  Later builds
    # will display only newly uploaded documents and will not modify the
    # documentation of previous events.
    try:
        builds_sorted = sorted(builds, key=lambda x: (getattr(x, 'created_at', None) or _dt.min))
    except Exception:
        builds_sorted = builds
    assembly_rows: list[dict[str, Any]] = []
    for pb in builds_sorted:
        # Determine the DataMatrix code for the current assembly build.  We mirror the
        # original logic by selecting the first DataMatrix from the production box,
        # preferring a code matching the built product, then any code with T=ASSIEME,
        # and finally any non-empty code.  When no code is found we fall back to a
        # synthetic code using the product name and ASSIEME type.
        assembly_code: str | None = None
        try:
            box_id = getattr(pb, 'production_box_id', None)
            if box_id:
                try:
                    candidate_items = StockItem.query.filter_by(production_box_id=box_id).all()
                except Exception:
                    candidate_items = []
                # Prefer the stock item whose product_id matches the build product
                for si in candidate_items or []:
                    try:
                        if si.product_id == pb.product_id and si.datamatrix_code:
                            assembly_code = si.datamatrix_code
                            break
                    except Exception:
                        continue
                # Next prefer a DataMatrix explicitly labelled ASSIEME
                if not assembly_code:
                    for si in candidate_items or []:
                        dmcode = (si.datamatrix_code or '').upper()
                        if 'T=ASSIEME' in dmcode:
                            assembly_code = si.datamatrix_code
                            break
                # As a last resort pick the first available code
                if not assembly_code:
                    for si in candidate_items or []:
                        code_val = getattr(si, 'datamatrix_code', None)
                        if code_val:
                            assembly_code = code_val
                            break
        except Exception:
            assembly_code = None
        if not assembly_code:
            assembly_code = f"P={product.name}|T=ASSIEME"
        # Determine the user responsible for the build
        user_display = _user_for(pb)
        # Build component rows and apply unique document filtering
        # Increase the recursion depth for component explosion to support
        # deeply nested assemblies.  A high value (e.g. 50) ensures all
        # subassemblies are rendered in the archive.
        comp_rows = _build_component_rows(pb, assembly_code, user_display, max_depth=50)
        _filter_component_docs(comp_rows)
        # Resolve assembly name and description based on the DataMatrix or product
        parsed_top = _parse_dm(assembly_code)
        assembly_name = parsed_top.get('component') or product.name
        assembly_description = ''
        try:
            st = Structure.query.filter_by(name=assembly_name).first()
            if st and getattr(st, 'description', None):
                assembly_description = st.description
            else:
                assembly_description = product.description or _desc(assembly_name)
        except Exception:
            assembly_description = product.description or _desc(assembly_name)
        # Compute the action label reflecting quantity built
        qty_val = getattr(pb, 'qty', 1)
        try:
            qty_int = int(qty_val)
            if abs(float(qty_val) - 1.0) < 1e-6 and qty_int == 1:
                action_label = 'COSTRUZIONE'
            else:
                action_label = f"COSTRUZIONE × {qty_val}"
        except Exception:
            action_label = f"COSTRUZIONE × {qty_val}"
        # Gather documents for the assembly stock item and its production box
        asm_docs: list[tuple[str, str]] = []
        try:
            # Use the StockItem model imported for this view.  The previous reference
            # to ``_StockItem`` originated from another blueprint and would raise a
            # NameError when executed here.  Switching to ``StockItem`` ensures we
            # query the correct model for the current context.
            stock_item = StockItem.query.filter_by(datamatrix_code=assembly_code).first()
        except Exception:
            stock_item = None
        if stock_item:
            try:
                doc_records = Document.query.filter_by(owner_type='STOCK', owner_id=stock_item.id).all()
            except Exception:
                doc_records = []
            # Helper to return the stored filename without modification.  This ensures
            # that the user sees exactly the name of the file that was saved
            # (including any compiled suffix) in the archive views.
            def _derive_original(fname: str) -> str:
                return fname
            for doc in doc_records or []:
                # Only include documents that have been uploaded or approved
                try:
                    status = getattr(doc, 'status', '').upper() if doc else ''
                except Exception:
                    status = ''
                if status not in ('CARICATO', 'APPROVATO'):
                    continue
                url = ''
                try:
                    url = url_for('static', filename=doc.url)
                except Exception:
                    url = doc.url
                # Display the original filename when possible rather than the compiled name
                fname = os.path.basename(doc.url or '')
                asm_docs.append((_derive_original(fname), url))
        box_id = getattr(pb, 'production_box_id', None)
        if box_id:
            try:
                box_docs = Document.query.filter_by(owner_type='BOX', owner_id=box_id).all()
            except Exception:
                box_docs = []
            # Reuse the helper for deriving original filenames.  When a compiled suffix
            # is present, remove it and keep the last underscore‑separated token as the base name.
            def _derive_original2(fname: str) -> str:
                return fname
            for doc in box_docs or []:
                # Only include documents that have been uploaded or approved
                try:
                    status = getattr(doc, 'status', '').upper() if doc else ''
                except Exception:
                    status = ''
                if status not in ('CARICATO', 'APPROVATO'):
                    continue
                url = ''
                try:
                    url = url_for('static', filename=doc.url)
                except Exception:
                    url = doc.url
                fname = os.path.basename(doc.url or '')
                asm_docs.append((_derive_original2(fname), url))
        # In addition to documents linked via the database, attempt to locate any
        # production or compiled documents for this assembly on disk.  The Document
        # table may be empty in some installations, so replicate the legacy
        # filesystem scanning used for components.  Use the component name
        # extracted from the DataMatrix payload (or the assembly name) to build
        # candidate directory names.
        try:
            parsed_ac = _parse_dm(assembly_code)
        except Exception:
            parsed_ac = {}
        comp_name = parsed_ac.get('component') or assembly_name
        # Collect additional documents from the Produzione archivio and static folders
        extra_docs: list[tuple[str, str | None]] = []
        # Build candidate names for matching against archive directories
        candidates: list[str] = []
        try:
            from werkzeug.utils import secure_filename  # ensure imported
            produzione_root = os.path.join(current_app.root_path, 'Produzione')
            archivio_root = os.path.join(produzione_root, 'archivio')
            if os.path.isdir(archivio_root) and comp_name:
                safe_name = secure_filename(comp_name)
                raw_name = comp_name.strip()
                if safe_name:
                    candidates.append(safe_name)
                if raw_name and raw_name != safe_name:
                    candidates.append(raw_name)
                lower_raw = raw_name.lower() if raw_name else ''
                # Avoid duplicate candidates with different case
                if lower_raw and lower_raw not in [c.lower() for c in candidates]:
                    candidates.append(lower_raw)
                candidate_lowers = [c.lower() for c in candidates]
                # Choose the most recent directory matching any candidate
                latest_mtime: float | None = None
                latest_dir: str | None = None
                for dir_name in os.listdir(archivio_root):
                    name_lower = dir_name.lower()
                    matched = False
                    for prefix in candidate_lowers:
                        if (
                            name_lower.startswith(prefix + '_')
                            or name_lower.startswith(prefix)
                            or prefix in name_lower
                        ):
                            matched = True
                            break
                    if not matched:
                        continue
                    dir_path = os.path.join(archivio_root, dir_name)
                    if not os.path.isdir(dir_path):
                        continue
                    try:
                        mtime = os.path.getmtime(dir_path)
                    except Exception:
                        mtime = None
                    if mtime is None:
                        continue
                    if latest_mtime is None or mtime > latest_mtime:
                        latest_mtime = mtime
                        latest_dir = dir_name
                if latest_dir:
                    comp_arch_path = os.path.join(archivio_root, latest_dir)
                    for dt_name in os.listdir(comp_arch_path):
                        dt_path = os.path.join(comp_arch_path, dt_name)
                        if not os.path.isdir(dt_path):
                            continue
                        for fname in os.listdir(dt_path):
                            file_path = os.path.join(dt_path, fname)
                            if os.path.isfile(file_path):
                                try:
                                    # Build a URL for downloading this file via the inventory blueprint
                                    rel = os.path.relpath(file_path, produzione_root)
                                    url_f = url_for('inventory.download_production_file', filepath=rel)
                                except Exception:
                                    url_f = None
                                extra_docs.append((fname, url_f))
        except Exception:
            # Ignore any errors during production archive scanning
            pass
        # Fallback: search compiled documents under static/documents when no archive docs found
        if not extra_docs:
            try:
                static_root = os.path.join(current_app.static_folder, 'documents')
                for cname in candidates:
                    comp_static_path = os.path.join(static_root, cname)
                    if not os.path.isdir(comp_static_path):
                        continue
                    static_docs: list[tuple[str, str | None]] = []
                    for doc_folder in os.listdir(comp_static_path):
                        doc_path = os.path.join(comp_static_path, doc_folder)
                        if not os.path.isdir(doc_path):
                            continue
                        for fname in os.listdir(doc_path):
                            fpath = os.path.join(doc_path, fname)
                            if os.path.isfile(fpath):
                                try:
                                    rel_static = os.path.relpath(fpath, current_app.static_folder)
                                    static_url = url_for('static', filename=rel_static)
                                except Exception:
                                    static_url = None
                                static_docs.append((fname, static_url))
                    if static_docs:
                        extra_docs.extend(static_docs)
                        break
            except Exception:
                # Ignore any errors scanning static docs
                pass
        # Append any additional docs from the filesystem
        if extra_docs:
            try:
                asm_docs.extend(extra_docs)
            except Exception:
                pass
        # Remove duplicate filenames within this build while preserving order.  Do not
        # suppress documents that may have appeared in previous builds so that
        # each assembly row retains its complete document history.
        unique_asm_docs: list[tuple[str, str]] = _dedupe_docs(asm_docs)
        # Append the assembled row to our list
        assembly_rows.append({
            'timestamp': pb.created_at,
            'datamatrix': assembly_code,
            'name': assembly_name,
            'description': assembly_description,
            'user': user_display,
            'action': action_label,
            'docs': unique_asm_docs,
            'image_data': _generate_dm_image(assembly_code),
            'components': comp_rows,
            # Preserve the production box id for this build.  The UI uses this
            # value to filter documents by the originating box when navigating
            # to the document view.  Without passing box_id, the docs view
            # cannot restrict results to those uploaded for this specific build.
            'box_id': getattr(pb, 'production_box_id', None),
        })
    # Sort rows in descending order by timestamp for display
    try:
        assemblies = sorted(assembly_rows, key=lambda r: (r['timestamp'] or _dt.min), reverse=True)
    except Exception:
        assemblies = assembly_rows
    # -------------------------------------------------------------------
    # Augment each assembly and component row with its revision label.  Since
    # assembly and component entries are derived from ProductBuild and
    # StockItem relationships rather than scan events, they lack the meta
    # information stored in ScanEvent.  To maintain historical accuracy, we
    # compute the revision from the current Structure definitions.  This
    # revision reflects the state of the component at display time; if
    # revisions are recorded at the time of the build, they should be
    # persisted separately (e.g. via ScanEvent meta) and referenced here.
    from ...models import Structure, ScanEvent  # ensure Structure and ScanEvent are available
    import json as _json  # JSON parser for meta
    def _add_revision(rows: list[dict[str, Any]]) -> None:
        """
        Attach a revision label to each row in the assemblies archive.

        Prefer the revision stored in the earliest ScanEvent meta for the
        component's DataMatrix code.  When no ScanEvent contains a
        revision_label, fall back to the current Structure revision.
        Recursively update nested component rows.
        """
        for row in rows or []:
            # Determine the DataMatrix code associated with this row
            try:
                dm_code = row.get('datamatrix')
            except Exception:
                dm_code = None
            revision_val = ''
            # Examine the scan events for this DataMatrix code to extract the
            # earliest recorded revision.  Instead of relying solely on the
            # very first event (which may predate persistence of revision
            # information), iterate over all events in ascending order and
            # select the first one that contains a ``revision_label`` in
            # its meta.  This ensures that once a revision has been
            # recorded, subsequent changes to the Structure do not
            # retroactively alter the archive.  When no event provides a
            # revision_label, the value remains empty and a fallback below
            # will use the current Structure revision.
            if dm_code:
                try:
                    events_for_code = (
                        ScanEvent.query
                        .filter_by(datamatrix_code=dm_code)
                        .order_by(ScanEvent.id.asc())
                        .all()
                    )
                except Exception:
                    events_for_code = []
                for _evt in events_for_code or []:
                    # Skip association events when determining revision.  The
                    # revision at the time of load should be used for the
                    # archive; ASSOCIA events reflect the revision of
                    # anagrafica at association time which may be newer.
                    try:
                        act = getattr(_evt, 'action', None) or ''
                        if act.upper() == 'ASSOCIA':
                            continue
                    except Exception:
                        pass
                    try:
                        meta_val = _evt.meta or ''
                        meta_dict = _json.loads(meta_val) if meta_val else {}
                        rev_from_meta = meta_dict.get('revision_label') or ''
                        if rev_from_meta:
                            revision_val = rev_from_meta
                            break
                    except Exception:
                        # Skip malformed meta and continue to next event
                        continue
            # When no revision information is present in any scan event meta,
            # attempt to derive it from persisted scan metadata via the
            # helper.  This helper searches across all ScanEvent meta for
            # static fields.  Avoid falling back to the current Structure
            # revision so that archives remain immutable even when the
            # anagrafica changes.  If no revision is discovered the
            # revision remains blank.
            if not revision_val:
                try:
                    static_info_rev = _static_fields_for_dm(dm_code)
                    if static_info_rev:
                        rev_static = static_info_rev.get('revision') or ''
                        if rev_static:
                            revision_val = rev_static
                except Exception:
                    pass
            try:
                row['revision'] = revision_val or ''
            except Exception:
                pass
            # Recurse into child components
            try:
                children = row.get('components')
            except Exception:
                children = None
            if children:
                _add_revision(children)
    try:
        _add_revision(assemblies)
    except Exception:
        pass
    return render_template(
        'inventory/product_archive_assemblies.html',
        product=product,
        assemblies=assemblies,
        active_tab='archive_assiemi'
    )

    # ---------------------------------------------------------------------------
    # Legacy assembly archive implementation
    # The code below retains the original filesystem-based and fallback logic for
    # building the assemblies archive.  It has been superseded by the unique
    # document assignment implementation above.  To prevent syntax and
    # indentation errors arising from the dead code, it is wrapped inside a
    # triple-quoted string literal.  This effectively comments out the legacy
    # code while preserving it for reference.  The string is never used.
    '''
        # Determine the DataMatrix code for the current assembly build.
        #
        # Previously this logic looked for a DataMatrix containing ``T=ASSIEME``
        # within the stock items of the build's production box and defaulted
        # to a synthetic ``P=<product>|T=ASSIEME`` when none was found.  This
        # approach failed when assemblies were created with a DataMatrix
        # labelled ``T=PARTE`` or omitted the type segment altogether: the
        # parent_code stored on child stock items would reference the true
        # DataMatrix, but the archive would attempt to match the synthetic
        # code, resulting in no children being displayed.  To correct this,
        # we now select the first available DataMatrix code from the
        # production box without filtering by its type.  Only when no
        # stock item exists (e.g. builds recorded outside of the production
        # box workflow) do we fall back to a synthetic code using the
        # product name and the ``ASSIEME`` type.  This keeps the archive
        # compatible with older builds while preserving correct parent/child
        # associations for assemblies built as parts.
        assembly_code: str | None = None
        try:
            box_id = getattr(b, 'production_box_id', None)
            if box_id:
                # Retrieve all stock items loaded in this production box.  We
                # attempt to select the DataMatrix corresponding to the
                # assembly itself by matching the stock item's product_id to
                # the built product.  If such a match cannot be found we
                # fall back to a more permissive search.
                try:
                    candidate_items = StockItem.query.filter_by(production_box_id=box_id).all()
                except Exception:
                    candidate_items = []
                # First pass: look for the stock item whose product_id equals
                # the assembly being built.  This ensures we pick the
                # DataMatrix of the parent assembly rather than a child
                # component when multiple items reside in the same box.
                for si in candidate_items or []:
                    try:
                        if si.product_id == b.product_id and si.datamatrix_code:
                            assembly_code = si.datamatrix_code
                            break
                    except Exception:
                        continue
                # Second pass: no exact product match found, so look for an
                # explicit assembly type in the DataMatrix.  Older builds may
                # correctly mark assemblies with ``T=ASSIEME`` even when the
                # product_id mismatch occurs.  Selecting such a code avoids
                # incorrectly using a child part as the parent.
                if not assembly_code:
                    for si in candidate_items or []:
                        dmcode = (si.datamatrix_code or '').upper()
                        if 'T=ASSIEME' in dmcode:
                            assembly_code = si.datamatrix_code
                            break
                # Final pass: as a last resort pick the first non-empty code.
                if not assembly_code:
                    for si in candidate_items or []:
                        if getattr(si, 'datamatrix_code', None):
                            assembly_code = si.datamatrix_code
                            break
        except Exception:
            assembly_code = None
        # Fallback to a synthetic assembly code when no DataMatrix is found.  This
        # maintains compatibility with legacy builds lacking a production box.
        if not assembly_code:
            assembly_code = f"P={product.name}|T=ASSIEME"
        user_display = _user_for(b)
        # Build component rows with nested subassemblies
        # Increase max_depth to allow deeper nested assemblies to be exploded in the archive.
        # The previous limit of 3 prevented subassemblies inside subassemblies from being displayed.
        # Use a high max_depth to fully explode nested subassemblies.  A value
        # of 50 avoids arbitrary cutoffs and enables unlimited expansion.
        comp_rows = _build_component_rows(b, assembly_code, user_display, max_depth=50)
        # Determine display name and description for this assembly.  Instead of
        # always using the product name, resolve the structure (or product)
        # name associated with the DataMatrix code.  When the DataMatrix
        # encodes a different component, use that as the name.
        # Parse the DataMatrix to extract the component name for the assembly
        parsed_top = _parse_dm(assembly_code)
        assembly_name = parsed_top.get('component') or product.name
        assembly_description = ''
        try:
            # Look up structure description if available
            st = Structure.query.filter_by(name=assembly_name).first()
            if st and getattr(st, 'description', None):
                assembly_description = st.description
            else:
                # Fall back to product description
                assembly_description = product.description or _desc(assembly_name)
        except Exception:
            assembly_description = product.description or _desc(assembly_name)
        # Compute quantity label for the assembly action
        qty_val = getattr(b, 'qty', 1)
        try:
            qty_int = int(qty_val)
            if abs(float(qty_val) - 1.0) < 1e-6 and qty_int == 1:
                action_label = 'COSTRUZIONE'
            else:
                action_label = f"COSTRUZIONE × {qty_val}"
        except Exception:
            action_label = f"COSTRUZIONE × {qty_val}"
        # ------------------------------------------------------------------
        # Assemble the list of documents uploaded during production of this assembly.
        # Legacy scanning of the ``Produzione/Assiemi_completati`` and static folders has been removed.
        asm_docs: list[tuple[str, str]] = []
        # Include documents linked directly to the finished assembly stock item.
        try:
            stock_item = _StockItem.query.filter_by(datamatrix_code=assembly_code).first()
        except Exception:
            stock_item = None
        if stock_item:
            try:
                doc_records = Document.query.filter_by(owner_type='STOCK', owner_id=stock_item.id).all()
            except Exception:
                doc_records = []
            for doc in doc_records:
                url = ''
                try:
                    url = url_for('static', filename=doc.url)
                except Exception:
                    url = doc.url
                asm_docs.append((os.path.basename(doc.url), url))
        # Append any box-level documents associated with this build.  When operators
        # upload documents via the production box rather than per-stock-item, they
        # are stored in the Document table with owner_type='BOX' and the
        # production_box_id as owner_id.  Include such documents so that
        # assembly-level manuals and quality reports appear in the archive.
        try:
            box_id = getattr(b, 'production_box_id', None)
        except Exception:
            box_id = None
        if box_id:
            try:
                box_docs = Document.query.filter_by(owner_type='BOX', owner_id=box_id).all()
            except Exception:
                box_docs = []
            for doc in box_docs:
                # Build a static URL for the document
                url = ''
                try:
                    url = url_for('static', filename=doc.url)
                except Exception:
                    url = doc.url
                asm_docs.append((os.path.basename(doc.url), url))
        # Finally append the assembly row with its compiled documentation.
        # Remove duplicate document entries before attaching them to the row.
        asm_docs = _dedupe_docs(asm_docs)
        # Include the production box id in the assembly entry.  This value
        # is required by the docs link in the template to filter documents
        # associated with the originating box.  When the build does not
        # originate from a production box this value may be None.
        assemblies.append({
            'timestamp': b.created_at,
            'datamatrix': assembly_code,
            'name': assembly_name,
            'description': assembly_description,
            'user': user_display,
            'action': action_label,
            'docs': asm_docs,
            'image_data': _generate_dm_image(assembly_code),
            'components': comp_rows,
            'box_id': getattr(b, 'production_box_id', None),
        })
    return render_template(
        'inventory/product_archive_assemblies.html',
        product=product,
        assemblies=assemblies,
        active_tab='archive_assiemi'
    )
    # Legacy filesystem-based implementation follows but is unreachable
    # Directory where completed assemblies are stored on disk.  If this
    # directory does not exist the archive will be empty.
    produzione_root = os.path.join(current_app.root_path, 'Produzione', 'Assiemi_completati')
    # ---------------------------------------------------------------------
    # Build a mapping of directory prefixes to the human‑readable assembly
    # names for the current product.  We include the product itself and
    # recursively all BOM children so that nested assemblies are covered.
    # Both sanitized (safe) and raw names are stored lowercased to
    # perform case‑insensitive matching against directory names on disk.
    target_names: dict[str, str] = {}

    def _collect_product_names(prod: Product, visited: set[int]) -> None:
        """Recursively collect all product names in the BOM tree.

        Args:
            prod: Product to process.
            visited: Set of product IDs already processed to avoid cycles.
        """
        if not prod or prod.id in visited:
            return
        visited.add(prod.id)
        # Safe and raw names for this product
        safe = secure_filename(prod.name) or f"id_{prod.id}"
        target_names[safe.lower()] = prod.name
        raw = (prod.name or '').strip()
        if raw:
            target_names[raw.lower()] = prod.name
        # Iterate through BOM children
        try:
            child_lines = BOMLine.query.filter_by(padre_id=prod.id).all()
            for line in child_lines:
                child_prod = Product.query.get(line.figlio_id)
                if child_prod:
                    _collect_product_names(child_prod, visited)
        except Exception:
            pass

    # Populate the target_names map
    _collect_product_names(product, set())

    # We will collect unique rows based on the directory prefix and timestamp to
    # avoid duplicates.
    seen: set[tuple[str, int]] = set()
    if os.path.isdir(produzione_root):
        # Iterate through all directories in the production archive
        for dir_name in os.listdir(produzione_root):
            dir_path = os.path.join(produzione_root, dir_name)
            if not os.path.isdir(dir_path):
                continue
            name_lower = dir_name.lower()
            matched_prefix = None
            assembly_name = None
            # Determine if this directory corresponds to one of the target
            # assembly names by checking for a prefix followed by an underscore.
            for prefix, human_name in target_names.items():
                if name_lower.startswith(f"{prefix}_"):
                    matched_prefix = prefix
                    assembly_name = human_name
                    break
            # Skip if no prefix matched
            if not matched_prefix or not assembly_name:
                continue
            # Extract the timestamp portion after the underscore.  If it cannot
            # be parsed as an integer the directory is ignored.
            suffix = dir_name[len(matched_prefix) + 1:]
            try:
                ts_int = int(suffix)
                from datetime import datetime
                timestamp = datetime.fromtimestamp(ts_int)
            except Exception:
                continue
            # Avoid processing the same prefix and timestamp multiple times
            key_tuple = (matched_prefix, ts_int)
            if key_tuple in seen:
                continue
            seen.add(key_tuple)
            # Synthetic DataMatrix code for this assembly
            dm_code = f"P={assembly_name}|T=ASSIEME"
            # Description lookup from Structure
            description = ''
            try:
                struct = Structure.query.filter_by(name=assembly_name).first()
                if struct and getattr(struct, 'description', None):
                    description = struct.description
            except Exception:
                pass
            # Determine the user who built this assembly and any static metadata from meta.json
            user_display = ''
            try:
                meta_path = os.path.join(dir_path, 'meta.json')
                if os.path.isfile(meta_path):
                    import json as _json
                    with open(meta_path, 'r', encoding='utf-8') as fp:
                        meta_data = _json.load(fp)
                    # Prefer stored structure description when available.  This allows
                    # assembly descriptions captured at build time to remain
                    # unchanged when the underlying Structure record is updated later.
                    try:
                        if meta_data.get('structure_description'):
                            description = meta_data['structure_description']
                    except Exception:
                        pass
                    # Determine the user display.  Prefer the stored username field
                    # if present.  Fall back to resolving the user_id into a username
                    # (or legacy email for backwards compatibility).
                    if meta_data.get('user_username'):
                        user_display = meta_data['user_username']
                    elif meta_data.get('username'):
                        user_display = meta_data['username']
                    elif meta_data.get('user_email'):
                        # Legacy key from previous versions that stored the email address.
                        user_display = meta_data['user_email']
                    elif meta_data.get('user_id'):
                        uid = meta_data['user_id']
                        try:
                            uid_int = int(uid)
                            from ...models import User as _User  # type: ignore
                            usr = _User.query.get(uid_int)
                            if usr:
                                username = getattr(usr, 'username', None)
                                if username:
                                    user_display = username
                                else:
                                    legacy_email = getattr(usr, 'email', None)
                                    if legacy_email:
                                        user_display = legacy_email
                                    else:
                                        user_display = str(uid)
                            else:
                                user_display = str(uid)
                        except Exception:
                            user_display = str(uid)
            except Exception:
                pass
            # Gather assembly-level documents (all subfolders except 'componenti')
            asm_docs: list[tuple[str, str | None]] = []
            try:
                production_base = os.path.join(current_app.root_path, 'Produzione')
                for subdir in os.listdir(dir_path):
                    subpath = os.path.join(dir_path, subdir)
                    if not os.path.isdir(subpath) or subdir.lower() == 'componenti':
                        continue
                    for fname in os.listdir(subpath):
                        fpath = os.path.join(subpath, fname)
                        if not os.path.isfile(fpath):
                            continue
                        try:
                            rel_path = os.path.relpath(fpath, production_base)
                            file_url = url_for('inventory.download_production_file', filepath=rel_path)
                        except Exception:
                            file_url = None
                        asm_docs.append((fname, file_url))
            except Exception:
                pass
            # Build a map of safe prefixes to component names for this assembly.  If the
            # assembly corresponds to a known product we inspect its BOM to
            # determine component names; otherwise we fall back to using the
            # directory names directly.
            comp_map: dict[str, str] = {}
            try:
                child_product = Product.query.filter_by(name=assembly_name).first()
                if child_product:
                    child_lines = BOMLine.query.filter_by(padre_id=child_product.id).all()
                    for line in child_lines:
                        c_prod = Product.query.get(line.figlio_id)
                        if not c_prod:
                            continue
                        c_safe = secure_filename(c_prod.name) or f"id_{c_prod.id}"
                        comp_map[c_safe.lower()] = c_prod.name
                        comp_map[c_safe] = c_prod.name
                        raw_c = (c_prod.name or '').strip()
                        if raw_c:
                            comp_map[raw_c.lower()] = c_prod.name
            except Exception:
                pass
            # Parse component documents under the 'componenti' subfolder
            comp_rows: list[dict[str, Any]] = []
            compi_dir = os.path.join(dir_path, 'componenti')
            if os.path.isdir(compi_dir):
                for comp_dir_name in os.listdir(compi_dir):
                    comp_dir_path = os.path.join(compi_dir, comp_dir_name)
                    if not os.path.isdir(comp_dir_path):
                        continue
                    # Each component folder is named <safe_prefix>_<timestamp>.  Extract the safe prefix.
                    comp_parts = comp_dir_name.rsplit('_', 1)
                    comp_safe = comp_parts[0] if comp_parts else comp_dir_name
                    # Look up the human name using comp_map; fallback to the safe name
                    comp_name = comp_map.get(comp_safe.lower(), comp_map.get(comp_safe, comp_safe))
                    # Fetch description from Structure
                    comp_desc = ''
                    try:
                        c_struct = Structure.query.filter_by(name=comp_name).first()
                        if c_struct and getattr(c_struct, 'description', None):
                            comp_desc = c_struct.description
                    except Exception:
                        pass
                    # Collect documents for this component
                    comp_docs: list[tuple[str, str | None]] = []
                    try:
                        for dt_sub in os.listdir(comp_dir_path):
                            dt_path = os.path.join(comp_dir_path, dt_sub)
                            if not os.path.isdir(dt_path):
                                continue
                            for f in os.listdir(dt_path):
                                fpath = os.path.join(dt_path, f)
                                if not os.path.isfile(fpath):
                                    continue
                                try:
                                    rel = os.path.relpath(fpath, production_base)
                                    url = url_for('inventory.download_production_file', filepath=rel)
                                except Exception:
                                    url = None
                                comp_docs.append((f, url))
                    except Exception:
                        pass
                    # Determine whether this component should be treated as an assembly.
                    # Look up the corresponding product record by name.  Use the
                    # `_ProductModel` alias imported earlier to avoid masking issues.
                    comp_prod = None
                    try:
                        comp_prod = _ProductModel.query.filter_by(name=comp_name).first()
                    except Exception:
                        comp_prod = None
                    # Evaluate if the component has its own build(s) or BOM definition.  When
                    # either exists we treat the component as an assembly (ASSIEME) and
                    # attempt to explode its children.  Otherwise it remains a PART.
                    has_build = False
                    has_bom = False
                    if comp_prod:
                        try:
                            has_build = ProductBuild.query.filter_by(product_id=comp_prod.id).first() is not None
                        except Exception:
                            has_build = False
                        try:
                            has_bom = BOMLine.query.filter_by(padre_id=comp_prod.id).first() is not None
                        except Exception:
                            has_bom = False
                    dm_type = 'ASSIEME' if (has_build or has_bom) else 'PART'
                    comp_dm = f"P={comp_name}|T={dm_type}"
                    # Recursively derive nested components when this entry is an assembly.
                    nested_components = []  # type: list[dict[str, Any]]
                    # Allow deeper recursion into filesystem-based component directories.
                    # A high value (e.g. 50) prevents premature cutoff when assemblies
                    # contain nested subassemblies.
                    fs_max_depth = 50
                    if fs_max_depth > 1:
                        try:
                            if (has_build or has_bom):
                                # Prefer build-derived children when available
                                if has_build:
                                    sub_build = (ProductBuild.query
                                                 .filter_by(product_id=comp_prod.id)
                                                 .order_by(ProductBuild.created_at.desc())
                                                 .first())
                                    if sub_build:
                                        nested_components = _build_component_rows(sub_build, comp_dm, user_display, max_depth=fs_max_depth-1)
                                # Fall back to BOM-only when no build-derived children were found
                                if (not nested_components) and has_bom:
                                    nested_components = _build_bom_only_component_rows(comp_prod, user_display, timestamp, fs_max_depth-1)
                            # If neither build nor BOM produced children, attempt filesystem recursion
                            if (not nested_components) and os.path.isdir(os.path.join(comp_dir_path, 'componenti')):
                                nested_components = _build_filesystem_component_rows(comp_dir_path, comp_name, timestamp, user_display, fs_max_depth-1)
                        except Exception:
                            nested_components = []
                    entry = {
                        'timestamp': timestamp,
                        'datamatrix': comp_dm,
                        'name': comp_name,
                        'description': comp_desc,
                        'user': '',
                        'action': 'COMPONENTE',
                        'docs': comp_docs,
                        'image_data': _generate_dm_image(comp_dm),
                    }
                    if nested_components:
                        entry['components'] = nested_components
                    comp_rows.append(entry)
            # Append the assembly row to the list
            assemblies.append({
                'timestamp': timestamp,
                'datamatrix': dm_code,
                'name': assembly_name,
                'description': description,
                'user': user_display,
                'action': 'COSTRUZIONE',
                'docs': asm_docs,
                'image_data': _generate_dm_image(dm_code),
                'components': comp_rows,
                'box_id': None,
            })
    # Sort the assemblies by timestamp (descending) so that most recent builds appear first
    assemblies.sort(key=lambda x: x['timestamp'] or 0, reverse=True)
    return render_template('inventory/product_archive_assemblies.html', assemblies=assemblies, product=product, active_tab='archive_assiemi')

    '''


@inventory_bp.route('/build_product/<int:product_id>', methods=['GET', 'POST'])
@login_required
def build_product(product_id: int) -> Any:
    """
    Guided procedure for manually building a finished product.

    This handler mirrors the assembly build workflow to provide a consistent
    experience when constructing finished products.  It lists all
    immediate child products (assemblies, parts or commercial items)
    required to assemble the selected product, shows their current stock
    levels, surfaces any existing documentation on the product and
    requires the operator to upload compiled versions of those documents.
    A traffic light indicator reflects readiness: it turns green only
    when all child components are in stock, each has been associated via
    DataMatrix scanning and all required document uploads have a file
    selected.  On POST the handler validates stock levels, required
    uploads and associations before decrementing child product stock,
    incrementing the finished product stock, resetting production
    history and recording a new ProductBuild with its consumed items.
    Documentation from both the static folders and the transient archive
    is consolidated into the ``Produzione/Assiemi_completati`` hierarchy.
    When invoked from a production box (via the ``box_id`` and
    ``embedded`` query parameters) the handler returns a minimal success
    page instead of performing a normal redirect so that the modal in
    the parent view can close itself and refresh automatically.

    :param product_id: primary key of the product to assemble.
    :return: a redirect, a success page or a rendered template depending
             on the request method and context.
    """
    # Determine whether this page is rendered in embedded mode (inside a modal)
    # and whether a production box context is provided.  When ``embedded`` is
    # true the template hides the application header/navigation and the POST
    # branch returns a minimal success page so that the parent can close
    # the modal and refresh itself.  ``box_id_param`` identifies the
    # production box initiating the build.
    embedded_flag: bool = bool(request.args.get('embedded'))
    box_id_param: str | None = request.args.get('box_id')

    # Lookup the product and its BOM definition.  Each BOMLine lists
    # a child product and the quantity required to build one unit of
    # the parent product.  Only first‑level children are considered.
    product = Product.query.get_or_404(product_id)
    # Retrieve BOM lines for this product.  Avoid type annotations that refer
    # to BOMLine directly inside the function scope to prevent potential
    # UnboundLocalError issues when Python evaluates annotations.  The
    # ``BOMLine`` model is imported at module level and will be resolved
    # properly at runtime.
    bom_lines = BOMLine.query.filter_by(padre_id=product.id).all()

    # Mapping of document folder keys to human readable labels.  These
    # labels mirror those used in the assembly build.  This mapping is
    # reused both for display in the template and for error messages.
    #
    # NOTE:
    # Product‑level documents can be uploaded into categories such as
    # ``manuale`` or ``disegni`` via the product detail page.  The
    # previous mapping omitted these keys, meaning that documents
    # uploaded under those categories were silently ignored when
    # gathering documentation for the product build.  To ensure all
    # user‑supplied documents are surfaced during the build process,
    # include the same categories used on the product upload form.
    doc_label_map: dict[str, str] = {
        # Generic and quality documents
        'qualita': 'Modulo Cert. qualità',
        '3_1_materiale': '3.1 Materiale',
        'step_tavole': 'Step/tavola',
        'funzionamento': 'Verifica funzionamento',
        'istruzioni': 'Montaggio istruzioni',
        'ddt_fornitore': 'DDT fornitore',
        # Product‑specific document categories
        'manuale': 'Manuale',
        'disegni': 'Disegni',
        'altro': 'Altro'
    }

    # Determine a safe folder name for this product.  Use a sanitized
    # version of the product name or fall back to id_<id> when the
    # sanitized name is empty.  This safe name is used to locate
    # documentation under ``static/documents`` and to name archive
    # directories when consolidating files after a build.
    asm_safe: str = secure_filename(product.name) or f"id_{product.id}"

    # Helper to collect documentation entries for the product or a child
    # component.  When ``child_safe`` is provided, documentation is
    # gathered under ``static/documents/<asm_safe>/<child_safe>`` in
    # addition to any matching entries in the ``Produzione/archivio``
    # directory.  Otherwise only product-level documents are scanned.
    def _collect_docs(child_safe: str | None) -> dict[str, list[dict[str, Any]]]:
        """
        Collect documentation for the current product and optionally for
        a child component.  When ``child_safe`` is provided, search
        under ``static/documents/<product>/<child>/<folder>`` and the
        production archive using the supplied child name.  Otherwise
        search product‑level directories.  To improve robustness on
        case‑sensitive filesystems, this helper attempts multiple
        directory name variants derived from the product name (and
        child name when applicable), including the sanitised form,
        the raw value and a lower‑case variant.  All matches are
        merged into a single document map, with duplicates removed
        based on the relative path.

        :param child_safe: Sanitised name of a child component or
            ``None`` for top‑level product documentation.
        :return: A mapping of folder keys to a list of document
            descriptors.  Each descriptor contains a display_name,
            URL and relative path.
        """
        doc_map: dict[str, list[dict[str, Any]]] = {k: [] for k in doc_label_map.keys()}
        # Track seen relative paths to avoid duplicates when merging
        seen_rel_paths: set[str] = set()
        # Determine candidate directory names for the product
        candidates: list[str] = []
        # Sanitised name (may lower-case and replace spaces) used in existing code
        if asm_safe:
            candidates.append(asm_safe)
        # Raw product name stripped of whitespace
        raw_name = (product.name or '').strip()
        if raw_name and raw_name not in candidates:
            candidates.append(raw_name)
        # Lower-case variant of raw name
        lower_raw = raw_name.lower() if raw_name else ''
        if lower_raw and lower_raw not in candidates:
            candidates.append(lower_raw)
        # Build list of candidate base directories depending on whether a child is specified
        base_dirs: list[str] = []
        for cname in candidates:
            if child_safe:
                cand_dir = os.path.join(current_app.static_folder, 'documents', cname, child_safe)
            else:
                cand_dir = os.path.join(current_app.static_folder, 'documents', cname)
            if os.path.isdir(cand_dir):
                base_dirs.append(cand_dir)
        # Scan each base directory and merge results
        for base_dir in base_dirs:
            for folder_key in doc_label_map.keys():
                folder_dir = os.path.join(base_dir, folder_key)
                if not os.path.isdir(folder_dir):
                    continue
                try:
                    for fname in os.listdir(folder_dir):
                        fpath = os.path.join(folder_dir, fname)
                        if not os.path.isfile(fpath):
                            continue
                        try:
                            rel_path = os.path.relpath(fpath, current_app.static_folder)
                        except Exception:
                            rel_path = None
                        if not rel_path or rel_path in seen_rel_paths:
                            continue
                        seen_rel_paths.add(rel_path)
                        url = None
                        try:
                            url = url_for('static', filename=rel_path)
                        except Exception:
                            url = None
                        doc_map[folder_key].append({'display_name': fname, 'url': url, 'rel_path': rel_path})
                except Exception:
                    continue
        # Scan the production archive for compiled documents from previous builds
        try:
            produzione_root = os.path.join(current_app.root_path, 'Produzione')
            archivio_root = os.path.join(produzione_root, 'archivio')
            # Determine prefixes to search for in the archive.  When a child is
            # specified, use only the child name; otherwise consider all
            # candidate product names.  Prefixes are suffixed by '_' in the
            # directory names under the archive.
            if child_safe:
                prefixes = [child_safe]
            else:
                prefixes = candidates.copy()
            if os.path.isdir(archivio_root):
                for dir_name in os.listdir(archivio_root):
                    # Skip directories that do not start with any recognised prefix
                    match_prefix = None
                    for pfx in prefixes:
                        if dir_name.startswith(f"{pfx}_"):
                            match_prefix = pfx
                            break
                    if not match_prefix:
                        continue
                    arch_dir = os.path.join(archivio_root, dir_name)
                    if not os.path.isdir(arch_dir):
                        continue
                    for folder_key in doc_label_map.keys():
                        dt_path = os.path.join(arch_dir, folder_key)
                        if not os.path.isdir(dt_path):
                            continue
                        try:
                            for fname in os.listdir(dt_path):
                                fpath = os.path.join(dt_path, fname)
                                if not os.path.isfile(fpath):
                                    continue
                                rel_prod_path = None
                                try:
                                    rel_prod_path = os.path.relpath(fpath, produzione_root)
                                except Exception:
                                    rel_prod_path = None
                                if not rel_prod_path or rel_prod_path in seen_rel_paths:
                                    continue
                                seen_rel_paths.add(rel_prod_path)
                                url = None
                                try:
                                    url = url_for('inventory.download_production_file', filepath=rel_prod_path)
                                except Exception:
                                    url = None
                                doc_map[folder_key].append({'display_name': fname, 'url': url, 'rel_path': rel_prod_path})
                        except Exception:
                            continue
        except Exception:
            pass
        return doc_map

    # Build a list of children to consume when constructing the finished product.
    # Include both BOM child products and root-level structures defined via
    # ProductComponent.  When a BOM child product's root structure matches
    # a ProductComponent structure, the structure is skipped to avoid
    # double-deducting stock.  This ensures that all first-level
    # components (assemblies, parts or commercial items) are considered.
    children: list[dict[str, Any]] = []
    # Track mapping of BOM child product id to its root structure id.  Used to
    # filter product components that correspond to these BOM children.
    bom_child_root_map: dict[int, int | None] = {}
    # Process BOM lines first
    for bl in bom_lines:
        # Resolve the child product referenced by this BOM line
        child_prod = None
        try:
            child_prod = Product.query.get(bl.figlio_id)
        except Exception:
            child_prod = None
        if not child_prod:
            continue
        # Quantity required for the child; default to 1 on error
        qty_required = 1
        try:
            qty_required = int(bl.quantita or 1)
        except Exception:
            qty_required = 1
        children.append({'child': child_prod, 'qty_required': qty_required})
        # Determine the root structure for this child product so that we can
        # skip matching ProductComponent entries later.  Use the first
        # top-level ProductComponent associated with the child product.
        root_struct_id_child = None
        try:
            pc_candidates = ProductComponent.query.filter_by(product_id=child_prod.id).order_by(ProductComponent.id.asc()).all()
        except Exception:
            pc_candidates = []
        for pc in pc_candidates:
            try:
                struct_tmp = Structure.query.get(pc.structure_id)
            except Exception:
                struct_tmp = None
            if struct_tmp and (getattr(struct_tmp, 'parent_id', None) is None):
                root_struct_id_child = struct_tmp.id
                break
        bom_child_root_map[child_prod.id] = root_struct_id_child
    # Process product component entries to include root-level structures not
    # already represented by BOM child products.  Only include true
    # root‑level structures: a structure is considered root‑level when its
    # ``parent_id`` is ``None``.  Skip nested structures (where
    # ``parent_id`` is defined) and avoid structures that correspond to BOM
    # child products.  Accumulate quantities across multiple product
    # components referencing the same structure.
    try:
        comps_pc = ProductComponent.query.filter_by(product_id=product.id).all()
    except Exception:
        comps_pc = []
    # Build a set of structure ids corresponding to BOM child products' root
    # structures.  These structures should not be consumed directly because
    # they are represented via their BOM child products.
    bom_root_struct_ids: set[int] = set()
    for val in bom_child_root_map.values():
        if val is not None:
            bom_root_struct_ids.add(val)
    # Prepare a map to detect nested structures within this product.  A
    # structure is considered nested if its parent structure also belongs to
    # this product's component associations.  This allows inclusion of
    # structures whose parent belongs to another product (i.e., the parent
    # id is not in ``comp_map_for_skip``) while skipping those nested
    # within this product's own assembly.
    comp_map_for_skip: dict[int, ProductComponent] = {comp.structure_id: comp for comp in comps_pc}
    # Accumulate quantities for each eligible structure.  Include a
    # structure when it is a top-level component in this product (its
    # parent_id is either None or not associated with this product) and
    # exclude structures that correspond to BOM child products.  Sum
    # quantities across duplicate ProductComponent records.
    pc_map: dict[int, int] = {}
    for pc in comps_pc:
        struct = None
        try:
            struct = Structure.query.get(pc.structure_id)
        except Exception:
            struct = None
        if not struct:
            continue
        # Skip nested structures whose parent is also part of this product's
        # component associations.  Only top-level components (parent_id is
        # None or not in comp_map_for_skip) should be consumed when
        # building a product.
        try:
            parent_id_val = getattr(struct, 'parent_id', None)
        except Exception:
            parent_id_val = None
        if parent_id_val is not None and parent_id_val in comp_map_for_skip:
            continue
        # Skip structures that correspond to BOM child products.  When a
        # BOM child product's root structure matches this structure, the
        # product-level consumption will occur via the BOM entry instead.
        if struct.id in bom_root_struct_ids:
            continue
        # Accumulate the quantity required for this structure
        qty_req_pc = 1
        try:
            qty_req_pc = int(pc.quantity or 1)
        except Exception:
            qty_req_pc = 1
        pc_map[struct.id] = pc_map.get(struct.id, 0) + qty_req_pc
    # Append these structures as children entries
    for struct_id, qty_req_total in pc_map.items():
        struct = None
        try:
            struct = Structure.query.get(struct_id)
        except Exception:
            struct = None
        if not struct:
            continue
        # Attach display image when available
        try:
            img_fname = _lookup_structure_image(struct)
        except Exception:
            img_fname = None
        if img_fname:
            try:
                setattr(struct, 'display_image_filename', img_fname)
            except Exception:
                pass
        children.append({'child': struct, 'qty_required': qty_req_total})

    # Determine the assembly code representing this product build.  This
    # synthetic DataMatrix payload is used as the parent_code when
    # associating components via the API and when deriving the nested
    # structure in the production history.  Use the product name as
    # the component (P=) segment and ASSIEME as the type so that
    # existing helpers treat the product as a top‑level assembly.  If
    # the product name sanitises to an empty string, fall back to
    # id_<product_id>.
    # Use a PRODOTTO type for the synthetic assembly code to match
    # DataMatrix codes generated for production boxes requesting finished
    # products.  This parent code is used when associating components and
    # deriving the nested structure in the production history.  Falling
    # back to ASSIEME here would still work due to alternative lookup
    # logic, but using PRODOTTO improves clarity.
    assembly_code: str = f"P={product.name}|T=PRODOTTO"

    # Compute required quantities and associated counts for each child.
    required_quantities: dict[int, int] = {}
    associated_counts: dict[int, int] = {}
    for c in children:
        required_quantities[c['child'].id] = c['qty_required']
        associated_counts[c['child'].id] = 0

    # Count how many of each child product have been associated to this
    # assembly_code via StockItem.parent_code.  Iterate over stock items
    # whose parent_code matches the assembly_code or alternate forms and
    # extract the P= segment of their DataMatrix to match against the
    # child product name.  When no items are found with the full code,
    # attempt matching simplified variants (without DMV prefix, G=, or
    # serial segments).  This replicates the logic used in
    # ``_build_component_rows`` for assembly builds.
    try:
        from ...models import StockItem as _StockItem
        # Helper to gather stock items whose parent_code equals a candidate
        def _gather_items_for_parent(code: str) -> list[Any]:
            try:
                return _StockItem.query.filter_by(parent_code=code).all()
            except Exception:
                return []
        # Primary lookup using the full assembly_code
        assoc_items: list[Any] = _gather_items_for_parent(assembly_code)
        # Fallback lookup using simplified variants when no results
        if not assoc_items:
            segments = (assembly_code or '').split('|')
            if segments:
                seg_no_dm = segments[1:] if segments and segments[0].upper().startswith('DM') else segments[:]
                seg_no_g = [s for s in seg_no_dm if not (s.upper().startswith('G='))]
                candidates: list[str] = []
                cand1 = '|'.join(seg_no_dm)
                if cand1 and cand1 != assembly_code:
                    candidates.append(cand1)
                cand2 = '|'.join(seg_no_g)
                if cand2 and cand2 != assembly_code and cand2 not in candidates:
                    candidates.append(cand2)
                comp_seg = None
                typ_seg = None
                for s in seg_no_g:
                    if s.upper().startswith('P='):
                        comp_seg = s
                    elif s.upper().startswith('T='):
                        typ_seg = s
                if comp_seg and typ_seg:
                    cand3 = '|'.join([comp_seg, typ_seg])
                    if cand3 and cand3 != assembly_code and cand3 not in candidates:
                        candidates.append(cand3)
                for cand in candidates:
                    items_candidate = _gather_items_for_parent(cand)
                    if items_candidate:
                        assoc_items = items_candidate
                        break
        # Count associations per child id by matching P= segment of the
        # stock item's DataMatrix payload to the child product name.
        for si in assoc_items or []:
            try:
                dmcode = si.datamatrix_code or ''
            except Exception:
                dmcode = ''
            comp_name = None
            try:
                for seg in dmcode.split('|'):
                    if seg.upper().startswith('P='):
                        comp_name = seg.split('=', 1)[1]
                        break
            except Exception:
                comp_name = None
            if comp_name:
                for c in children:
                    if c['child'].name == comp_name:
                        associated_counts[c['child'].id] = associated_counts.get(c['child'].id, 0) + 1
    except Exception:
        # On error leave associated_counts at zero
        pass

    # Determine readiness flags.  A part is ready when there is at least
    # one unit in stock; the overall product is ready only when all
    # required units of every child are available.  Documentation is
    # considered ready when no uploads are required.  Association
    # readiness requires that for each child product the number of
    # associated components is at least the required quantity.
    ready_parts: bool = bool(children) and all((c['child'].quantity_in_stock or 0) >= c['qty_required'] for c in children)
    # Documentation map: owner id -> folder key -> list of entries
    # Only product-level documents are considered required for uploads.
    # Child-level documentation may be displayed in other contexts but is not
    # enforced during product build.  Build the map in both GET and POST
    # branches to compute required uploads.
    if request.method == 'GET':
        doc_map: dict[int, dict[str, list[dict[str, Any]]]] = {}
        # Collect documentation for the product itself.  Pass None for child_safe
        # so that _collect_docs scans ``static/documents/<product>`` and the
        # production archive for top-level files.  The resulting list is
        # presented to the operator.  Uploads for these documents are
        # optional; therefore the list of upload fields is built here
        # purely to display input elements in the template.  The build
        # workflow does not enforce that an upload is provided for each
        # document.  ``docs_ready`` is set to True unconditionally to
        # reflect that documentation does not block the build.
        # Gather documentation for the product. For each document category (folder key)
        # require exactly one upload per category rather than one per document.  This
        # prevents duplicate upload prompts when multiple source documents exist in
        # the same category.
        doc_map[product.id] = _collect_docs(None)
        required_uploads: list[tuple[int, str, int]] = []
        for folder_key, docs in doc_map[product.id].items():
            # Only require an upload when at least one document exists in the category.
            if docs:
                required_uploads.append((product.id, folder_key, 0))
        # Documentation readiness is true only when there are no categories requiring
        # replacement uploads.  Otherwise the traffic light remains red until all
        # required uploads are provided.
        docs_ready: bool = (len(required_uploads) == 0)
        # Association readiness: ensure every child has at least the
        # required number of associated items.  When no children exist
        # set assoc_ready to False to prevent building an empty product.
        assoc_ready: bool = True if children else False
        for c in children:
            rid = c['child'].id
            req = required_quantities.get(rid, 1)
            cnt = associated_counts.get(rid, 0)
            if cnt < req:
                assoc_ready = False
                break
        # Compute how many units can be built given current stock.  This
        # metric is displayed to the operator but does not affect the
        # enablement of the build button.  When no children exist
        # number_buildable defaults to zero.
        if children:
            buildable_counts: list[int] = []
            for c in children:
                try:
                    avail = int(c['child'].quantity_in_stock or 0)
                    req_qty = int(c['qty_required'] or 1)
                    buildable_counts.append(avail // req_qty)
                except Exception:
                    buildable_counts.append(0)
            number_buildable = min(buildable_counts) if buildable_counts else 0
        else:
            number_buildable = 0
        # Determine where to return after completion/cancellation.  When
        # coming from a production box the referrer is usually the
        # production_box view; otherwise fall back to the product detail
        # page.  Capture this here so that it can be passed through to
        # the template and persisted in the hidden form field on POST.
        back_url = request.referrer or url_for('inventory.product_detail', product_id=product.id)
        return render_template(
            'inventory/build_product.html',
            product=product,
            children=children,
            doc_map=doc_map,
            required_uploads=required_uploads,
            ready_parts=ready_parts,
            docs_ready=docs_ready,
            assoc_ready=assoc_ready,
            ready_all=(ready_parts and docs_ready and assoc_ready),
            number_buildable=number_buildable,
            back_url=back_url,
            doc_label_map=doc_label_map,
            required_quantities=required_quantities,
            associated_counts=associated_counts,
            assembly_code=assembly_code,
            active_tab='products',
            embedded=embedded_flag,
            box_id=box_id_param
        )
    # -------------------------------------------------------------------
    # POST: validate all inputs and perform the build.  Recompute
    # documentation requirements and association readiness to handle
    # concurrent modifications (e.g. stock movements or additional
    # scans).  Abort when any condition fails and redirect back with
    # an appropriate message.
    # ---------------------------------------------------------------------
    # POST: recompute documentation.  Collect product-level documentation
    # similarly to the GET branch.  For each document category, require a
    # single compiled upload rather than one per document.  Missing
    # uploads will block the build to ensure that compiled versions are
    # provided for every existing document category.
    doc_map: dict[int, dict[str, list[dict[str, Any]]]] = {}
    doc_map[product.id] = _collect_docs(None)
    required_uploads: list[tuple[int, str, int]] = []
    for folder_key, docs in doc_map[product.id].items():
        if docs:
            required_uploads.append((product.id, folder_key, 0))
    # Validate stock availability again
    insufficient: list[str] = []
    for c in children:
        child = c['child']
        qty_required = int(c['qty_required'] or 1)
        available = int(child.quantity_in_stock or 0)
        if available < qty_required:
            try:
                cname = child.name
            except Exception:
                cname = 'N/D'
            insufficient.append(f"{cname} (richiesti {qty_required}, disponibili {available})")
    if insufficient:
        flash('Componenti insufficienti per costruire questo prodotto.', 'warning')
        return redirect(request.referrer or url_for('inventory.product_detail', product_id=product.id))
    # Collect uploaded files for optional documentation.  Iterate over
    # all expected upload fields based on ``required_uploads`` and
    # capture those files which have a non-empty filename.  Missing
    # files are silently ignored.
    uploads: dict[tuple[int, str, int], Any] = {}
    for (owner_id, folder_key, idx) in required_uploads:
        field_name = f"upload_{owner_id}_{folder_key}_{idx}"
        file = request.files.get(field_name)
        if file and getattr(file, 'filename', ''):
            uploads[(owner_id, folder_key, idx)] = file
    # Validate that a compiled upload has been provided for each document category.
    # When a required category is missing its upload, abort the build and
    # redirect back to the build form with a warning message.  Use the
    # doc_label_map to derive user-friendly names for the missing categories.
    missing_docs: list[str] = []
    for (owner_id, folder_key, idx) in required_uploads:
        # Each required_upload tuple has idx==0 because only one upload per category is required
        if (owner_id, folder_key, idx) not in uploads:
            # Use doc_label_map if available to translate folder_key into a readable name
            try:
                display_name = doc_label_map.get(folder_key, folder_key)
            except Exception:
                display_name = folder_key
            missing_docs.append(display_name)
    if missing_docs:
        # Some required document uploads are missing.  Compose a message and re-render
        # the build page with the appropriate context.  Replicate the GET logic for
        # computing readiness and buildable quantities so that the UI reflects
        # the current state.  Use a multi-line flash message to highlight
        # missing categories.
        msgs = ["Carica tutti i documenti richiesti prima di procedere:"]
        msgs.extend(missing_docs)
        flash('\n'.join(msgs), 'danger')
        # Documentation not ready due to missing uploads
        docs_ready = False
        # Recompute association readiness based on current association counts (pre-scan)
        assoc_ready_temp = True if children else False
        for c in children:
            rid = c['child'].id
            req_qty = required_quantities.get(rid, 1)
            cnt = associated_counts.get(rid, 0)
            if cnt < req_qty:
                assoc_ready_temp = False
                break
        # Compute number of units buildable from current stock (same as GET logic)
        if children:
            buildable_counts: list[int] = []
            for c in children:
                try:
                    avail = int(c['child'].quantity_in_stock or 0)
                    req_qty = int(c['qty_required'] or 1)
                    buildable_counts.append(avail // req_qty)
                except Exception:
                    buildable_counts.append(0)
            number_buildable = min(buildable_counts) if buildable_counts else 0
        else:
            number_buildable = 0
        # Determine back URL.  Prefer form parameter, then referrer, then product detail page.
        back_url_val = request.form.get('back_url') or (request.referrer or url_for('inventory.product_detail', product_id=product.id))
        # Return the build page with updated readiness flags.  The build button will
        # remain disabled until all file inputs have a file selected.
        return render_template(
            'inventory/build_product.html',
            product=product,
            children=children,
            doc_map=doc_map,
            required_uploads=required_uploads,
            ready_parts=ready_parts,
            docs_ready=docs_ready,
            assoc_ready=assoc_ready_temp,
            ready_all=False,
            number_buildable=number_buildable,
            back_url=back_url_val,
            doc_label_map=doc_label_map,
            required_quantities=required_quantities,
            associated_counts=associated_counts,
            assembly_code=assembly_code,
            active_tab='products',
            embedded=embedded_flag,
            box_id=box_id_param
        )
    # Recompute association readiness.  If any child product has fewer
    # associated components than required, abort.  This ensures that
    # operators complete the scanning workflow before building.  Use
    # the same logic as in the GET branch but update counts from the
    # latest database state.
    assoc_ready: bool = True if children else False
    # Reset counts
    for c in children:
        associated_counts[c['child'].id] = 0
    try:
        from ...models import StockItem as _StockItem
        def _gather_items_for_parent(code: str) -> list[Any]:
            try:
                return _StockItem.query.filter_by(parent_code=code).all()
            except Exception:
                return []
        assoc_items: list[Any] = _gather_items_for_parent(assembly_code)
        if not assoc_items:
            segments = (assembly_code or '').split('|')
            if segments:
                seg_no_dm = segments[1:] if segments and segments[0].upper().startswith('DM') else segments[:]
                seg_no_g = [s for s in seg_no_dm if not (s.upper().startswith('G='))]
                candidates: list[str] = []
                cand1 = '|'.join(seg_no_dm)
                if cand1 and cand1 != assembly_code:
                    candidates.append(cand1)
                cand2 = '|'.join(seg_no_g)
                if cand2 and cand2 != assembly_code and cand2 not in candidates:
                    candidates.append(cand2)
                comp_seg = None
                typ_seg = None
                for s in seg_no_g:
                    if s.upper().startswith('P='):
                        comp_seg = s
                    elif s.upper().startswith('T='):
                        typ_seg = s
                if comp_seg and typ_seg:
                    cand3 = '|'.join([comp_seg, typ_seg])
                    if cand3 and cand3 != assembly_code and cand3 not in candidates:
                        candidates.append(cand3)
                for cand in candidates:
                    items_candidate = _gather_items_for_parent(cand)
                    if items_candidate:
                        assoc_items = items_candidate
                        break
        for si in assoc_items or []:
            dmcode = ''
            try:
                dmcode = si.datamatrix_code or ''
            except Exception:
                dmcode = ''
            comp_name = None
            try:
                for seg in dmcode.split('|'):
                    if seg.upper().startswith('P='):
                        comp_name = seg.split('=', 1)[1]
                        break
            except Exception:
                comp_name = None
            if comp_name:
                for c in children:
                    if c['child'].name == comp_name:
                        associated_counts[c['child'].id] = associated_counts.get(c['child'].id, 0) + 1
    except Exception:
        pass
    # Check counts
    for c in children:
        rid = c['child'].id
        req = required_quantities.get(rid, 1)
        cnt = associated_counts.get(rid, 0)
        if cnt < req:
            assoc_ready = False
            break
    if not assoc_ready:
        flash('Associa tutti i componenti richiesti prima di procedere.', 'warning')
        return redirect(request.referrer or url_for('inventory.product_detail', product_id=product.id))
    # At this point validations have passed.  Perform the build in a
    # transaction.  Any exception triggers a rollback.
    try:
        from ...models import ProductComponent as PC, Structure as STR, ProductBuild, ProductBuildItem
        # Helper to adjust the stock of a product and its structure.
        def _adjust_structure_stock(prod: Product, delta: float) -> None:
            root_comp = (
                PC.query
                .filter_by(product_id=prod.id)
                .order_by(PC.id.asc())
                .first()
            )
            if not root_comp:
                return
            struct = STR.query.get(root_comp.structure_id)
            if not struct:
                return
            matches_dict: dict[int, STR] = {}
            try:
                for m in STR.query.filter(STR.name == struct.name).all():
                    matches_dict[m.id] = m
            except Exception:
                pass
            if struct.component_id:
                try:
                    for m in STR.query.filter(STR.component_id == struct.component_id).all():
                        matches_dict[m.id] = m
                except Exception:
                    pass
            matches = list(matches_dict.values())
            quantities: list[float] = []
            for m in matches:
                try:
                    quantities.append(float(m.quantity_in_stock or 0))
                except Exception:
                    pass
            if not quantities:
                try:
                    quantities.append(float(struct.quantity_in_stock or 0))
                except Exception:
                    quantities.append(0)
            current_qty: float = max(quantities) if quantities else 0.0
            new_qty: float = current_qty + delta
            if new_qty < 0:
                new_qty = 0
            struct.quantity_in_stock = new_qty
            for m in matches:
                m.quantity_in_stock = new_qty
        # Persist uploaded files into the production archive.  Group
        # uploads by owner and folder under a timestamped directory.  Use
        # owner.safe_name_<timestamp>/<folder>/<filename>.
        produzione_root = os.path.join(current_app.root_path, 'Produzione')
        archivio_root = os.path.join(produzione_root, 'archivio')
        os.makedirs(archivio_root, exist_ok=True)
        timestamp = int(time.time())
        for (owner_id, folder_key, idx), upload in uploads.items():
            # Determine the owning product (either parent product or child)
            owner = product if owner_id == product.id else Product.query.get(owner_id)
            if not owner:
                continue
            owner_safe = secure_filename(owner.name) or f"id_{owner.id}"
            folder_name = f"{owner_safe}_{timestamp}"
            dest_dir = os.path.join(archivio_root, folder_name, folder_key)
            os.makedirs(dest_dir, exist_ok=True)
            filename = secure_filename(upload.filename)
            dest_path = os.path.join(dest_dir, filename)
            try:
                upload.save(dest_path)
            except Exception:
                pass
        # Reset existing product builds and their items
        try:
            existing_builds = ProductBuild.query.filter_by(product_id=product.id).all()
            for pb in existing_builds:
                ProductBuildItem.query.filter_by(build_id=pb.id).delete()
            ProductBuild.query.filter_by(product_id=product.id).delete()
            db.session.flush()
        except Exception:
            pass
        # ---------------------------------------------------------------------
        # NOTE: Do **not** delete build records for child assemblies when
        # building a finished product.
        #
        # Previous versions of the application removed ProductBuild records
        # for assemblies once they were consumed in a final product build.
        # This caused the assembly build history to disappear from the global
        # "storico produzione" view.  Keeping the assembly build records
        # allows the archive to show a complete hierarchy of the product
        # including all sub‑assemblies and their components.  See issue
        # described by user: importare le righe da archivio componenti e
        # archivio assiemi nello storico magazzino per i prodotti finiti.
        #
        # Therefore, the deletion of ProductBuild and ProductBuildItem rows
        # for child products has been removed.  The remaining logic simply
        # resets existing ProductBuild entries for the current product but
        # leaves sub‑assembly build records intact.
        # Deduct stock from each child and adjust their structures.  When
        # the child is a Structure rather than a Product, update the
        # quantity on the structure itself and propagate the new value
        # across all structures sharing the same name or component_id.
        # Only invoke the original _adjust_structure_stock helper for
        # Product instances (child products) because it expects a
        # Product and uses ProductComponent to locate the root
        # structure.  Passing a Structure into that helper would
        # effectively no-op and fail to propagate stock changes.
        from ...models import Structure as _Struct  # type: ignore
        for c in children:
            child = c['child']
            # Default to a single unit when parsing quantity fails
            qty_required = 1
            try:
                qty_required = int(c['qty_required'] or 1)
            except Exception:
                qty_required = 1
            if not child:
                continue
            # Determine current on-hand quantity
            try:
                current_qty = float(getattr(child, 'quantity_in_stock', 0) or 0)
            except Exception:
                current_qty = 0.0
            new_qty = current_qty - float(qty_required)
            if new_qty < 0:
                new_qty = 0.0
            # Update the child's own stock
            try:
                child.quantity_in_stock = new_qty
            except Exception:
                pass
            # If this child is a Structure, propagate the change to
            # duplicate structures that represent the same component.
            # Duplicate structures are identified either by matching
            # names or by sharing the same component_id.  Updating all
            # matches ensures that the warehouse reflects a consistent
            # stock level across different products that reuse this
            # structure.  When the child is a Product, adjust its
            # associated structure stock via the helper.
            try:
                if isinstance(child, _Struct):
                    # Collect structures matching by name or component_id
                    matches: list[_Struct] = []
                    try:
                        # Filter by exact name match
                        matches = list(_Struct.query.filter(_Struct.name == child.name).all())
                    except Exception:
                        matches = []
                    # Extend matches with structures sharing the same component_id
                    try:
                        comp_id = getattr(child, 'component_id', None)
                        if comp_id is not None:
                            addl = _Struct.query.filter(_Struct.component_id == comp_id).all()
                            for s in addl:
                                if s not in matches:
                                    matches.append(s)
                    except Exception:
                        pass
                    for s in matches:
                        try:
                            s.quantity_in_stock = new_qty
                        except Exception:
                            pass
                else:
                    # For Product children, adjust the corresponding structure stock
                    _adjust_structure_stock(child, -qty_required)
            except Exception:
                # If any propagation fails, proceed without blocking the build
                pass
        # Increment the finished product stock and adjust its structure
        product.quantity_in_stock = (product.quantity_in_stock or 0) + 1
        _adjust_structure_stock(product, 1)
        # Create new ProductBuild record and its items.  When the product
        # build originates from a production box (via ?box_id= query
        # parameter), attach that box id to the ProductBuild so that
        # downstream archive views can link the build back to the box.  The
        # assignment occurs before committing so that the foreign key is
        # persisted in the same transaction as the build items.
        pb = ProductBuild(product_id=product.id, qty=1, user_id=getattr(current_user, 'id', None))
        # Attach production box id when supplied in the request.  Only
        # integer values are accepted; invalid input results in a null
        # assignment.
        box_id_param = request.args.get('box_id')
        if box_id_param:
            try:
                pb.production_box_id = int(box_id_param)
            except Exception:
                pb.production_box_id = None
        db.session.add(pb)
        db.session.flush()
        # Determine the root structure id for this product to avoid adding
        # a ProductBuildItem referencing the product itself or its top‑level
        # structure.  Products are composed of structures via ProductComponent;
        # the first top‑level structure (with no parent) is considered the
        # root.  When this structure id coincides with the product id in the
        # database, comparing child.id to product.id erroneously skips
        # legitimate children.  Instead compute the root structure id and
        # compare against it.
        root_struct_id: int | None = None
        try:
            from ...models import ProductComponent as _PC, Structure as _ST  # type: ignore
            # Retrieve the first top-level ProductComponent for this product
            pcs = _PC.query.filter_by(product_id=product.id).order_by(_PC.id.asc()).all()
            for pc in pcs:
                try:
                    struct = _ST.query.get(pc.structure_id)
                except Exception:
                    struct = None
                if struct and not getattr(struct, 'parent_id', None):
                    root_struct_id = struct.id
                    break
        except Exception:
            root_struct_id = None
        for c in children:
            # Avoid recording a ProductBuildItem for the product itself or its root
            # structure.  When fallback logic derives children from top‑level
            # structures, the structure id may coincide with the product id.  In
            # addition, skip structures entirely because ProductBuildItem
            # references the Product table via product_id.  Associating a
            # structure id would violate the foreign key constraint.  Only
            # record items for Product instances.
            child_obj = None
            child_id_val = None
            try:
                child_obj = c.get('child')
                child_id_val = getattr(child_obj, 'id', None)
            except Exception:
                child_obj = None
                child_id_val = None
            # Skip when the child matches the finished product id or the root structure id
            if child_id_val is not None and (child_id_val == product.id or (root_struct_id is not None and child_id_val == root_struct_id)):
                continue
            # Skip when the child is not a Product (e.g. Structure)
            # Determine if child_obj is a Product by checking for a 'name' and not having a 'flag_assembly'
            is_structure_child = False
            try:
                # Structures have flag_assembly attribute; Products do not
                if hasattr(child_obj, 'flag_assembly') or hasattr(child_obj, 'flag_part') or hasattr(child_obj, 'flag_commercial'):
                    is_structure_child = True
            except Exception:
                is_structure_child = False
            if is_structure_child:
                continue
            # Add ProductBuildItem for valid Product children
            try:
                db.session.add(ProductBuildItem(build_id=pb.id, product_id=child_obj.id, quantity_required=c['qty_required']))
            except Exception:
                # Continue processing other children even on failure
                continue
        # Commit the build and its items.  The production box id on the
        # ProductBuild will be persisted here as well.
        db.session.commit()

        # -----------------------------------------------------------------
        # Determine the DataMatrix code used to represent the finished
        # product for this build and ensure that a corresponding stock
        # item exists in the production box.  When a finished product is
        # constructed from a reservation, there may be no existing
        # StockItem for the final product; alternatively its DataMatrix
        # may be missing the ``T=PRODOTTO`` segment.  A unique
        # DataMatrix code is needed so that all child components can be
        # linked via ``parent_code`` and so that the production history
        # correctly identifies the build.  The logic below derives the
        # appropriate code as follows:
        #   1. When a stock item for the product exists in the box, use
        #      its existing DataMatrix code if it already contains
        #      ``T=PRODOTTO``; otherwise overwrite the code with a
        #      synthetic payload ``P=<product.name>|T=PRODOTTO``.
        #   2. When no stock item exists, create one with the synthetic
        #      ``P=<product.name>|T=PRODOTTO`` payload.  The status is
        #      set to ``COMPLETATO`` since the build will complete
        #      immediately after association.
        #   3. Use the resulting DataMatrix code (original or synthetic)
        #      as ``dm_code`` when linking all child stock items via
        #      their ``parent_code`` field.
        dm_code: str = f"P={product.name}|T=PRODOTTO"
        try:
            from ...models import StockItem as _SI  # type: ignore
            box_id_raw = request.args.get('box_id')
            box_id_associated: int | None = None
            if box_id_raw:
                try:
                    box_id_associated = int(box_id_raw)
                except Exception:
                    box_id_associated = None
            if box_id_associated:
                # Attempt to locate an existing stock item for this product in the box
                try:
                    si_existing = (
                        _SI.query
                        .filter_by(product_id=product.id, production_box_id=box_id_associated)
                        .first()
                    )
                except Exception:
                    si_existing = None
                if si_existing:
                    current_dm = getattr(si_existing, 'datamatrix_code', '') or ''
                    # Ensure the DataMatrix contains the PRODOTTO type; update when necessary
                    if 'T=PRODOTTO' not in current_dm.upper():
                        try:
                            si_existing.datamatrix_code = dm_code
                            db.session.add(si_existing)
                            db.session.commit()
                        except Exception:
                            db.session.rollback()
                    else:
                        dm_code = current_dm
                else:
                    # No stock item exists; create one with the synthetic code
                    try:
                        new_si = _SI(
                            product_id=product.id,
                            datamatrix_code=dm_code,
                            parent_code=None,
                            status='COMPLETATO',
                            production_box_id=box_id_associated
                        )
                        db.session.add(new_si)
                        db.session.commit()
                    except Exception:
                        db.session.rollback()
        except Exception:
            # Even if the stock item cannot be created, proceed with the synthetic code
            pass

        # -----------------------------------------------------------------
        # Associate all loaded components in the originating production box
        # with the finished product build.  When building a product from a
        # production box, operators traditionally scan each component
        # individually to generate an ``ASSOCIA`` event and set the
        # ``parent_code`` on each stock item.  Those scans link the
        # component stock items to the assembly DataMatrix code so that
        # nested component trees are correctly rendered in the global
        # history (storico magazzino) view.  However, when constructing
        # a finished product directly from a reservation the operator
        # expects all child components (parts, commercial parts and
        # sub‑assemblies) to appear under a single row without performing
        # manual scans.  To achieve this automatically, assign the
        # ``parent_code`` of every stock item in the originating
        # production box to the DataMatrix code of the finished product
        # build and record a corresponding ``ASSOCIA`` scan event.  This
        # mirrors the manual association workflow and ensures that all
        # components from the component and assembly archives are
        # imported into the storage history.  Only stock items whose
        # product_id differs from the finished product are updated so
        # that the final product's own stock item (if present) remains
        # unchanged.  Wrap in a try/except to avoid blocking the build
        # when scanning or association fails.
        try:
            from ...models import StockItem as _StockItem, ScanEvent as _ScanEvent  # type: ignore
            import json as _json
            # Determine the production box id passed in the request.  A valid
            # integer indicates that the build originated from a production box;
            # otherwise this step is skipped.
            box_id_raw = request.args.get('box_id')
            box_id_assoc: int | None = None
            if box_id_raw:
                try:
                    box_id_assoc = int(box_id_raw)
                except Exception:
                    box_id_assoc = None
            # Compute a DataMatrix code for the finished product build.  When a
            # stock item representing the finished product already exists in
            # the production box and its DataMatrix contains ``T=PRODOTTO``,
            # reuse that exact code.  Otherwise fallback to a synthetic
            # ``P=<product.name>|T=PRODOTTO`` payload.  This ensures that
            # associations use the same code as the build history and avoids
            # mismatches when a scanned code includes prefixes (e.g. DM
            # variants) not present in the synthetic code.
            dm_code: str = f"P={product.name}|T=PRODOTTO"
            items: list[Any] | None = None
            if box_id_assoc:
                try:
                    items = _StockItem.query.filter_by(production_box_id=box_id_assoc).all()
                except Exception:
                    items = []
                # Prefer a stock item for this product with T=PRODOTTO
                if items:
                    for si in items:
                        try:
                            if getattr(si, 'product_id', None) == product.id:
                                dm_tmp = getattr(si, 'datamatrix_code', '') or ''
                                if dm_tmp and 'T=PRODOTTO' in dm_tmp.upper():
                                    dm_code = dm_tmp
                                    break
                        except Exception:
                            continue
                    # If not found, use any item in the box with T=PRODOTTO
                    if dm_code == f"P={product.name}|T=PRODOTTO":
                        for si in items:
                            try:
                                dm_tmp = getattr(si, 'datamatrix_code', '') or ''
                                if dm_tmp and 'T=PRODOTTO' in dm_tmp.upper():
                                    dm_code = dm_tmp
                                    break
                            except Exception:
                                continue
            # Assign parent_code and record association events for each
            # component stock item in the production box.  Skip the finished
            # product stock item itself to avoid a self‑link.  Only update
            # items whose parent_code differs from ``dm_code`` to prevent
            # duplicate events.
            if box_id_assoc:
                # Ensure we have the list of items
                if items is None:
                    try:
                        items = _StockItem.query.filter_by(production_box_id=box_id_assoc).all()
                    except Exception:
                        items = []
                for si in items or []:
                    try:
                        # Skip association for the finished product itself
                        if getattr(si, 'product_id', None) == product.id:
                            continue
                        # Only update items that do not already have the
                        # desired parent_code to prevent duplicate events
                        current_parent = getattr(si, 'parent_code', None)
                        if current_parent == dm_code:
                            continue
                        # Update the parent_code to link this component stock
                        # item to the finished product's DataMatrix
                        si.parent_code = dm_code
                        # Record a ScanEvent with action ASSOCIA.  The meta
                        # field stores the id of the user performing the
                        # association.  Use the current user's id when
                        # available.  The ScanEvent timestamp will be
                        # generated automatically via TimestampMixin.
                        meta_dict: dict[str, Any] = {}
                        # Include the operator performing the association when authenticated
                        try:
                            uid = getattr(current_user, 'id', None)
                            if uid:
                                meta_dict['user_id'] = int(uid)
                                # Include the operator's username in the meta to preserve the original user display.
                                try:
                                    meta_dict['user_username'] = current_user.username
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        # Persist static component information (name, description, revision) for historical accuracy
                        try:
                            dm_meta = getattr(si, 'datamatrix_code', '') or ''
                            comp_code_meta: str | None = None
                            for seg in (dm_meta or '').split('|'):
                                seg_str = seg.strip()
                                if seg_str.upper().startswith('P='):
                                    comp_code_meta = seg_str.split('=', 1)[1]
                                    break
                        except Exception:
                            comp_code_meta = None
                        struct_meta = None
                        if comp_code_meta:
                            try:
                                struct_meta = Structure.query.filter_by(name=comp_code_meta).first()
                            except Exception:
                                struct_meta = None
                        # Fallback to stock item's product root when component lookup fails
                        if not struct_meta:
                            try:
                                prod_tmp = getattr(si, 'product', None)
                                if prod_tmp:
                                    root_comp_tmp = (
                                        ProductComponent.query
                                        .filter_by(product_id=prod_tmp.id)
                                        .order_by(ProductComponent.id.asc())
                                        .first()
                                    )
                                    if root_comp_tmp:
                                        struct_meta = Structure.query.get(root_comp_tmp.structure_id)
                            except Exception:
                                struct_meta = None
                        if struct_meta:
                            try:
                                meta_dict['structure_name'] = getattr(struct_meta, 'name', '') or ''
                            except Exception:
                                meta_dict['structure_name'] = ''
                            try:
                                meta_dict['structure_description'] = getattr(struct_meta, 'description', '') or ''
                            except Exception:
                                meta_dict['structure_description'] = ''
                            # Intentionally omit ``revision_label`` and ``revision_index``
                            # when recording ASSOCIA events.  The revision is captured
                            # at load time and should not be updated during
                            # associations.  Leaving these fields out of the
                            # meta prevents later revisions from overriding
                            # the original value in the archive.
                        # Serialise the meta to JSON when any fields exist
                        meta_json_val = None
                        try:
                            meta_json_val = _json.dumps(meta_dict) if meta_dict else None
                        except Exception:
                            meta_json_val = None
                        se = _ScanEvent(
                            datamatrix_code=getattr(si, 'datamatrix_code', ''),
                            action='ASSOCIA',
                            meta=meta_json_val
                        )
                        db.session.add(se)
                    except Exception:
                        # Continue processing other items even if one fails
                        continue
                # Persist the updates to parent_code and the new scan events
                db.session.commit()
        except Exception:
            # Silently ignore association errors; the primary build has
            # already been committed at this point.
            pass
        # Consolidate documentation into ``Produzione/Assiemi_completati``.  Use
        # the product safe name and a fresh timestamp to avoid collisions.
        try:
            # -----------------------------------------------------------------
            # Define a helper to copy documentation for nested BOM components.
            #
            # When building a finished product from a production box the
            # temporary archive (``archivio_root``) contains one folder per
            # associated component.  Each folder is named ``<safe_name>_<ts>``
            # and contains subdirectories for each document type (e.g.
            # ``qualita``, ``manuale``, ``altro``).  To mirror the hierarchical
            # structure of the BOM in the final archive we recursively copy
            # these folders into nested ``componenti`` directories under the
            # assembly folder.  The helper below traverses the BOM and, for
            # each child component, creates a destination folder using a new
            # timestamp and moves any matching archive folders into it.  It
            # then recurses on the child to handle deeper levels.  Any
            # errors encountered during copying are silently ignored to
            # avoid blocking the build.
            # `secure_filename` is imported at module scope; avoid re-importing
            # it here to prevent masking the global name and triggering
            # UnboundLocalError.  Import only Product as an alias.
            # BOMLine is also imported at module scope and will be resolved
            # correctly inside this helper.
            from ...models import Product as _ProdModel  # type: ignore

            def _copy_bom_docs(parent_prod, dest_path: str) -> None:
                """
                Recursively copy documentation for the BOM children of
                ``parent_prod`` into nested ``componenti`` folders under
                ``dest_path``.  Uses ``archivio_root`` as the source for
                temporary upload folders.  New folders are named
                ``<safe_name>_<timestamp>`` and created under a
                ``componenti`` subdirectory of ``dest_path``.  After copying
                a source folder it is removed from ``archivio_root``.

                Args:
                    parent_prod: The product whose BOM children should be
                        processed.  Must have an ``id`` and ``name``.
                    dest_path: The directory corresponding to this product
                        where nested ``componenti`` folders should be created.
                """
                try:
                    # Fetch BOM lines where parent is this product
                    child_lines = BOMLine.query.filter_by(padre_id=parent_prod.id).all()
                except Exception:
                    child_lines = []
                for line in (child_lines or []):
                    child_prod = None
                    try:
                        child_prod = _ProdModel.query.get(line.figlio_id)
                    except Exception:
                        child_prod = None
                    if not child_prod:
                        continue
                    # Generate a safe folder name and timestamp for the child
                    try:
                        child_safe = secure_filename(child_prod.name) or f"id_{child_prod.id}"
                    except Exception:
                        child_safe = f"id_{child_prod.id}"
                    try:
                        comp_ts = int(time.time())
                    except Exception:
                        comp_ts = 0
                    comp_folder = f"{child_safe}_{comp_ts}"
                    # Determine destination directory for this child
                    nested_dir = os.path.join(dest_path, 'componenti', comp_folder)
                    try:
                        os.makedirs(nested_dir, exist_ok=True)
                    except Exception:
                        pass
                    # Move/copy any matching folders from the temporary archive
                    try:
                        if os.path.isdir(archivio_root):
                            for dn in os.listdir(archivio_root):
                                # Match by safe prefix
                                if not dn.startswith(f"{child_safe}_"):
                                    continue
                                src_dir = os.path.join(archivio_root, dn)
                                if not os.path.isdir(src_dir):
                                    continue
                                # Copy document type subdirectories
                                try:
                                    for dt_name in os.listdir(src_dir):
                                        dt_path = os.path.join(src_dir, dt_name)
                                        if not os.path.isdir(dt_path):
                                            continue
                                        dest_type_dir = os.path.join(nested_dir, dt_name)
                                        try:
                                            os.makedirs(dest_type_dir, exist_ok=True)
                                        except Exception:
                                            pass
                                        for fname in os.listdir(dt_path):
                                            src_file = os.path.join(dt_path, fname)
                                            dest_file = os.path.join(dest_type_dir, fname)
                                            try:
                                                if not os.path.exists(dest_file):
                                                    shutil.copyfile(src_file, dest_file)
                                            except Exception:
                                                pass
                                except Exception:
                                    pass
                                # Remove the source folder after copying
                                try:
                                    shutil.rmtree(src_dir)
                                except Exception:
                                    pass
                    except Exception:
                        pass
                    # Recurse into the child's BOM to handle its children
                    try:
                        _copy_bom_docs(child_prod, nested_dir)
                    except Exception:
                        pass
            completati_root = os.path.join(produzione_root, 'Assiemi_completati')
            os.makedirs(completati_root, exist_ok=True)
            asm_timestamp = int(time.time())
            asm_folder_name = f"{asm_safe}_{asm_timestamp}"
            asm_dir = os.path.join(completati_root, asm_folder_name)
            os.makedirs(asm_dir, exist_ok=True)
            # Copy product-level docs from static
            try:
                prod_doc_base = os.path.join(current_app.static_folder, 'documents', asm_safe)
                if os.path.isdir(prod_doc_base):
                    for doc_type in os.listdir(prod_doc_base):
                        src_dir = os.path.join(prod_doc_base, doc_type)
                        if not os.path.isdir(src_dir):
                            continue
                        dest_dir = os.path.join(asm_dir, doc_type)
                        os.makedirs(dest_dir, exist_ok=True)
                        for fname in os.listdir(src_dir):
                            src_file = os.path.join(src_dir, fname)
                            dest_file = os.path.join(dest_dir, fname)
                            try:
                                shutil.copyfile(src_file, dest_file)
                            except Exception:
                                pass
            except Exception:
                pass
            # Copy product-level docs from the temporary archive
            try:
                if os.path.isdir(archivio_root):
                    for dir_name in os.listdir(archivio_root):
                        if not dir_name.startswith(f"{asm_safe}_"):
                            continue
                        arch_dir = os.path.join(archivio_root, dir_name)
                        if not os.path.isdir(arch_dir):
                            continue
                        for dt_name in os.listdir(arch_dir):
                            dt_path = os.path.join(arch_dir, dt_name)
                            if not os.path.isdir(dt_path):
                                continue
                            dest_dir = os.path.join(asm_dir, dt_name)
                            os.makedirs(dest_dir, exist_ok=True)
                            for fname in os.listdir(dt_path):
                                src_file = os.path.join(dt_path, fname)
                                dest_file = os.path.join(dest_dir, fname)
                                try:
                                    if not os.path.exists(dest_file):
                                        shutil.copyfile(src_file, dest_file)
                                except Exception:
                                    pass
                        try:
                            shutil.rmtree(arch_dir)
                        except Exception:
                            pass
            except Exception:
                pass
            # Copy documentation for each child product into component subfolders
            comps_dir = os.path.join(asm_dir, 'componenti')
            os.makedirs(comps_dir, exist_ok=True)
            for c in children:
                child = c['child']
                if not child:
                    continue
                child_safe = secure_filename(child.name) or f"id_{child.id}"
                comp_timestamp = int(time.time())
                comp_folder_name = f"{child_safe}_{comp_timestamp}"
                comp_dest_dir = os.path.join(comps_dir, comp_folder_name)
                os.makedirs(comp_dest_dir, exist_ok=True)
                # Copy from static/documents/<asm_safe>/<child_safe>
                try:
                    comp_src_base = os.path.join(current_app.static_folder, 'documents', asm_safe, child_safe)
                    if os.path.isdir(comp_src_base):
                        for doc_type in os.listdir(comp_src_base):
                            src_dir = os.path.join(comp_src_base, doc_type)
                            if not os.path.isdir(src_dir):
                                continue
                            dest_type_dir = os.path.join(comp_dest_dir, doc_type)
                            os.makedirs(dest_type_dir, exist_ok=True)
                            for fname in os.listdir(src_dir):
                                src_file = os.path.join(src_dir, fname)
                                dest_file = os.path.join(dest_type_dir, fname)
                                try:
                                    shutil.copyfile(src_file, dest_file)
                                except Exception:
                                    pass
                except Exception:
                    pass
                # Copy from temporary archive directories matching child_safe
                try:
                    if os.path.isdir(archivio_root):
                        for dir_name in os.listdir(archivio_root):
                            if not dir_name.startswith(f"{child_safe}_"):
                                continue
                            arch_comp_dir = os.path.join(archivio_root, dir_name)
                            if not os.path.isdir(arch_comp_dir):
                                continue
                            for dt_name in os.listdir(arch_comp_dir):
                                dt_path = os.path.join(arch_comp_dir, dt_name)
                                if not os.path.isdir(dt_path):
                                    continue
                                dest_type_dir = os.path.join(comp_dest_dir, dt_name)
                                os.makedirs(dest_type_dir, exist_ok=True)
                                for fname in os.listdir(dt_path):
                                    src_file = os.path.join(dt_path, fname)
                                    dest_file = os.path.join(dest_type_dir, fname)
                                    try:
                                        if not os.path.exists(dest_file):
                                            shutil.copyfile(src_file, dest_file)
                                    except Exception:
                                        pass
                            try:
                                shutil.rmtree(arch_comp_dir)
                            except Exception:
                                pass
                except Exception:
                    pass

                # -----------------------------------------------------------------
                # After copying documentation for the immediate child, propagate
                # documentation for its BOM descendants.  The helper
                # ``_copy_bom_docs`` (defined above) will recursively walk
                # through the BOM hierarchy and move any matching folders
                # from ``archivio_root`` into nested ``componenti``
                # directories under ``comp_dest_dir``.  Use a try/except
                # wrapper to avoid interrupting the build flow on error.
                try:
                    _copy_bom_docs(child, comp_dest_dir)
                except Exception:
                    pass
        except Exception:
            # Suppress errors when copying documentation so that the
            # build can complete.
            pass
        # ---------------------------------------------------------------
        # If this product build was initiated from a production box (specified
        # via the ``box_id`` query parameter), mark the box and its items as
        # completed.  This mirrors the behaviour of assembly builds and
        # ensures that returning to the production dashboard reflects the
        # updated state.  Any errors encountered while updating the box are
        # silently ignored to avoid blocking the main build flow.
        try:
            box_id_param = request.args.get('box_id')
            if box_id_param:
                try:
                    box_id_int = int(box_id_param)
                except (TypeError, ValueError):
                    box_id_int = None
                if box_id_int:
                    from ...models import ProductionBox
                    box_obj = ProductionBox.query.get(box_id_int)
                    if box_obj and box_obj.status != 'COMPLETATO':
                        box_obj.status = 'COMPLETATO'
                        try:
                            for si in box_obj.stock_items:
                                si.status = 'COMPLETATO'
                        except Exception:
                            pass
                        db.session.commit()
        except Exception:
            # Do not block the build if updating the production box fails
            pass
        flash('Prodotto assemblato con successo.', 'success')
        # Determine where to redirect after a successful build.  Use the
        # back_url provided in the form, then fall back to the referrer
        # header and finally the product detail page.  When called from
        # within a production box modal (embedded_flag true and box_id
        # provided), return a minimal success page instructing the parent
        # window to close the modal and refresh.  This mirrors the
        # behaviour of assembly builds.
        try:
            back_url = request.form.get('back_url') or request.referrer or url_for('inventory.product_detail', product_id=product.id)
        except Exception:
            back_url = url_for('inventory.product_detail', product_id=product.id)
        # If initiated from a production box modal, return a success page
        # instead of performing a redirect.  The template will trigger
        # closure of the modal and refresh the parent page.
        if box_id_param and embedded_flag:
            return render_template(
                'inventory/build_product_success.html',
                back_url=back_url
            )
    except Exception:
        db.session.rollback()
        flash('Errore durante la costruzione del prodotto.', 'danger')
        # In case of error, always redirect back to the originating page
        # rather than returning a success page.
        return redirect(request.form.get('back_url') or request.referrer or url_for('inventory.product_detail', product_id=product.id))
    # Default: when not in embedded mode, redirect to the back_url
    return redirect(back_url)


# -----------------------------------------------------------------------------
# Production file download endpoint
#
# This route allows users to download files stored under the ``Produzione``
# hierarchy.  Documents saved during component loads and assembly builds are
# persisted outside of the Flask static folder under ``Produzione`` (for
# example in ``Produzione/archivio`` or ``Produzione/Assiemi_completati``).  The
# component and assembly archive views construct download URLs using this
# endpoint.  When a user clicks a document link, the application serves the
# corresponding file as an attachment.  To protect against directory
# traversal, the resolved absolute path is verified to reside within the
# ``Produzione`` directory and to reference an existing file.  If the
# validation fails a 404 is returned.
@inventory_bp.route('/download_production_file/<path:filepath>')
@login_required
def download_production_file(filepath: str):
    """Serve a file from the Produzione directory as an attachment.

    :param filepath: Path relative to the ``Produzione`` folder.  May include
        nested subdirectories (e.g. ``archivio/componenti/manuale.pdf``).
    :return: A ``send_file`` response containing the file or a 404 if the
        requested path is invalid or the file does not exist.
    """
    try:
        # Determine the absolute path to the Produzione root.  Joining
        # ``current_app.root_path`` with 'Produzione' ensures that we always
        # reference the application's production data directory regardless of
        # the working directory.  Normalise both the root and requested paths
        # to guard against symbolic links and redundant separators.
        produzione_root = os.path.join(current_app.root_path, 'Produzione')
        abs_root = os.path.abspath(produzione_root)
        # Combine the relative filepath with the root and normalise
        file_path = os.path.abspath(os.path.join(produzione_root, filepath))
        # Reject any request that attempts to escape the production folder or
        # references a non-existent file.  ``startswith`` ensures that the
        # computed absolute path begins with the production root, thus
        # preventing directory traversal.  The second check confirms the
        # existence of the file on disk and ensures we only serve regular
        # files.
        if not file_path.startswith(abs_root) or not os.path.isfile(file_path):
            abort(404)
        # Serve the file as an attachment using the basename as the download
        # name.  Flask's ``send_file`` will raise an error if it fails to
        # read the specified file, which is converted into a 404 by our
        # exception handling below.
        return send_file(file_path, as_attachment=True, download_name=os.path.basename(file_path))
    except Exception:
        # On any error (invalid path, file not found, permission issues)
        # respond with a 404 to avoid exposing internal details.
        abort(404)


@inventory_bp.route('/document_by_name/<path:filename>')
@login_required
def download_document_by_name(filename: str):
    """
    Serve a document based solely on its filename by searching the
    ``static/documents`` hierarchy.

    This endpoint acts as a fallback for files whose stored URL cannot
    be resolved (for example when directory separators have been
    flattened into underscores).  Given only a base filename it walks
    through the ``static/documents`` directory tree and returns the
    first matching file as an attachment.  If no match is found a
    404 error is returned.

    :param filename: The exact name of the file to search for.  Any
        directory segments are treated as part of the filename when
        underscores are used instead of separators.
    :return: A send_file response for the first matching document.
    """
    try:
        # Search for the file in a few safe subdirectories within the
        # ``static`` folder.  We look under ``documents`` (the primary
        # repository for compiled documents), ``tmp_components`` (used for
        # transient compiled files) and ``uploads`` (user‑uploaded files).
        search_roots = [
            os.path.join(current_app.static_folder, 'documents'),
            os.path.join(current_app.static_folder, 'tmp_components'),
            os.path.join(current_app.static_folder, 'uploads'),
        ]
        for root in search_roots:
            if not os.path.isdir(root):
                continue
            for dirpath, dirnames, files in os.walk(root):
                for f in files:
                    if f == filename:
                        file_path = os.path.join(dirpath, f)
                        if os.path.isfile(file_path):
                            return send_file(file_path, as_attachment=True, download_name=f)
        # No matching file found
        abort(404)
    except Exception:
        abort(404)


@inventory_bp.route('/production_box/<int:box_id>')
@login_required
def production_box_view(box_id: int):
    """Render a page showing details of a production box.

    The box view lists each contained stock item with its DataMatrix
    code and status.  When the box status is not ``COMPLETATO`` a
    "Carica" button is displayed that allows operators to finalise
    loading.  This page delegates the actual status update to the
    corresponding API endpoint via client‑side JavaScript.
    """
    box = ProductionBox.query.get_or_404(box_id)
    # Determine the assembly id for this box by parsing the component code
    # from the first stock item's DataMatrix.  The DataMatrix format is
    # DMV1|P=<component_code>|S=<serial>|T=<type>[|G=...].  Extract P=<...>.
    assembly_id = None
    root_structure = None
    root_image_filename = None
    try:
        # Import models locally to avoid circular imports
        from ...models import Structure, Product, ProductComponent as _PC
        # Initialise variables
        root_structure = None
        assembly_id = None
        root_image_filename = None
        box_product = None
        component_name = None
        # Extract the component name from the first stock item's DataMatrix code
        if box.stock_items:
            try:
                dm = box.stock_items[0].datamatrix_code or ''
            except Exception:
                dm = ''
            for seg in (dm.split('|') if dm else []):
                if seg.startswith('P='):
                    component_name = seg.split('=', 1)[1]
                    break
        # If a component name was found, determine whether this box represents
        # an assembly or a finished product.  When the P= segment matches the
        # name of the product associated with the first stock item, interpret
        # the box as a finished product build, regardless of the declared
        # box_type.  This addresses reservations created via the assembly
        # workflow with componentCode equal to the product name.  Otherwise
        # prefer a matching structure (for assemblies) and fall back to a
        # product lookup when no structure exists.
        if component_name:
            # Determine if the component name matches the product on the first stock item
            first_product = None
            try:
                first_product = box.stock_items[0].product if box.stock_items else None
            except Exception:
                first_product = None
            match_finished = False
            if first_product:
                try:
                    fp_name = (first_product.name or '').strip().lower()
                except Exception:
                    fp_name = None
                try:
                    comp_norm = (component_name or '').strip().lower()
                except Exception:
                    comp_norm = None
                if fp_name and comp_norm and fp_name == comp_norm:
                    match_finished = True
            if match_finished:
                # Treat as finished product: assign box_product and skip structure lookup
                box_product = first_product
            else:
                if box.box_type == 'PRODOTTO':
                    # For explicit PRODOTTO boxes, resolve directly to a product
                    try:
                        prod_candidate = Product.query.filter_by(name=component_name).first()
                    except Exception:
                        prod_candidate = None
                    if prod_candidate:
                        box_product = prod_candidate
                else:
                    # Attempt to match a structure first for assembly-type boxes
                    try:
                        root_candidate = Structure.query.filter_by(name=component_name).first()
                    except Exception:
                        root_candidate = None
                    if root_candidate:
                        root_structure = root_candidate
                        try:
                            assembly_id = root_structure.id
                        except Exception:
                            assembly_id = None
                        try:
                            root_image_filename = _lookup_structure_image(root_structure)
                        except Exception:
                            root_image_filename = None
                    else:
                        # When no structure matches, attempt to resolve as a product
                        try:
                            prod_candidate = Product.query.filter_by(name=component_name).first()
                        except Exception:
                            prod_candidate = None
                        if prod_candidate:
                            box_product = prod_candidate
        # When neither a root structure nor a product is determined, fall back
        # to deriving the root structure from the first component of the stock item's product.
        if not root_structure and not box_product and box.stock_items:
            try:
                prod = box.stock_items[0].product
            except Exception:
                prod = None
            if prod:
                try:
                    pc = _PC.query.filter_by(product_id=prod.id).order_by(_PC.id.asc()).first()
                except Exception:
                    pc = None
                if pc:
                    try:
                        root_candidate2 = Structure.query.get(pc.structure_id)
                    except Exception:
                        root_candidate2 = None
                    if root_candidate2:
                        root_structure = root_candidate2
                        try:
                            assembly_id = root_structure.id
                        except Exception:
                            assembly_id = None
                        try:
                            root_image_filename = _lookup_structure_image(root_structure)
                        except Exception:
                            root_image_filename = None
        # If a product was identified, attach attributes to the box
        if box_product:
            setattr(box, 'product', box_product)
            setattr(box, 'product_id', box_product.id)
            setattr(box, 'product_name', box_product.name)
    except Exception:
        # Silently ignore any errors during root structure and product determination
        pass

    # Attach additional attributes to the box object for the template
    setattr(box, 'assembly_id', assembly_id)
    setattr(box, 'root_structure', root_structure)
    setattr(box, 'root_image_filename', root_image_filename)
    # Pass the root structure name directly to the template for convenience.
    # Determine any document checklist entries for this box's root structure.
    # When the warehouse operator opens the box for a part or commercial
    # component we want to display the list of required documents (carica/scarica)
    # selected in the anagrafiche via the checklist flags.  Load the
    # checklist from disk and extract entries for the root structure ID.
    try:
        required_docs: list[dict[str, str]] = []
        if root_structure:
            from ...checklist import load_checklist
            data = load_checklist()
            sid = str(root_structure.id)
            if sid in data:
                for rel_path in data[sid]:
                    # Split the path to show only the filename
                    name = os.path.basename(rel_path)
                    required_docs.append({'path': rel_path, 'name': name})
    except Exception:
        required_docs = []
    # Attach the required documents list to the box object
    setattr(box, 'required_docs', required_docs)

    # ---------------------------------------------------------------------
    # Determine whether batch (lot) management is enabled for the root
    # component displayed in this production box.  When enabled, all
    # stock items in the box share the same DataMatrix code and the
    # loading interface presents a single "Carica" action for the
    # entire box.  When disabled each stock item is loaded
    # individually.  Examine the notes fields on the structure and its
    # component master for a JSON key ``lot_management`` set to true.
    # Determine whether batch (lot) management is enabled for the root
    # component displayed in this production box.  When enabled, all
    # stock items in the box share the same DataMatrix code and the
    # loading interface presents a single "Carica" action for the
    # entire box.  Examine the notes fields on the structure and its
    # component master for a JSON key ``lot_management`` set to true.  If
    # parsing fails (e.g. notes contain plain text), fall back to a
    # substring search for the key to support legacy data.
    lot_mgmt_flag = False
    try:
        import json as _json
        root_struct = getattr(box, 'root_structure', None)
        if root_struct:
            candidates: list[str] = []
            # Collect notes from the component master and the structure itself
            try:
                cm = root_struct.component_master
            except Exception:
                cm = None
            if cm and getattr(cm, 'notes', None):
                candidates.append(cm.notes)
            if getattr(root_struct, 'notes', None):
                candidates.append(root_struct.notes)
            # Attempt strict JSON parsing first
            for candidate in candidates:
                if not candidate:
                    continue
                try:
                    parsed = _json.loads(candidate)
                    if isinstance(parsed, dict) and 'lot_management' in parsed:
                        lot_mgmt_flag = bool(parsed.get('lot_management'))
                        if lot_mgmt_flag:
                            break
                except Exception:
                    # ignore errors; fall back to substring search later
                    pass
            # If no JSON flag found, fallback to substring search
            if not lot_mgmt_flag:
                for candidate in candidates:
                    if not candidate or not isinstance(candidate, str):
                        continue
                    if 'lot_management' in candidate.lower():
                        lot_mgmt_flag = True
                        break
    except Exception:
        lot_mgmt_flag = False
    setattr(box, 'lot_management', lot_mgmt_flag)

    return render_template('inventory/production_box.html', box=box)