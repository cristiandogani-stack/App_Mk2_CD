"""
Helpers to manage per-structure document checklist flags.

This module centralises reading and writing of the ``checklist.json``
file stored under the application's instance folder.  The checklist
records, for each structure (assembly, part or commercial component),
which document files should be considered part of the build and load
procedures.  When a document is flagged on the anagrafiche pages via
the "Check list" checkboxes, its relative static path is stored in
the checklist under the key equal to the structure's identifier.

The JSON file has the following shape::

    {
        "1": ["documents/Pump/qualita/quality.pdf", "tmp_components/P001/qualita/default.pdf"],
        "2": ["documents/Valve/3_1_materiale/certificate.pdf"]
    }

Each key is a stringified structure ID; each value is a list of
relative paths (relative to ``static``) identifying the documents
selected for that structure.  When loading or building components
these flags are used to determine which documents need to be shown
and which uploads are required.  If a structure ID is absent or its
list is empty, no documents are required for that component.

These helpers handle file creation, JSON parsing and persistence.
If the checklist file does not exist it is created automatically
with an empty dictionary.
"""

from __future__ import annotations

import json
import os
from typing import Dict, List, Set

from flask import current_app


def _get_checklist_path() -> str:
    """Return the absolute path to the checklist JSON file.

    The file is stored in the Flask instance folder.  If the folder
    does not exist it will be created lazily when writing the file.
    """
    inst_path = current_app.instance_path  # type: ignore[attr-defined]
    # Ensure the instance directory exists.  Flask will normally
    # create this when the application starts, but guard against
    # environments where it might be missing (e.g. during tests).
    os.makedirs(inst_path, exist_ok=True)
    return os.path.join(inst_path, "checklist.json")


def load_checklist() -> Dict[str, List[str]]:
    """Load the document checklist from disk.

    Returns a mapping of structure ID (as string) to a list of
    relative document paths.  If the file cannot be read or parsed
    the function returns an empty dictionary.
    """
    path = _get_checklist_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Ensure the value is always a dict
            if isinstance(data, dict):
                # Normalise values to lists of strings
                for k, v in list(data.items()):
                    # Ensure each value is a list of strings; normalise path separators.
                    if not isinstance(v, list):
                        data[k] = []
                        continue
                    # Remove duplicates while preserving order.  Normalise path
                    # separators to forward slashes so that different OS formats
                    # (e.g. "\\" vs "/") map to the same entry.
                    seen: Set[str] = set()
                    dedup: List[str] = []
                    for item in v:
                        if not isinstance(item, str):
                            continue
                        # normalise backslashes to forward slashes and strip whitespace
                        norm_item = item.replace('\\', '/').strip()
                        if norm_item and norm_item not in seen:
                            seen.add(norm_item)
                            dedup.append(norm_item)
                    data[k] = dedup
                return data
    except FileNotFoundError:
        # If the file doesn't exist, return an empty mapping
        return {}
    except Exception:
        # On any other error return empty mapping
        return {}
    return {}


def save_checklist(data: Dict[str, List[str]]) -> None:
    """Persist the given checklist mapping to disk.

    The mapping is serialised as JSON.  Any intermediate directories
    required to write the file are created automatically.  Errors are
    silently ignored to avoid crashing the application.  For correct
    behaviour of the checklist flags ensure that the process has
    permission to write to the instance directory.
    """
    path = _get_checklist_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Normalise all stored paths before writing.  Replace
        # backslashes with forward slashes and strip whitespace so
        # that checklist entries remain consistent across operating
        # systems.  Without this step Windows-originating paths
        # containing "\\" would persist and fail to match
        # Linux-style paths when loading.
        norm_data: Dict[str, List[str]] = {}
        for k, v in data.items():
            if not isinstance(v, list):
                continue
            new_list: List[str] = []
            for item in v:
                if not isinstance(item, str):
                    continue
                try:
                    norm_item = item.replace('\\', '/').strip()
                except Exception:
                    norm_item = item
                if norm_item:
                    new_list.append(norm_item)
            norm_data[k] = new_list
        with open(path, "w", encoding="utf-8") as f:
            json.dump(norm_data, f, indent=2)
    except Exception:
        # Ignore I/O errors
        pass


def is_flagged(structure_id: int, doc_path: str) -> bool:
    """Return True if the specified document is flagged for the given structure.

    :param structure_id: Identifier of the Structure
    :param doc_path: Relative static path of the document
    """
    data = load_checklist()
    # Normalise the document path before lookup.  Replace backslashes
    # with forward slashes and strip whitespace so that paths stored
    # on different platforms match consistently.
    norm_path = ''
    try:
        norm_path = doc_path.replace('\\', '/').strip()
    except Exception:
        norm_path = doc_path
    return str(structure_id) in data and norm_path in data.get(str(structure_id), [])


def toggle_flag(structure_id: int, doc_path: str, flag: bool) -> None:
    """Set or clear a flag on a document for the given structure.

    When ``flag`` is True the document path is added to the list of
    flagged documents for the structure if not already present.  When
    ``flag`` is False the document is removed from the list.  Changes
    are persisted immediately to disk.

    :param structure_id: Identifier of the Structure
    :param doc_path: Relative static path of the document
    :param flag: Desired flag state
    """
    data = load_checklist()
    key = str(structure_id)
    # Ensure entry exists
    if key not in data:
        data[key] = []
    # Normalise the path separators and strip whitespace before storing.
    norm = ''
    try:
        norm = doc_path.replace('\\', '/').strip()
    except Exception:
        norm = doc_path
    if flag:
        if norm not in data[key]:
            data[key].append(norm)
    else:
        if norm in data[key]:
            data[key].remove(norm)
    # Save back to disk
    save_checklist(data)