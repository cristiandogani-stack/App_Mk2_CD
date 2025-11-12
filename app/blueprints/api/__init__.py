"""API blueprint package.

This package defines a simple set of RESTful endpoints used by the
warehouse UI to perform operations related to reservations and
production boxes.  The endpoints return JSON responses and do not
render templates directly.  They are prefixed under ``/api`` when
registered with the Flask application.
"""

from flask import Blueprint

api_bp = Blueprint('api', __name__)

from . import routes  # noqa: F401