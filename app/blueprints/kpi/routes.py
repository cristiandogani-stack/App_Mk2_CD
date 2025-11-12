from flask import Blueprint, render_template
from flask_login import login_required

kpi_bp = Blueprint('kpi', __name__, template_folder='../../templates')

@kpi_bp.route('/')
@login_required
def index():
    return render_template('stubs/generic.html', title='KPI', subtitle='Work in progress')
