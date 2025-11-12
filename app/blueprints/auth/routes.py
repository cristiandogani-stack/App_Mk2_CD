from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required, current_user
from ...extensions import db
from ...models import User

auth_bp = Blueprint('auth', __name__, template_folder='../../templates')

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.index'))

    if request.method == 'POST':
        username = request.form.get('username', '').strip().lower()
        password = request.form.get('password', '')
        remember = True if request.form.get('remember') == 'on' else False

        user = User.query.filter_by(username=username, active=True).first()
        if user and user.check_password(password):
            login_user(user, remember=remember)
            flash('Bentornato!', 'success')
            next_url = request.args.get('next') or url_for('dashboard.index')
            return redirect(next_url)
        else:
            flash('Credenziali non valide.', 'danger')

    return render_template('auth/login.html')

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Sei uscito dall\'applicazione.', 'info')
    return redirect(url_for('auth.login'))
