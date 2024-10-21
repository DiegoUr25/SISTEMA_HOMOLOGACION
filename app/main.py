# # from flask import Blueprint, render_template, request, redirect, url_for, flash
# # import os
# # import subprocess

# # main = Blueprint('main', __name__)

# # @main.route('/')
# # def index():
# #     return render_template('index.html')

# # @main.route('/upload', methods=['POST'])
# # def upload():
# #     if 'file' not in request.files:
# #         flash('No file part')
# #         return redirect(request.url)
# #     file = request.files['file']
# #     if file.filename == '':
# #         flash('No selected file')
# #         return redirect(request.url)
# #     if file:
# #         filename = file.filename
# #         file.save(os.path.join('input', filename))
# #         flash('File successfully uploaded')
# #         return redirect(url_for('main.index'))

# # @main.route('/confirm', methods=['POST'])
# # def confirm_etl():
# #     confirmation = request.form.get('confirm')
# #     if confirmation == 'Y':
# #         try:
# #             # Asegúrate de que la ruta al script es correcta y de que acepta argumentos de línea de comando
# #             subprocess.run(['python', os.path.join('ejecutador.py'), 'Y'], check=True)
# #             flash('ETL process executed successfully.')
# #         except subprocess.CalledProcessError as e:
# #             flash(f'An error occurred: {e}')
# #     else:
# #         flash('ETL execution canceled.')
# #     return redirect(url_for('main.index'))


# from flask import Blueprint, render_template, request, redirect, url_for, flash
# import os
# import subprocess
# import glob

# main = Blueprint('main', __name__)

# @main.route('/')
# def index():
#     return render_template('index.html')

# @main.route('/upload', methods=['POST'])
# def upload():
#     files = request.files.getlist('files')
#     if not files:
#         flash('No files selected')
#         return redirect(request.url)
    
#     # Limpia la carpeta 'input' de archivos existentes con las mismas extensiones
#     existing_files = glob.glob('input/*')
#     for existing_file in existing_files:
#         if existing_file.split('.')[-1] in [file.filename.split('.')[-1] for file in files]:
#             os.remove(existing_file)
#             flash(f'Removed existing file {os.path.basename(existing_file)}')

#     # Guarda los nuevos archivos
#     for file in files:
#         if file and file.filename:
#             filepath = os.path.join('input', file.filename)
#             file.save(filepath)
#             flash(f'File {file.filename} successfully uploaded')
    
#     return redirect(url_for('main.index'))

# @main.route('/confirm', methods=['POST'])
# def confirm_etl():
#     confirmation = request.form.get('confirm')
#     if confirmation == 'Y':
#         try:
#             # Asegúrate de que la ruta al script es correcta y de que acepta argumentos de línea de comando
#             subprocess.run(['python', os.path.join('ejecutador.py'), 'Y'], check=True)
#             flash('ETL process executed successfully.')
#         except subprocess.CalledProcessError as e:
#             flash(f'An error occurred: {e}')
#     else:
#         flash('ETL execution canceled.')
#     return redirect(url_for('main.index'))

# from flask import Blueprint, render_template, request, redirect, url_for, flash
# import os
# import subprocess

# main = Blueprint('main', __name__)

# @main.route('/')
# def index():
#     return render_template('index.html')

# @main.route('/upload', methods=['POST'])
# def upload():
#     if 'file' not in request.files:
#         flash('No file part')
#         return redirect(request.url)
#     file = request.files['file']
#     if file.filename == '':
#         flash('No selected file')
#         return redirect(request.url)
#     if file:
#         filename = file.filename
#         file.save(os.path.join('input', filename))
#         flash('File successfully uploaded')
#         return redirect(url_for('main.index'))

# @main.route('/confirm', methods=['POST'])
# def confirm_etl():
#     confirmation = request.form.get('confirm')
#     if confirmation == 'Y':
#         try:
#             # Asegúrate de que la ruta al script es correcta y de que acepta argumentos de línea de comando
#             subprocess.run(['python', os.path.join('ejecutador.py'), 'Y'], check=True)
#             flash('ETL process executed successfully.')
#         except subprocess.CalledProcessError as e:
#             flash(f'An error occurred: {e}')
#     else:
#         flash('ETL execution canceled.')
#     return redirect(url_for('main.index'))

import glob
from flask import Blueprint, render_template, request, redirect, send_from_directory, url_for, flash
import os
import subprocess
from ejecutadores.funciones import obtener_datos_distribuidoras

main = Blueprint('main', __name__)

@main.route('/')
def index():
    return render_template('index.html')

@main.route('/ETL')
def procesoETL():
    return render_template('ETL.html')

@main.route('/upload', methods=['POST'])
def upload():
    files = request.files.getlist('files')
    if not files:
        flash('No files selected')
        return redirect(request.url)
    
    # Limpia la carpeta 'input' de archivos existentes con las mismas extensiones
    existing_files = glob.glob('input/*')
    for existing_file in existing_files:
        if existing_file.split('.')[-1] in [file.filename.split('.')[-1] for file in files]:
            os.remove(existing_file)
            flash(f'Removed existing file {os.path.basename(existing_file)}')

    # Guarda los nuevos archivos
    for file in files:
        if file and file.filename:
            filepath = os.path.join('input', file.filename)
            file.save(filepath)
            flash(f'File {file.filename} successfully uploaded')
    
    return redirect(url_for('main.procesoETL'))

@main.route('/confirm', methods=['POST'])
def confirm_etl():
    confirmation = request.form.get('confirm')
    if confirmation == 'Y':
        try:
            # Asegúrate de que la ruta al script es correcta y de que acepta argumentos de línea de comando
            subprocess.run(['python', os.path.join('ejecutador.py'), 'Y'], check=True)
            flash('ETL process executed successfully.')
        except subprocess.CalledProcessError as e:
            flash(f'An error occurred: {e}')
    else:
        flash('ETL execution canceled.')
    return redirect(url_for('main.procesoETL'))


# @main.route('/download/<filename>')
# def download_file(filename):
#     directory = os.path.join(os.getcwd(), 'output')  # Asegúrate de que este es el directorio correcto
#     try:
#         return send_from_directory(directory, filename, as_attachment=True)
#     except FileNotFoundError:
#         flash("File not found.")
#         return redirect(url_for('index'))
    

# @main.route('/results')
# def show_results():
#     directory = os.path.join(os.getcwd(), 'output')
#     files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
#     return render_template('results.html', files_available=files)


@main.route('/distribuidoras')
def mostrar_distribuidoras():
    # Obtenemos los datos desde la base de datos
    distribuidoras = obtener_datos_distribuidoras()
    
    # Renderizamos el template y pasamos los datos a la tabla
    return render_template('distribuidoras.html', distribuidoras=distribuidoras)