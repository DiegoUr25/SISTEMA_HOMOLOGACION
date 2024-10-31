import glob
import time
from flask import Blueprint, Response, render_template, request, redirect,stream_with_context, url_for, flash, jsonify # type: ignore
import os
import subprocess
from clasificacion import clasificar_productos
from ejecutadores.funciones import obtener_datos_distribuidoras

main = Blueprint('main', __name__)

@main.route('/')
def index():
    return render_template('indexTest.html')

@main.route('/ETL')
def procesoETL():
    return render_template('ETLTest.html')

@main.route('/homologacion')
def procesoHomologacion():
    return render_template('Homologacion.html')

@main.route('/upload', methods=['POST'])
def upload():
    files = request.files.getlist('files')
    if not files:
        flash('No files selected')
        return redirect(request.url)
    
    existing_files = glob.glob('input/*')
    for existing_file in existing_files:
        if existing_file.split('.')[-1] in [file.filename.split('.')[-1] for file in files]:
            os.remove(existing_file)
            flash(f'Removed existing file {os.path.basename(existing_file)}')

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
            subprocess.run(['python', os.path.join('ejecutador.py'), 'Y'], check=True)
            flash('ETL process executed successfully.')
        except subprocess.CalledProcessError as e:
            flash(f'An error occurred: {e}')
    else:
        flash('ETL execution canceled.')
    return redirect(url_for('main.procesoETL'))



@main.route('/clasificar_productos', methods=['POST'])
def clasificar_productos_endpoint():
    try:
        result = clasificar_productos()  # Llama a la función de clasificación
        if "error" in result:
            flash(f"Error en la clasificación: {result['error']}")
            return jsonify(result), 500
        else:
            flash(f"Clasificación completada. Accuracy: {result['accuracy']:.2f}")
            return jsonify(result), 200
    except Exception as e:
        flash(f"An error occurred: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@main.route('/stream_logs')
def stream_logs():
    def generate():
        yield 'data: Iniciando clasificación...\n\n'
        # Simular logs de un proceso largo
        for i in range(1, 11):
            time.sleep(1)
            yield f'data: Progreso {i * 10}%\n\n'
        yield 'data: Clasificación completada.\n\n'
    return Response(stream_with_context(generate()), mimetype='text/event-stream')


@main.route('/distribuidoras')
def mostrar_distribuidoras():
    distribuidoras = obtener_datos_distribuidoras()
    
    return render_template('distribuidoras.html', distribuidoras=distribuidoras)