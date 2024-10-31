# clasificacion.py
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import json
import pyodbc
from datetime import datetime
import time
from sklearn.metrics import accuracy_score
import gc

def load_config():
    try:
        with open("config.json", "r") as file:
            config = json.load(file)
        return config
    except Exception as e:
        print(f"Error al cargar configuración: {e}")
        raise

def clasificar_productos():
    start_time = time.time()
    result = {}

    try:
        config = load_config()
        connection_string = (
            f"DRIVER={{SQL Server}};SERVER={config['server']};DATABASE={config['database']};"
            f"UID={config['username']};PWD={config['password']}"
        )

        with pyodbc.connect(connection_string) as connection:
            
            # Carga de datos
            load_data_start = time.time()
            df_ventas = pd.read_sql("SELECT * FROM dbo.VENTAS", connection)
            df_equivalencias = pd.read_sql("SELECT * FROM dbo.EQUIVALENCIAS", connection)
            load_data_end = time.time()
            result["tiempo_carga_datos"] = load_data_end - load_data_start

            # Procesamiento de datos
            process_data_start = time.time()
            df_ventas['KEY'] = df_ventas['COD_DISTRIBUIDORA'].astype(str) + '-' + df_ventas['COD_PRODUCTO'].astype(str)
            df_equivalencias['KEY'] = df_equivalencias['COD_DISTRIBUIDORA'].astype(str) + '-' + df_equivalencias['COD_PROD_DISTRIBUIDOR'].astype(str)
            df_ventas['Homologado'] = np.where(df_ventas['KEY'].isin(df_equivalencias['KEY']), 'Homologados', 'No Homologados')
            process_data_end = time.time()
            result["tiempo_procesamiento_datos"] = process_data_end - process_data_start

            # Modelo de machine learning
            ml_model_start = time.time()
            X = pd.get_dummies(df_ventas[['KEY']])
            y = df_ventas['Homologado']
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            model = RandomForestClassifier(n_estimators=100, random_state=42)
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)
            result["accuracy"] = accuracy
            df_ventas.loc[X_test.index, 'Predicted_Homologado'] = y_pred
            ml_model_end = time.time()
            result["tiempo_modelo_ml"] = ml_model_end - ml_model_start

            # Inserción en base de datos
            insertion_start = time.time()
            now = datetime.now()
            df_unique_ventas = df_ventas.loc[X_test.index].drop_duplicates(subset=['COD_DISTRIBUIDORA', 'COD_PRODUCTO'])

            with connection.cursor() as cursor:
                for index, row in df_unique_ventas.iterrows():
                    cursor.execute(
                        "INSERT INTO RESULTADOS_CLASIFICACION_RF (COD_DISTRIBUIDORA, COD_PRODUCTO, Predicted_Homologado, Fecha_Registro) VALUES (?, ?, ?, ?)",
                        row['COD_DISTRIBUIDORA'],
                        row['COD_PRODUCTO'],
                        row['Predicted_Homologado'],
                        now
                    )
                connection.commit()
            insertion_end = time.time()
            result["tiempo_insercion_bd"] = insertion_end - insertion_start

    except Exception as e:
        result["error"] = str(e)
        return result
    finally:
        gc.collect()
        total_time = time.time() - start_time
        result["tiempo_total"] = total_time

    return result
