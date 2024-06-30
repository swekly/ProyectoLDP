import requests
import pandas as pd
from io import BytesIO
import mysql.connector
from renovate import renovate

# 1. Conexión a MySQL (sin especificar base de datos)
connection = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="1234"
)

cursor = connection.cursor()

# 2. Crear base de datos si no existe
DB_NAME = "PRUEBA"
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
cursor.execute(f'USE {DB_NAME}')
connection.database = DB_NAME

# 3. Crear tablas
# https://stackoverflow.com/questions/53872471/python-mysql-connector-multiple-statements
for command in renovate:
    for result in cursor.execute(command, multi=True):
        if result.with_rows:
            print("Rows produced by statement '{}':".format(
            result.statement))
            print(result.fetchall())
        else:
            print("Number of rows affected by statement '{}': {}".format(
            result.statement, result.rowcount), '\n')
record = cursor.fetchall()

# 4. Leer XLSX y obtener DataFrame
url = "https://datos.gob.cl/dataset/adbf9537-7602-470f-a3ae-2b7e32922e65/resource/0d3470c7-73bd-4255-a975-18905e39d9d6/download/datos-abiertos-24.06.2024-.xlsx"
response = requests.get(url)
response.raise_for_status()

with BytesIO(response.content) as fh:
    df = pd.read_excel(fh, engine="openpyxl")

df = df.rename(columns={df.columns[1]: 'Año', df.columns[2]: 'Tipo_Pago'})


# 5. Insertar datos en MySQL
for i, row in df.iterrows():
    
    # Datos para la tabla "PROPIETARIO"
    queryBuscaIDComunaP = f"SELECT ID_Comuna FROM COMUNA WHERE Nombre_Comuna = '{row["Comuna_Propietario"]}';"
    cursor.execute(queryBuscaIDComunaP)
    resultado_comuna = cursor.fetchone()

    # Manejar el caso donde la comuna no existe
    if resultado_comuna is None:
        id_comuna = 'NULL'
    else:
        id_comuna = resultado_comuna[0]

    insert_propietario_query = f"INSERT INTO PROPIETARIO (ID_Comuna) VALUES ({id_comuna})"
    cursor.execute(insert_propietario_query)

    '''
    # Datos para la tabla "permisos_circulacion"
    sql_permisos = """
                INSERT INTO PERMISO_CIRCULACION (Fecha_Emision, Año, Tipo_Pago, Cuotas_Permiso, Codigo_SII, Comuna_Propietario, Comuna_Permiso, Valor_Contado, Total_a_Pagar, Comuna_Anterior, Fecha_Vencimiento)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
    val_permisos = (row["Fecha_Emision"], row["Año"], row["Tipo_Pago"], row["Cuotas Permiso"], row["Codigo_SII"], row["Comuna_Propietario"], row["Comuna_Permiso"], row["Valor_Contado"], row["Total_a_Pagar"], row["Comuna_Anterior"], row["Fecha_Vencimiento"])  # Ajusta según las columnas de permisos_circulacion
    cursor.execute(sql_permisos, val_permisos)

    try:
        cursor.execute(sql_permisos, val_permisos)
        connection.commit()
    except mysql.connector.errors.IntegrityError as e:
        if e.errno == 1062:  # Duplicate entry error (1062)
            print(f"Registro duplicado, omitiendo inserción: {row}")
        else:
            raise e 

    # Datos para la tabla "vehiculo"
    sql_vehiculo = "INSERT INTO VEHICULO (Año_Fabricacion, TipoVehiculo, Marca, ...) VALUES (%s, %s, %s, ...)"
    val_vehiculo = (row["Año_Fabricacion"], row["TipoVehiculo"], row["Marca"], ...)  # Ajusta según las columnas de vehiculo
    cursor.execute(sql_vehiculo, val_vehiculo)
    '''

connection.commit()
print(cursor.rowcount, "registro(s) insertado(s).")

connection.close()