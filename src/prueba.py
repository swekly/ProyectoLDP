import mysql.connector
import requests
import pandas as pd
from io import BytesIO
from config import config, renovate

def obtener_datos_ckan(resource_id, base_url="https://datos.gob.cl/api/3/action"):
    """Obtiene todos los registros de un recurso CKAN en un DataFrame."""

    try:
        # Obtener el total de registros
        url_total = f"{base_url}/datastore_search"
        params = {
            'resource_id': resource_id,
            'limit': 0
        }
        response_total = requests.get(url_total, params=params)
        response_total.raise_for_status()  # Lanzar una excepción si hay un error HTTP
        limit = response_total.json()["result"]["total"]

        # Obtener todos los registros en lotes
        records = []
        batch_size = 1000  # Puedes ajustar este valor según sea necesario
        for offset in range(0, limit, batch_size):
            params = {
                'resource_id': resource_id,
                'limit': batch_size,
                'offset': offset
            }
            response_records = requests.get(url_total, params=params)
            response_records.raise_for_status()
            records.extend(response_records.json()["result"]["records"])

        # Crear el DataFrame
        df = pd.DataFrame(records)

        # Convertir "NULL" a None en el DataFrame
        df = df.replace("NULL", None)
        
        # Eliminar duplicados basados en 'Placa'
        df = df.drop_duplicates(subset=['Placa'])

        return df

    except requests.RequestException as e:
        print(f"Error al obtener datos de CKAN: {e}")
        #return pd.DataFrame()  # Retornar un DataFrame vacío en caso de error
        return None
    except KeyError as e:
        print(f"Error al procesar la respuesta: {e}")
        #return pd.DataFrame()
        return None
    
def obtener_vehiculos_existentes(cursor):
    """Obtiene los registros existentes en la tabla VEHICULO."""
    cursor.execute("SELECT * FROM VEHICULO")
    #return cursor.fetchall()
    return {record['Placa']: record for record in cursor.fetchall()}

def registrosDiferentes(record_db, record_ckan):
    """Compara dos registros y determina si son diferentes."""
    keys = ['Placa', 'Año', 'Num_Puertas', 'Cilindrada', 'ID_Modelo', 'ID_TCombustible', 'ID_Color', 'ID_TVehiculo', 'ID_Equipamiento', 'ID_Transmision']
    for key in keys:
        if record_db.get() != record_ckan.get(key):
            return True
    return False

def obtener_id(cursor, tabla, nombre, valor):
    """Obtiene el ID de una tabla dado un valor específico, insertando el valor si no existe."""
    cursor.execute(f"SELECT {nombre} FROM {tabla} WHERE Nombre = '{valor}';")
    result = cursor.fetchone()
    if result:
        return result[f"{nombre}"]
    else:
        cursor.execute(f"INSERT INTO {tabla} (Nombre) VALUES ('{valor}');")
        return cursor.lastrowid

def updateVehiculo(conn, cursor, df):
    # Insertar o actualizar los datos de vehiculos en la tabla vehiculo
    try:
        registrosExistentes = obtener_vehiculos_existentes(cursor)
        #registrosExistentesDict = {v['Placa']: v for v in registrosExistentes}

        for index, record in df.iterrows():
            placa = record.get('Placa', None)
            print(placa)
            if placa:
                # Obtener o insertar las llaves foráneas necesarias
                idModelo = obtener_id(cursor, "MODELO", "ID_Modelo", record.get('Modelo'))
                idTCombustible = obtener_id(cursor, "TIPO_COMBUSTIBLE", "ID_TCombustible", record.get('Tipo Combustible')) if record.get('Tipo Combustible') else None
                idColor = obtener_id(cursor, "COLOR", "ID_Color", record.get('Color'))
                idTVehiculo = obtener_id(cursor, "TIPO_VEHICULO", "ID_TVehiculo", record.get('Tipo Vehiculo'))
                idEquipamiento = obtener_id(cursor, "EQUIPAMIENTO", "ID_Equipamiento", record.get('Equipamiento')) if record.get('Equipamiento') else None
                idTransmision = obtener_id(cursor, "TRANSMISION", "ID_Transmision", record.get('Transmisión')) if record.get('Transmisión') else None

                record['ID_Modelo'] = idModelo
                record['ID_TCombustible'] = idTCombustible
                record['ID_Color'] = idColor
                record['ID_TVehiculo'] = idTVehiculo
                record['ID_Equipamiento'] = idEquipamiento
                record['ID_Transmision'] = idTransmision

                if placa in registrosExistentes:
                    if registrosDiferentes(registrosExistentes[placa], record):
                        query = """
                        UPDATE VEHICULO SET
                            Año = %s, Num_Puertas = %s, Cilindrada = %s,
                            ID_Modelo = %s, ID_TCombustible = %s, ID_Color = %s, ID_TVehiculo = %s,
                            ID_Equipamiento = %s, ID_Transmision = %s
                        WHERE Placa = %s
                        """
                        cursor.execute(query, (
                            record.get('Año Vehículo'), record.get('Numero Puertas'),
                            record.get('Cilindrada') if record.get('Cilindrada') else None, idModelo, idTCombustible, idColor,
                            idTVehiculo, idEquipamiento, idTransmision, placa
                        ))
                        print("dede")
                else:
                    query = """
                    INSERT INTO VEHICULO (Placa, Año, Num_Puertas, Cilindrada,
                                        ID_Modelo, ID_TCombustible, ID_Color, ID_TVehiculo,
                                        ID_Equipamiento, ID_Transmision)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(query, (
                        placa, record.get('Año Vehículo'), record.get('Numero Puertas'),
                        record.get('Cilindrada') if record.get('Cilindrada') else None, idModelo, idTCombustible, idColor,
                        idTVehiculo, idEquipamiento, idTransmision
                    ))
                 # Añadir la placa a vehiculos_existentes para futuras verificaciones
                registrosExistentes[placa] = record

        conn.commit()
        print("Datos de vehículos insertados/actualizados correctamente.")

    except mysql.connector.Error as e:
        conn.rollback()
        print(f"Error de MySQL: {e}")
    
    #except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
  
def main():
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor(dictionary=True) #Para obtener resultados como diccionarios

        # 2. Crear base de datos si no existe
        DB_NAME = "PRUEBA"
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        cursor.execute(f'USE {DB_NAME}')
        conn.database = DB_NAME

        # Ejecutar cada script para crear las tablas
        for query in renovate:
            cursor.execute(query)
        '''
        for command in renovate:
            for result in cursor.execute(command, multi=True):
                if result.with_rows:
                    print("Rows produced by statement '{}':".format(
                    result.statement))
                    print(result.fetchall())
                else:
                    print("Number of rows affected by statement '{}': {}".format(
                    result.statement, result.rowcount), '\n')
        #record = cursor.fetchall()
        '''
        
        resource_id = "a95bcfb6-3800-4931-8e52-e0862417c058"
        df = obtener_datos_ckan(resource_id)

        if df is not None:
            # Insertar o actualizar registros en la tabla Vehiculo
            updateVehiculo(conn, cursor, df)

        # Llamar a la función para obtener registros de CKAN
        #resource_id = "a95bcfb6-3800-4931-8e52-e0862417c058"
        #records = obtener_datos_ckan(resource_id)
        #for record in records:
        #    print(record)

        #if records:
        #    # Insertar registros en la tabla VEHICULO
        #    insertar_datos_vehiculo(conn, cursor, records)

        # Confirmar los cambios y cerrar la conexión
        conn.commit()
        cursor.close()
        conn.close()

        print("Tablas creadas correctamente.")

    except mysql.connector.Error as e:
        print(f"Error de MySQL: {e}")

    #except Exception as e:
    #    print(f"Error: {e}")

if __name__ == "__main__":
    main()

# Uso de la función
#resource_id = "a95bcfb6-3800-4931-8e52-e0862417c058"
#df = obtener_datos_ckan(resource_id)

'''
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
#record = cursor.fetchall()

# 4. Leer XLSX y obtener DataFrame

try:
    response = requests.get(url)
    response.raise_for_status()

    with BytesIO(response.content) as fh:
        df = pd.read_excel(fh, engine="openpyxl")


    # 5. Insertar datos en MySQL
    for i, row in df.iterrows():
        IDPropietario = row['ID_Propietario']

        # Datos para la tabla "PROPIETARIO"
        queryBuscaIDComunaP = f"SELECT ID_Comuna FROM COMUNA WHERE Nombre_Comuna = '{row["Comuna_Propietario"]}';"
        cursor.execute(queryBuscaIDComunaP)
        resultado_comuna = cursor.fetchone()

        # Consulta para verificar si el ID_Propietario existe
        select_query = "SELECT COUNT(*) FROM PROPIETARIO WHERE ID_Propietario %s"
        cursor.execute(select_query, (IDPropietario,))
        existe = cursor.fetchone()[0] > 0

        # Manejar el caso donde la comuna no existe
        if resultado_comuna is None:
            id_comuna = 'NULL'
        else:
            id_comuna = resultado_comuna[0]

        insert_propietario_query = f"INSERT INTO PROPIETARIO (ID_Comuna) VALUES (%s) ON DUPLICATE KEY UPDATE ID_Comuna = VALUES({id_comuna});"
        cursor.execute(insert_propietario_query)

        
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
        

    connection.commit()
print(cursor.rowcount, "registro(s) insertado(s).")
'''