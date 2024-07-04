import mysql.connector

# Configuración de la conexión a la base de datos
config = {
    'user': 'root',
    'password': '1234',
    'host': '127.0.0.1'
}

renovate = [
    """
    CREATE TABLE IF NOT EXISTS MARCA (
        ID_Marca INT AUTO_INCREMENT PRIMARY KEY,
        Nombre VARCHAR(20)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS MODELO (
        ID_Modelo INT AUTO_INCREMENT PRIMARY KEY,
        Nombre VARCHAR(50)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS TIPO_COMBUSTIBLE (
        ID_TCombustible INT AUTO_INCREMENT PRIMARY KEY,
        Nombre VARCHAR(20)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS COLOR (
        ID_Color INT AUTO_INCREMENT PRIMARY KEY,
        Nombre VARCHAR(50)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS GRUPO_VEHICULO (
        ID_GVehiculo INT AUTO_INCREMENT PRIMARY KEY,
        Nombre VARCHAR(20)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS TIPO_VEHICULO (
        ID_TVehiculo INT AUTO_INCREMENT PRIMARY KEY,
        Nombre VARCHAR(50)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS EQUIPAMIENTO (
        ID_Equipamiento INT AUTO_INCREMENT PRIMARY KEY,
        Nombre VARCHAR(10)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS TRANSMISION (
        ID_Transmision INT AUTO_INCREMENT PRIMARY KEY,
        Nombre VARCHAR(10)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS VEHICULO (
        Placa VARCHAR(10) PRIMARY KEY,
        Año INT,
        Num_Puertas INT,
        Cilindrada INT NULL,
        ID_Modelo INT,
        ID_TCombustible INT NULL,
        ID_Color INT,
        ID_TVehiculo INT,
        ID_Equipamiento INT NULL,
        ID_Transmision INT NULL,
        FOREIGN KEY (ID_Modelo) REFERENCES MODELO(ID_Modelo),
        FOREIGN KEY (ID_TCombustible) REFERENCES TIPO_COMBUSTIBLE(ID_TCombustible),
        FOREIGN KEY (ID_Color) REFERENCES COLOR(ID_Color),
        FOREIGN KEY (ID_TVehiculo) REFERENCES TIPO_VEHICULO(ID_TVehiculo),
        FOREIGN KEY (ID_Equipamiento) REFERENCES EQUIPAMIENTO(ID_Equipamiento),
        FOREIGN KEY (ID_Transmision) REFERENCES TRANSMISION(ID_Transmision)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS FORMA_PAGO (
        ID_FPago INT AUTO_INCREMENT PRIMARY KEY,
        Nombre VARCHAR(10)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS TIPO_PAGO (
        ID_TPago INT AUTO_INCREMENT PRIMARY KEY,
        Nombre VARCHAR(10)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS PERMISO_CIRCULACION (
        ID_Permiso INT AUTO_INCREMENT PRIMARY KEY,
        Placa VARCHAR(10),
        Fecha_Pago DATE,
        Codigo_SII VARCHAR(20),
        Tasacion DECIMAL(10,2),
        Valor_Neto INT,
        Valor_IPC INT,
        Valor_Multa INT,
        Valor_Pagado INT,
        Año_Permiso YEAR,
        Dígito CHAR(1),
        ID_TPago INT,
        ID_FPago INT,
        FOREIGN KEY (ID_TPago) REFERENCES TIPO_PAGO(ID_TPago),
        FOREIGN KEY (ID_FPago) REFERENCES FORMA_PAGO(ID_FPago),
        FOREIGN KEY (Placa) REFERENCES VEHICULO(Placa)
    )
    """
]