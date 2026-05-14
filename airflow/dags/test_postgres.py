from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging 
import pandas as pd
from io import StringIO
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
 
def probar_conexion():
    hook = PostgresHook(postgres_conn_id="postgres_local")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT current_database();")
    result = cursor.fetchone()
    print(f"Conectado a la base: {result[0]}")

def crear_tabla():
    tabla = Variable.get("tabla_origen")
    hook = PostgresHook(postgres_conn_id="postgres_local")
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {tabla} (
            id SERIAL PRIMARY KEY,
            nombre VARCHAR(50),
            edad INT
        );
    """)
    print("Tabla creada correctamente.")

def insertar_datos():
    tabla = Variable.get("tabla_origen")
    hook = PostgresHook(postgres_conn_id="postgres_local")
    hook.run(f"""
        TRUNCATE TABLE {tabla} ;
        INSERT INTO {tabla} (nombre, edad)
        VALUES
        ('Daniel', 25),
        ('Maria', 30),
        ('Carlos', 22);
    """)
    print("Datos insertados correctamente.")

def leer_datos():
    tabla = Variable.get("tabla_origen")
    hook = PostgresHook(postgres_conn_id="postgres_local")
    records = hook.get_records(f"SELECT * FROM {tabla};")
   
    print("Registros encontrados:")
    for row in records:
        print(row)

def leer_postgres(ti):
    tabla = Variable.get("tabla_origen")
    hook = PostgresHook(postgres_conn_id="postgres_local")
    df = hook.get_pandas_df(f"SELECT * FROM {tabla};")
    ti.xcom_push(key="datos_alumnos", value=df.to_json())
    logging.info("Datos leidos desde Postgres")

def transformar_datos(ti):
    data_json = ti.xcom_pull(key="datos_alumnos", task_ids="leer_postgres")
    df = pd.read_json(StringIO(data_json))

    df["edad"] = df["edad"] + 1
    df["mayor_edad"] = df ["edad"] >= 30
    logging.info("Datos Transformados")
    print(df)

    ti.xcom_push(key="datos_transformados",value=df.to_json())

def crear_tabla_destino():
    tabla_destino = Variable.get("tabla_destino")
    hook = PostgresHook(postgres_conn_id="postgres_local")
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {tabla_destino} (
            id INT,
            nombre VARCHAR(50),
            edad INT,
            mayor_edad BOOLEAN
        );
    """)
    logging.info("Tabla destino verificada.")

def cargar_datos_transformados(ti):
    tabla_destino = Variable.get("tabla_destino")
    data_json = ti.xcom_pull(key="datos_transformados", task_ids="transformar_datos")
    df = pd.read_json(StringIO(data_json))

    hook = PostgresHook(postgres_conn_id="postgres_local")

    hook.run(f"TRUNCATE TABLE {tabla_destino};")

    for _, row in df.iterrows():
        hook.run(
            f"""
            INSERT INTO {tabla_destino} (id, nombre, edad, mayor_edad)
            VALUES (%s, %s, %s, %s);
            """,
            parameters=(int(row["id"]), row["nombre"], int(row["edad"]), bool(row["mayor_edad"]))
        )

    logging.info("Datos transformados cargados correctamente.")

def tarea_que_falla():
    raise ValueError("Error intencional para prpbar trigger rules")

def tarea_final():
    logging.info("Esta tarea se ejecuta aunque existan fallas")

def datos_nuevos():
    data = {
        "producto": ["Laptop","Mouse","Teclado","Audifonos","Microfono"],
        "precio": [10567,170,230,345,208]
    }
    df = pd.DataFrame(data)
    df.to_csv("/tmp/archivo_trigger.txt", index=False)
    print("Datos correctamente.")

with DAG(
    dag_id="test_postgres_hook",
    start_date=datetime(2026, 5, 13),
    schedule=None,
    catchup=False
) as dag:

    inicio = EmptyOperator(task_id="inicio_etl")
 
    test = PythonOperator(
        task_id="probar_conexion",
        python_callable=probar_conexion
    )

    crear = PythonOperator(
        task_id="crear_tabla",
        python_callable=crear_tabla
    )
    
    insertar = PythonOperator(
        task_id="insertar_datos",
        python_callable=insertar_datos
    )

    leer = PythonOperator(
        task_id="leer_datos",
        python_callable=leer_datos
    )

    leer_post = PythonOperator(
        task_id="leer_postgres",
        python_callable=leer_postgres
    )
    
    transformar = PythonOperator(
        task_id="transformar_datos",
        python_callable=transformar_datos
    )

    crear_destino = PythonOperator(
        task_id="crear_tabla_destino",
        python_callable=crear_tabla_destino
    )

    cargar_transformados = PythonOperator(
        task_id="cargar_datos_transformados",
        python_callable=cargar_datos_transformados
    )

    falla = PythonOperator(
        task_id="tarea_que_falla",
        python_callable=tarea_que_falla
    )

    tarea_final_siempre_ejecuta = PythonOperator(
        task_id="tarea_final",
        python_callable=tarea_final,
        trigger_rule="all_done"
    )

    email_alerta = EmailOperator(
        task_id="email_alerta",
        to="miriam.ramirez@axity.com",
        subject="Airflow Alert: DAG Falló",
        html_content="""
            <h3>Una tarea ha fallado en el DAG</h3>
            <p>Revisa los logs inmediatamente.</p>
        """,
        trigger_rule="one_failed"
    )

    datos_nuevos = PythonOperator(
        task_id="datos_nuevos",
        python_callable=datos_nuevos
    )
    
    esperar_archivo = FileSensor(
        task_id="esperar_archivo",
        fs_conn_id="fs_default",
        filepath="/tmp/archivo_trigger.txt",
        poke_interval=10,
        timeout=60,
        mode="poke"
    )

    fin = EmptyOperator(task_id="fin_etl")

 
inicio >> test >> crear >> datos_nuevos >> esperar_archivo >> insertar >> leer >> leer_post >> transformar >> crear_destino >> cargar_transformados >> falla >> email_alerta >> tarea_final_siempre_ejecuta >> fin

