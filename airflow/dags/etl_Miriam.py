from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import time
from airflow.utils.task_group import TaskGroup

# FUNCIONES 


def extraccion ():
    data = {
        "producto": ["Laptop","Mouse","Teclado","Audifonos","Microfono"],
        "precio": [10567,170,230,345,208]
    }
    df = pd.DataFrame(data)
    df.to_csv("/tmp/datos_productos.csv", index=False)
    print("Datos extraídos correctamente.")

def transformacion ():
    df = pd.read_csv("/tmp/datos_productos.csv")
    df["precio_descuento"] = df["precio"] * 0.91
    df.to_csv("/tmp/datos_productos_transformados.csv", index=False)
    print("Datos transformados correctamente.")
    print(df)

def carga ():
    df = pd.read_csv("/tmp/datos_productos_transformados.csv")
    promedio = df["precio"].mean()
    print(f"El promedio de precio es: {promedio}")

def reporte_dos():
    df = pd.read_csv("/tmp/datos_productos_transformados.csv")
    promedio = df["precio"].mean()
    productos_caros = df[df["precio"] > promedio]
    print(f"Productos más caros que el promedio: ")
    print(productos_caros)



# Configuración del DAG

default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=10)
}

with DAG (
    dag_id="etl_productos_ejercicio",
    start_date=datetime(2026, 5, 12),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    description="ETL básico"
) as dag:
    
    inicio = EmptyOperator(task_id="inicio_etl")
 
    tarea_extraer = PythonOperator(
        task_id="extraer_datos",
        python_callable=extraccion 
    )
 
    tarea_transformacion = PythonOperator(
        task_id="transformacion_datos",
        python_callable=transformacion
    )

    fin = EmptyOperator(task_id="fin_etl")

    #Agrupacion

    with TaskGroup("grupo_carga") as grupo_carga:

        tarea_cargar = PythonOperator(
        task_id="carga_datos",
        python_callable=carga
        )

        tarea_reporte_dos = PythonOperator(
            task_id="reporte_dos",
            python_callable=reporte_dos
        )

        tarea_cargar>> tarea_reporte_dos

inicio >> tarea_extraer >> grupo_carga >> fin
tarea_extraer >> tarea_transformacion