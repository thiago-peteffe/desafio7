from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os

# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': "thiago.oliveira@indicium.tech",
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Função para ler dados da tabela Order e exportar para CSV
def read_orders():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    query = "SELECT * FROM 'Order';"
    df_orders = pd.read_sql_query(query, conn)
    df_orders.to_csv('output_orders.csv', index=False)
    conn.close()

# Função para ler dados da tabela OrderDetail e fazer o JOIN
def count_rio_de_janeiro():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    query_orders = "SELECT * FROM 'Order';"
    df_orders = pd.read_sql_query(query_orders, conn)
    
    query_details = "SELECT * FROM 'OrderDetail';"
    df_details = pd.read_sql_query(query_details, conn)
    
    # Realizando o JOIN
    merged_df = pd.merge(df_details, df_orders, left_on='OrderId', right_on='Id')
    count = merged_df[merged_df['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()
    
    # Exportando o resultado para count.txt
    with open('count.txt', 'w') as f:
        f.write(str(count))

    conn.close()

# Função para exportar o resultado final
def export_final_output():
    with open('final_output.txt', 'w') as f:
        f.write("Este é o resultado final gerado automaticamente pelo Airflow.")

# Definindo o DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'northwind_dag',
    default_args=default_args,
    description='Um DAG simples para trabalhar com o banco de dados Northwind',
    schedule_interval=None,
)

# Definindo as tasks
read_orders_task = PythonOperator(
    task_id='read_orders',
    python_callable=read_orders,
    dag=dag,
)

count_rio_de_janeiro_task = PythonOperator(
    task_id='count_rio_de_janeiro',
    python_callable=count_rio_de_janeiro,
    dag=dag,
)

export_final_output_task = PythonOperator(
    task_id='export_final_output',
    python_callable=export_final_output,
    dag=dag,
)

# Definindo a ordem de execução
read_orders_task >> count_rio_de_janeiro_task >> export_final_output_task