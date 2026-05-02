from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define task1
def preprocess_data():
    print("This is task 1- Preprocess Data")

# Define task2
def train_model():
    print("This is task 2- Train Model")

# Define task3
def evaluate_model():
    print("This is task 3- Evaluate Model")


# Define the DAG
with DAG(
    'ml_pipeline',
    start_date=datetime(2026, 4, 30),
    schedule='@weekly'
) as dag:
    #Define the tasks
    preprocess = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data)
    train = PythonOperator(
        task_id='train_model',
        python_callable=train_model)
    evaluate = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model) 
    # Set task dependencies
    preprocess >> train >> evaluate