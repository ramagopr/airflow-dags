from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import urllib


from datetime import datetime

default_args = {
    'start_date': datetime(2021, 11, 24),
    'owner': 'Airflow',
}

def process(p1):
    print(p1)
    return 'done'

def downloadExplorerfile():
   
   url = 'https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/NEMO-2317/workspaces/explorer_product_list.csv'
   urllib.urlretrieve(url, filename="./explorer.csv")

   infile = open('explorer_product_list.csv', "r")
   read = csv.reader(infile)
   returnDictionary={}

   for row in read:
     key   = row[0]
     value = row[1]
     returnDictionary[key] = value

   return returnDictionary

def getExplorerDict():
   explorer_dictionary = downloadExplorerfile()
   print(explorer_dictionary)
    
with DAG(dag_id='explorer', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    
    # Tasks dynamically generated 
    tasks = [BashOperator(task_id='task_{0}'.format(t), bash_command='sleep 60'.format(t)) for t in range(1, 4)]

    task_4 = PythonOperator(task_id='task_4', python_callable=getExplorerDict)

    tasks >> task_4 
    
    
    
    
