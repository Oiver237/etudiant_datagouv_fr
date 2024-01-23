from airflow import DAG
from datetime import timedelta,datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3
from airflow.operators.bash import BashOperator

s3_client=boto3.client('s3')

target_bucket_name='olivier-datagouv-education-transform'

url_data='https://www.data.gouv.fr/fr/datasets/r/b10fd6c8-6bc9-41fc-bdfd-e0ac898c674a'

def extract_data(**kwargs):
    url=kwargs['url']
    df=pd.read_csv(url, sep=';')
    now=datetime.now()
    date_now_string= now.strftime("%d%m%Y%H%M%S")
    file_str= 'data_gouv_'+date_now_string
    df.to_csv(f"{file_str}.csv", index=False, sep=';')
    output_file_path=f"/home/lifu237/{file_str}.csv"
    output_list= [output_file_path,file_str]
    return output_list


def transform_data(task_instance):
    data=task_instance.xcom_pull(task_ids="tsk_extract_data_gouv")[0]
    object_key= task_instance.xcom_pull(task_ids="tsk_extract_data_gouv")[1]
    df=pd.read_csv(data, sep=';')

    print("Columns in DataFrame:")
    print(df.columns)
    print("\nFirst few rows of DataFrame:")
    print(df.head())

    columns=['rentree', 'categorie_etablissement','secteur_etablissement','sigle_etablissement', 'libelle_etablissement_1','libelle_etablissement_2','reg_nom','aca_nom','geo','degetu','degre_etudes','effectifhdccpge',
         'dont_femmes','dont_hommes']

    
    df=df[columns]
    df.drop_duplicates()
    df[['latitude', 'longitude']] = df['geo'].str.extract(r'([0-9]+\.[0-9]+),\s*([0-9]+\.[0-9]+)', expand=True)
    df['latitude'] = pd.to_numeric(df['latitude'])
    df['longitude'] = pd.to_numeric(df['longitude'])
    df = df.drop('geo', axis=1)
    df['effectifhdccpge'] = pd.to_numeric(df['effectifhdccpge'], errors='coerce')
    df = df.dropna(subset=['effectifhdccpge'])
    csv_data=df.to_csv(index=False, sep=';')

    print('Num of rows:', len(df))
    print('Num of cols:', len(df.columns))

    object_key=f"{object_key}.csv"
    s3_client.put_object(Bucket=target_bucket_name, Key=object_key, Body=csv_data)



default_args={
    'owner':'olivierassiene',
    'depends_on_past':False,
    'start_date':datetime(2024,1,17),
    'email':'connextu.webtech@gmail.com',
    'email_on_failure':False,
     'email_on_retry':False,
    'retries':2,
    'retry_delay':timedelta(seconds=30)
}

with DAG('data_gouv_analytics_dag',
         default_args=default_args,
         schedule_interval='@monthly',
         catchup=False) as dag:
    
    extract_data_gouv=PythonOperator(
        task_id='tsk_extract_data_gouv',
        python_callable=extract_data,
        op_kwargs={'url':url_data}
    )

    transform_data_gouv=PythonOperator(
        task_id='tsk_transform_data_gouv',
        python_callable=transform_data
    )

    load_to_s3=BashOperator(
        task_id='tsk_load_to_s3',
        bash_command='aws s3 mv {{ ti.xcom_pull("tsk_extract_data_gouv")[0] }} s3://olivier-datagouv-education-raw'
    )


extract_data_gouv>>transform_data_gouv>>load_to_s3