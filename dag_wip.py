
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models import Variable

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
import requests

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier

@dag(DAG_NAME, default_args=default_args, schedule_interval="* 4-23,0-2 * * *", start_date=days_ago(2))
def dag_projet():

    @task()
    def get_and_save_data(date):
        USER_SNCF = Variable.get("user_sncf")
        PASS_SNCF = Variable.get("password_sncf")

        url = [
        "https://api.transilien.com/gare/87271460/depart",  # Charles de gaulles 2
        "https://api.transilien.com/gare/87271460/depart",  # Charles de gaulles 1
        "https://api.transilien.com/gare/87271452/depart",  # Parc des expositions
        "https://api.transilien.com/gare/87271429/depart",  # Villepinte
        "https://api.transilien.com/gare/87271411/depart",  # Sevran Beaudottes

        "https://api.transilien.com/gare/87271510/depart",  # Mitry Clay
        "https://api.transilien.com/gare/87271437/depart",  # Villeparisis Mitry-le-Neuf
        "https://api.transilien.com/gare/87271528/depart",  # Vert Galant
        "https://api.transilien.com/gare/87271445/depart",  # Sevran Livry

        "https://api.transilien.com/gare/87271486/depart",  # Aulnay Sous bois
        "https://api.transilien.com/gare/87271478/depart",  # Le Blanc Mesnil
        "https://api.transilien.com/gare/87271403/depart",  # Drancy
        "https://api.transilien.com/gare/87271395/depart",  # Le Bourget
        "https://api.transilien.com/gare/87271304/depart",  # La Courneuve - Aubervilliers
        "https://api.transilien.com/gare/87164798/depart",  # La Plaine Stade-de-France
        "https://api.transilien.com/gare/87271007/depart"]  # Paris Gare-du-Nord

        df_array = []

        response = []
        for u in url:
           response.append(requests.request("GET", u, auth=(USER_SNCF, PASS_SNCF)))
        for u in response:
            as_dict = xmltodict.parse(u.content)
            s = json.dumps(as_dict).replace('\'', '"').replace('#', '').replace('@', '')
            json_object = json.loads(s)
            df_array.append(make_df(json_object))
        # df = pd.DataFrame()
        # for u in df_array:
        df = pd.concat(df_array)
        pattern = r'^\D'
        df.reset_index(drop=True, inplace=True)
        df = df[df['num'].str.contains(pattern)]
        

    
    def make_df(js):
        df = pd.DataFrame.from_dict(js, orient='index')
        df = df.explode('train').reset_index(drop=True)
        df = df.join(pd.json_normalize(df['train'])).drop('train', axis=1)
        return df
        
dag_projet_instances = dag_projet()  # Instanciation du DAG
