import requests
import pandas as pd
import xmltodict
import json
from datetime import datetime


def make_df(js):
    df = pd.DataFrame.from_dict(js, orient='index')
    df = df.explode('train').reset_index(drop=True)
    df = df.join(pd.json_normalize(df['train'])).drop('train', axis=1)
    return df


def main():
    payload = {}
    headers = {
        'Authorization': 'Basic '
    }

    df_array = []
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

    response = []
    for u in url:
        response.append(requests.request("GET", u, headers=headers, data=payload))

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


    unix_timestamp = datetime.now().timestamp()
    # Getting date and time in local time
    datetime_obj = datetime.fromtimestamp(int(unix_timestamp))
    df = df.assign(timestamp=datetime_obj)
    print(df.to_string())



if __name__ == "__main__":
    main()
