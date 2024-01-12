import os
import json
import argparse
import pandas as pd
import urllib.request
from urllib.parse import urlparse
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

ROOT = os.path.dirname(__file__)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(ROOT, 'credentials.json')
PSI_API_KEY = os.getenv('PSI_API_KEY')

def speed_test(url, strategy):
    api_url = f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url={url}&strategy={strategy}&locale=en&key={PSI_API_KEY}"

    response = urllib.request.urlopen(api_url)
    data = json.loads(response.read())

    with open(os.path.join(ROOT, 'data.json'), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    # core web vitals
    vitals = {
        'FCP': data['lighthouseResult']['audits']['first-contentful-paint'],
        'LCP': data['lighthouseResult']['audits']['largest-contentful-paint'],
        'FID': data['lighthouseResult']['audits']['max-potential-fid'],
        'TBT': data['lighthouseResult']['audits']['total-blocking-time'],
        'CLS': data['lighthouseResult']['audits']['cumulative-layout-shift']
    }

    row = {}

    for v in vitals:
        row[f'{v}_time'] = f'{round(vitals[v]["numericValue"] / 1000, 4)}s'
        row[f'{v}_score'] = f'{round(vitals[v]["score"] * 100, 2)}%'

    df = pd.DataFrame([row])

    return df

def write_to_bq(dataset_id, table_id, dataframe):

    client = bigquery.Client(location='US')
    print("Client creating using default project: {}".format(client.project))

    dataset_ref = client.dataset(dataset_id)

    try:
        client.create_dataset(dataset_id)
    except:
        client.get_dataset(dataset_ref)
    
    table_ref = dataset_ref.table(table_id)
    job = client.load_table_from_dataframe(dataframe, table_ref, location='US')
    job.result()
    print("Loaded dataframe to {}".format(table_ref.path))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Audit webpage on PageSpeed Insights and store results in BigQuery dataset'
    )

    parser.add_argument(
        '-u',
        '--url',
        required=True,
        type=str,
        help='Provide URL to audit'
    )

    args = parser.parse_args()

    dataset_id = urlparse(args.url).netloc.split('.')[0]
    if '-' in dataset_id:
        dataset_id = dataset_id.replace('-', '_')
        
    strategies = ['mobile', 'desktop']
    for strategy in strategies:
        df = speed_test(args.url, strategy)
        table_id = f'{dataset_id}_{strategy}'
        write_to_bq(dataset_id, table_id, df)

