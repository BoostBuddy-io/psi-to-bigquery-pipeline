import os
import json
import urllib.request
import pandas as pd
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
        row[f'{v} - time'] = f'{vitals[v]["numericValue"] / 1000}s'
        row[f'{v} - score'] = f'{vitals[v]["score"] * 100}%'

    df = pd.DataFrame([row])

    return df

url = 'https://foodready.ai/'
df = speed_test(url, 'mobile')

client = bigquery.Client(location='US')
print("Client creating using default project: {}".format(client.project))

# dataset_id = 'psi_test_data'
# dataset = client.create_dataset(dataset_id)

# table_ref = dataset.table(dataset_id)
# job = client.load_table_from_dataframe(df, table_ref, location='US')
# job.result()
# print("Loaded dataframe to {}".format(table_ref.path))


