

def get_data(API_KEY, LOCAL_PATH):
    from datetime import date
    import pandas as pd
    import requests

    local_path = LOCAL_PATH
    hoy = date.today()
    hoy_str = hoy.strftime("%Y-%m-%d")
    file = f'raw_{hoy_str}.csv'
    full_path = LOCAL_PATH + file
    api_key = API_KEY
    moneda_base = 'MXN'
    target_monedas = ['USD', 'EUR', 'JPY', 'GBP', 'CAD']

    df = pd.DataFrame()
    for divisa in target_monedas:
        url = f'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={divisa}&to_currency={moneda_base}&apikey={api_key}'
        response = requests.get(url)
        data = response.json()

        if 'Realtime Currency Exchange Rate' in data:
            xchange = data['Realtime Currency Exchange Rate']
            df = df.append(xchange, ignore_index=True)

    df.to_csv(full_path, index=False)

def upload_to_gcs():
    from google.cloud import storage
    from datetime import date

    hoy = date.today()
    hoy_str = hoy.strftime("%Y-%m-%d")

    local_path = "D:\\Documents\\raw_curr\\"
    file = f'raw_{hoy_str}.csv'
    full_path = local_path + file

   
    

    bucket_name = 'raw_bucket_diego'
    destination_blob_name = f'gs:/raw_bucket_diego/{file}'

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(filename = full_path)
