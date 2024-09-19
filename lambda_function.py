import boto3
import json
import requests
from datetime import timedelta, datetime


def obtainData(url, startDate=None, endDate=None):
    response = requests.get(url)    
    if response.status_code == 200:
        accounts = response.json()
        # Validar la coherencia de las fechas
        if (startDate and not endDate) or (endDate and not startDate):
            return "Error: Ambas fechas, startDate y endDate, deben ser proporcionadas o ninguna."
        if startDate and endDate:
            # Obtener año, mes y día de las fechas
            start_year, start_month, start_day = map(int, startDate.split('-'))
            end_year, end_month, end_day = map(int, endDate.split('-'))
            
            start_date = datetime(start_year, start_month, start_day)
            end_date = datetime(end_year, end_month, end_day)
            if start_date > end_date:
                return "Error: startDate no puede ser posterior a endDate."
            
            # Filtrar las transacciones que cumplen con el rango de fechas
            filtered_accounts = [
                account for account in accounts 
                if 'transactionDate' in account and start_date <= datetime.strptime(account['transactionDate'], '%Y-%m-%d') <= end_date
            ]
            return filtered_accounts
        else:
            # Si no se proporcionan fechas, devolver todos los datos
            return accounts
    else:
        return f'Error al obtener los datos. Código de estado: {response.status_code}'
    
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    url = 'https://api.sampleapis.com/fakebank/accounts'
    #################CAMBIAR EL NOMBRE DEL BUCKET########################################
    rawbucket = 'fakeapi-raw-data-ingestion'
    #####################################################################################
    # revisar si el bucket esta vacio, si esta vacio traen todos los datos de la api
    # si no esta vacio, se traen los objetos del ultimo mes
    bucket = s3.list_objects(Bucket=rawbucket)

    if 'Contents' in bucket:
        # Obtener la fecha actual
        current_date = datetime.now()
        # Obtener el último mes
        last_month = current_date - timedelta(days=30)
        # Convertir la fecha a string
        last_month_str = last_month.strftime('%Y-%m-%d')
        # Filtrar los objetos del último mes
        historical_data = obtainData(url, startDate=last_month_str, endDate=current_date.strftime('%Y-%m-%d'))
    else:
        # Obtener todos los datos
        historical_data = obtainData(url)
    #toma cada registro en historical data y lo guarda en el bucket como un archivo json
    for record in historical_data:
        record_json = json.dumps(record)
        # Convertir la cadena JSON a bytes
        #meter el dato dentro de una carpeta llamada transactions en el bucket
        s3.put_object(Bucket=rawbucket, Key=f"transactions/{record['id']}.json", Body=record_json)
        print(f"Registro {record['id']} guardado exitosamente en S3.")
    print("Proceso de ingesta finalizado.")
    return {
        'statusCode': 200,
        'body': json.dumps('Proceso de ingesta finalizado.')
    }
