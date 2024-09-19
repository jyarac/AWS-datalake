import boto3
import time

################################################
##### Aprovisionamiento de pipeline en glue ####
################################################


# Creación de database para almacenar esquema y datos encontrados por el Crawler 
def createGlueDatabase(database_name, region):
    glue_client = boto3.client('glue', region_name=region)
    response = glue_client.create_database(
        DatabaseInput={
            'Name': database_name,
            'Description': 'Base de datos para almacenar las tablas generadas por el crawler de Glue.'
        }
    )
    print(f"Base de datos {database_name} creada con éxito.")


def create_glue_database(database_name, region):
    glue_client = boto3.client('glue', region_name=region)
    response = glue_client.create_database(
        DatabaseInput={
            'Name': database_name,
            'Description': 'Base de datos para almacenar las tablas generadas por el crawler de Glue.'
        }
    )
    print(f"Base de datos {database_name} creada con éxito.")

# Creacion de crawler para encontrar esquema de los datos crudos almacenados en el bucket 

def create_glue_crawler(crawler_name, database_name, s3_target_path, role_arn, region):
    glue_client = boto3.client('glue', region_name=region)
    response = glue_client.create_crawler(
        Name=crawler_name,
        Role=role_arn,
        DatabaseName=database_name,
        Targets={
            'S3Targets': [
                {
                    'Path': s3_target_path
                }
            ]
        },
        Description='Crawler para identificar la estructura de los datos en S3.',
        Schedule='cron(0 0 */30 * ? *)',  # Programar para que se ejecute mensualmente
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'LOG'
        }
    )
    print(f"Crawler {crawler_name} creado con éxito.")



# luego de la creación del crawler y la base de datos, esta funcion inicializa el crawler para hacer un primer recorrido

def start_and_wait_for_crawler(crawler_name, region):
    glue_client = boto3.client('glue', region_name=region)
    glue_client.start_crawler(Name=crawler_name)
    print(f"Crawler {crawler_name} iniciado.")
    
    while True:
        response = glue_client.get_crawler(Name=crawler_name)
        state = response['Crawler']['State']
        if state == 'READY':
            print(f"Crawler {crawler_name} ha terminado.")
            break
        else:
            print(f"Estado del crawler {crawler_name}: {state}. Esperando 30 segundos...")
            time.sleep(30)

if __name__ == "__main__":
    # Cambiar con el nombre de la abse de datos en la que va a quedar almacenados los datos
    database_name = 'fakeapi_raw_data'
    # Cambiar con el crawler
    crawler_name = 'fakeapi_data_crawler'
    # Poner link donde estan almacenados los datos crudos
    s3_target_path = 's3://fakeapi-raw-data-ingestion/'  
    # Poner rol de servicio creado para Glue
    role_arn = "arn:aws:iam::288185525184:role/service-role/AWSGlueServiceRole"  
    # Cambiar con region en la que se esta realizando el despliegue
    region = 'us-east-1'

    create_glue_database(database_name=database_name, region=region)
    create_glue_crawler(crawler_name=crawler_name, database_name=database_name, s3_target_path=s3_target_path, role_arn=role_arn, region=region)
    start_and_wait_for_crawler(crawler_name, region)