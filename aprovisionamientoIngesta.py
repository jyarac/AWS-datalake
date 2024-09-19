import boto3
import os
import subprocess
import shutil
import zipfile

def create_s3_buckets(bucket1, bucket2):
    aws_management_console = boto3.session.Session(profile_name='default')
    s3 = aws_management_console.client('s3')

    #verificacion si existen los buckets
    bucket1_exists = False
    bucket2_exists = False
    buckets = s3.list_buckets()

    for bucket in buckets['Buckets']:
        if bucket['Name'] == bucket:
            bucket1_exists = True
        if bucket['Name'] == bucket2:
            bucket2_exists = True

    if not bucket1_exists:
        s3.create_bucket(Bucket=bucket1)
        print(f'Bucket {bucket1} creado exitosamente.')
    print(f'Aprovisionamiento de bucket {bucket1} exitoso.')
    if not bucket2_exists:
        s3.create_bucket(Bucket=bucket2)
        print(f'Bucket {bucket2} creado exitosamente.')

# Puesto que requests ni pandas son librerias estándar de Python, necesitamos crear una capa para la funcion lambda
# Esta capa contiene las dependencias necesarias 
def create_layer_package():
    # Creacion de directorio temporal para la capa
    layer_dir = 'python'
    
    # Crear directorio para las dependencias
    os.makedirs(layer_dir, exist_ok=True)

    # Instalar la dependencia en el directorio
    subprocess.check_call(['pip', 'install', 'requests', '-t', layer_dir])

    # Crear el archivo ZIP
    with zipfile.ZipFile('layer.zip', 'w', zipfile.ZIP_DEFLATED) as layer_zip:
        for root, dirs, files in os.walk(layer_dir):
            print(f"Añadiendo archivos de {root} al paquete de capa...")
            for file in files:
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, layer_dir)
                layer_zip.write(full_path, os.path.join('python', relative_path))
    # Eliminar el directorio temporal
    # Como este script se ejecuta en un computador windows, se utiliza la libreria shutil para esta tarea
    shutil.rmtree(layer_dir)
    print("Paquete de capa creado con éxito.")



#Creacion de paquete para la implementacion de la lambda
#este paquete contiene el codigo de la lambda
def create_lambda_deployment_package():
    with zipfile.ZipFile('lambda_function.zip', 'w') as z:
        z.write(r'./lambda_function.py')
    print("Paquete de implementación creado con éxito.")


# Creacion de la capa para lambda   
def create_lambda_layer(region):
    lambda_client = boto3.client('lambda', region_name=region)

    with open('layer.zip', 'rb') as f:
        layer_zip = f.read()

    response = lambda_client.publish_layer_version(
        LayerName='requests-layer',
        Description='Capa para la librería requests',
        Content={'ZipFile': layer_zip},
        CompatibleRuntimes=['python3.12'],
    )

    layer_arn = response['LayerVersionArn']
    print(f"Capa Lambda creada con éxito: {layer_arn}")
    return layer_arn


#Creacion de la funcion lambda
def create_lambda_function(layer_arn, name, region, role_arn):
    create_lambda_deployment_package()

    lambda_client = boto3.client('lambda', region_name=region)

    with open('lambda_function.zip', 'rb') as f:
        zipped_code = f.read()
    role = role_arn

    response = lambda_client.create_function(
        FunctionName=name,
        Runtime='python3.12',
        Role=role,
        Handler='lambda_function.lambda_handler',
        Code=dict(ZipFile=zipped_code),
        Timeout=300,
        MemorySize=128,
        Layers=[layer_arn]  # Añadir la capa aquí
    )

    print(f"Función Lambda creada con éxito: {response['FunctionArn']}")


############################################
##### Creacion de modulo EventBridge #######
############################################


# Este modulo se acciona la funcion lambda cada 30 dias para obtener los nuevos datos de la API

def create_eventbridge_rule(function_name, ruleName, id, region):
    events_client = boto3.client('events', region_name=region)  # Asegúrate de especificar la región correcta

    # Definir la descripción y el patrón de la regla (en este caso, mensualmente)
    rule_description = 'Regla mensual para ejecutar la función Lambda de ingesta'
    rule_name = ruleName
    schedule_expression = 'cron(0 0 */30 * ? *)' # expresion cron para ejecutar la funcion cada 30 dias

    # Crear la regla de EventBridge
    response = events_client.put_rule(
        Name=rule_name,
        ScheduleExpression=schedule_expression,
        State='ENABLED',  # La regla se habilita automáticamente
        Description=rule_description
    )

    # Asociar la regla con la función Lambda
    events_client.put_targets(
        Rule=rule_name,
        Targets=[
            {
                'Id': '1',
                'Arn': f'arn:aws:lambda:{region}:{id}:function:{function_name}',
            }
        ]
    )

    print(f"Regla de EventBridge creada con éxito: {response['RuleArn']}")
    return response['RuleArn']

def invoke_lambda_function(function_name, region):
    lambda_client = boto3.client('lambda', region_name=region)
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='Event'  # InvocationType='Event' invoca la función de forma asincrónica
    )
    print(f"Función Lambda invocada con éxito: {response}")



if __name__ == "__main__":

    # cambiar con el nombre del bucket que servira de datalake
    bucketDataLake = 'fakeapi-raw-data-ingestion'
    # cambiar con el nombre del bucket que alamcena los reportes
    bucketReports = 'fakeapi-reports'
    # cambiar con el nombre de la funcion lambda
    lambdaFunctionName = "fakeAPIIngestionLambda"
    # cambiar con el rol IAM creado para la lambda
    role = "arn:aws:iam::288185525184:role/dataIngestFakeAPI"
    # Cambiar nombre de la regla de EventBridge si se necesita
    eventBridgeRuleName = "monthlyIngestionRule"
    # cambiar con el id de la cuenta de AWS
    account_id = "288185525184"
    # Cambiar la region si es necesario
    region = "us-east-1"


    create_s3_buckets(bucket1=bucketDataLake, bucket2=bucketReports)
    create_layer_package()
    layer_arn = create_lambda_layer(region=region)
    create_lambda_function(layer_arn, name=lambdaFunctionName, role_arn=role, region=region)
    rule_arn = create_eventbridge_rule(lambdaFunctionName, eventBridgeRuleName, id=account_id, region=region)
    invoke_lambda_function(lambdaFunctionName, region=region)
