import boto3
import uuid
import os

def lambda_handler(event, context):
    # Entrada (json)
    try:
        log_info = {
            "tipo": "INFO",
            "log_datos": {
                "event": event
            }
        }
        print(json.dumps(log_info)) # Log json en CloudWatch
    
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]
        
        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        
        log_info = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Pelicula creada correctamente",
                "pelicula": pelicula,
                "response": response
            }
        }

        # Salida (json)
        print(json.dumps(log_info)) # Log json en CloudWatch
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }
    except Exception as e:
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": str(e),
                "event": event
            }
        }
        print(json.dumps(log_error)) # Log json en CloudWatch

        return {
            'statusCode': 500,
            'mensaje_error': str(e)
        }
