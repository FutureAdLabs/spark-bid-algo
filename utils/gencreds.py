'''
Generate credential from aws-secret manager
secrets will be stored in ./.env/**
'''
#
import os,sys
import json
import base64
import boto3
from botocore.exceptions import ClientError
#
sys.path.append(os.path.join(os.path.dirname(__file__)))
#
# from constants import 
CREDENTIALS_FOLDER_NAME = ".env"

def get_secret(secret_name: str) -> dict:
    '''
    DOCS
    '''
    if  not secret_name:
        return None

    # return config data if exists
    data = None
    region_name = "eu-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as error:
        if error.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise error
        elif error.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise error
        elif error.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise error
        elif error.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that
            # is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise error
        elif error.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            print("You might have entered Invalid Secret Name")
            #raise error
            return None
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string
        # or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            data = get_secret_value_response['SecretString']
        else:
            data = base64.b64decode(get_secret_value_response['SecretBinary'])

    if data:
        data = json.loads(data)

    # These two line of code is used only for database data
    if 'username' in data.keys() and not 'user' in data.keys():
        data['user'] = data['username']

    return data


def save_secret(data: dict, saving_file_path: str) -> None:
    '''
    Save seret file generate from secret manager to the file path.
    saving_file_path could look like -> .credentials/secretthing.json
    '''
    if not isinstance(data, dict):
        return None

    if not saving_file_path :
        return None

    print(f"** saving credentials to => {saving_file_path} **")
    directory_name = os.path.dirname(saving_file_path)
    os.makedirs(directory_name, exist_ok=True)

    with open(saving_file_path, 'w', encoding="utf-8") as opened_file:
        json.dump(data, opened_file)

    print("***\tCredential Saved\t***")


def get_and_save_secret(secret_name: str, saving_file_name: str) -> dict:
    '''
    Generate secret key and save file to default(CREDENTIAL_FOLDER_NAME) path.
    '''
    if not secret_name:
        return None
    if not saving_file_name:
        return None

    # setup default credential path
    file_path = f"{CREDENTIALS_FOLDER_NAME}/{saving_file_name}.json"

    if os.path.exists(file_path):
        print("***\tCredentials Already Exist\t***")
        with open(file_path, 'r', encoding="utf-8") as opened_file:
            secret = json.load(opened_file)
        return secret

    secret = get_secret(secret_name)
    save_secret(secret, file_path)

    return secret

if __name__ == "__main__":
    print(get_and_save_secret("google/service/sheet", "gslide"))