import json
import logging
import boto3
from botocore.exceptions import ClientError


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_secret(secret_name, region="us-east-1"):
    # Create a Secrets Manager client
    secrets_manager = boto3.client("secretsmanager", region)

    try:
        # Retrieve the secret
        get_secret_value_response = secrets_manager.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # Handle potential errors
        raise e
    else:
        # If successful, return the secret
        if "SecretString" in get_secret_value_response:
            try:
                return json.loads(get_secret_value_response["SecretString"])
            except Exception as e:
                logging.error(e)
                get_secret_value_response["SecretString"]

        else:
            return get_secret_value_response["SecretBinary"]
