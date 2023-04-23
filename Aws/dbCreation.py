# Before running the code below, please follow these steps to setup your workspace if you have not
# set it up already:
#
# 1. Setup credentials for DynamoDB access. One of the ways to setup credentials is to add them to
#    ~/.aws/credentials file (C:\Users\USER_NAME\.aws\credentials file for Windows users) in
#    following format:
#
#    [<profile_name>]
#    aws_access_key_id = YOUR_ACCESS_KEY_ID
#    aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
#
#    If <profile_name> is specified as "default" then AWS SDKs and CLI will be able to read the credentials
#    without any additional configuration. But if a different profile name is used then it needs to be
#    specified while initializing DynamoDB client via AWS SDKs or while configuring AWS CLI.
#    Please refer following guide for more details on credential configuration:
#    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration
#
# 2. Install the latest Boto 3 release via pip:
#
#    pip install boto3
#
#    Please refer following guide for more details on Boto 3 installation:
#    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#installation
#    Please note that you may need to follow additional setup steps for using Boto 3 from an IDE. Refer
#    your IDE's documentation if you run into issues.


# Load the AWS SDK for Python
import boto3
from botocore.exceptions import ClientError

ERROR_HELP_STRINGS = {
    # Operation specific errors
    'LimitExceededException': 'Number of simultaneous table operations may exceed the limit. Up to 50 simultaneous table operations are allowed per account.' +
                              'You can have up to 25 such requests running at a ; however, if the table or index specifications are complex,' +
                              'DynamoDB might temporarily reduce the number of concurrent operations. Consider retry it later',
    'ResourceInUseException': 'Table already exists, verify table does not exist before retrying',
    # Common Errors
    'InternalServerError': 'Internal Server Error, generally safe to retry with exponential back-off',
    'ProvisionedThroughputExceededException': 'Request rate is too high. If you\'re using a custom retry strategy make sure to retry with exponential back-off.' +
                                              'Otherwise consider reducing frequency of requests or increasing provisioned capacity for your table or secondary index',
    'ResourceNotFoundException': 'One of the tables was not found, verify table exists before retrying',
    'ServiceUnavailable': 'Had trouble reaching DynamoDB. generally safe to retry with exponential back-off',
    'ThrottlingException': 'Request denied due to throttling, generally safe to retry with exponential back-off',
    'UnrecognizedClientException': 'The request signature is incorrect most likely due to an invalid AWS access key ID or secret key, fix before retrying',
    'ValidationException': 'The input fails to satisfy the constraints specified by DynamoDB, fix input before retrying',
    'RequestLimitExceeded': 'Throughput exceeds the current throughput limit for your account, increase account level throughput before retrying',
}

# Use the following function instead when using DynamoDB Local
def create_dynamodb_client(region="eu-central-1"):
    return boto3.client("dynamodb", endpoint_url="http://localhost:8000")

#def create_dynamodb_client(region="eu-central-1"):
#    return boto3.client("dynamodb", region_name="eu-central-1")


def create_table_input():
    return {
    "TableName": "itugurme",
    "KeySchema": [
        {
            "AttributeName": "PK",
            "KeyType": "HASH"
        },
        {
            "AttributeName": "SK",
            "KeyType": "RANGE"
        }
    ],
    "BillingMode": "PAY_PER_REQUEST",
    "AttributeDefinitions": [
        {
            "AttributeName": "PK",
            "AttributeType": "S"
        },
        {
            "AttributeName": "SK",
            "AttributeType": "S"
        },
        {
            "AttributeName": "PK1",
            "AttributeType": "S"
        },
        {
            "AttributeName": "type_",
            "AttributeType": "S"
        },
        {
            "AttributeName": "open",
            "AttributeType": "S"
        },
        {
            "AttributeName": "openSK",
            "AttributeType": "S"
        },
        {
            "AttributeName": "average",
            "AttributeType": "N"
        }
    ],
    "GlobalSecondaryIndexes": [
        {
            "IndexName": "GSI1",
            "KeySchema": [
                {
                    "AttributeName": "PK1",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "SK",
                    "KeyType": "RANGE"
                }
            ],
            "Projection": {
                "ProjectionType": "ALL"
            }
        },
        {
            "IndexName": "GSI2",
            "KeySchema": [
                {
                    "AttributeName": "type_",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "average",   #eskisi PK idi...
                    "KeyType": "RANGE"
                }
            ],
            "Projection": {
                "ProjectionType": "ALL"
            }
        },
        {
            "IndexName": "currentMeal",
            "KeySchema": [
                {
                    "AttributeName": "open",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "openSK",
                    "KeyType": "RANGE"
                }
            ],
            "Projection": {
                "ProjectionType": "ALL"
            }
        }
    ]
}


def execute_create_table(dynamodb_client, input):
    try:
        response = dynamodb_client.create_table(**input)
        print("Successfully created table.")
        # Handle response
    except ClientError as error:
        handle_error(error)
    except BaseException as error:
        print("Unknown error while creating table: " + error.response['Error']['Message'])


def handle_error(error):
    error_code = error.response['Error']['Code']
    error_message = error.response['Error']['Message']

    error_help_string = ERROR_HELP_STRINGS[error_code]

    print('[{error_code}] {help_string}. Error message: {error_message}'
          .format(error_code=error_code,
                  help_string=error_help_string,
                  error_message=error_message))


def main():
    # Create the DynamoDB Client with the region you want
    dynamodb_client = create_dynamodb_client()

    # Create the dictionary containing arguments for create_table call
    create_table_request = create_table_input()

    # Call DynamoDB's create_table API
    execute_create_table(dynamodb_client, create_table_request)

  
if __name__ == "__main__":
    main()
