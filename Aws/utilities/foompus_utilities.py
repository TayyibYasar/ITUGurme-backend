import json
import decimal
from boto3.dynamodb.types import TypeDeserializer

serializer = TypeDeserializer()

ERROR_HELP_STRINGS = {
    # Operation specific errors
    'ConditionalCheckFailedException': 'Condition check specified in the operation failed, review and update the condition check before retrying',
    'TransactionConflictException': 'Operation was rejected because there is an ongoing transaction for the item, generally safe to retry with exponential back-off',
    'ItemCollectionSizeLimitExceededException': 'An item collection is too large, you\'re using Local Secondary Index and exceeded size limit of items per partition key.' +
                                                ' Consider using Global Secondary Index instead',
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



class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)
        
def validate(body, params):
    missingParams = []
    if body is {}:
        return False, {"status": False, "message": "Missing parameters"}
    for i in params:
        if i not in body:
            missingParams.append(i)
    
    if len(missingParams) > 0:
        return False,  {"status": False, "message": "Missing params: "+",".join(missingParams)}
    return True, {}

def response(code, content):
    return {
        'headers': {'Content-Type': 'application/json'},
        'statusCode': code,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST'
        },
        'body': json.dumps(content, cls = DecimalEncoder)
    }

def deserialize(data):
    if isinstance(data, list):
        return [deserialize(v) for v in data]
    if isinstance(data, dict):
        try:
            return serializer.deserialize(data)
        except TypeError:
            return {k: deserialize(v) for k,v in data.items()}
    else:
        return data

def get_current_meal(dynamodb, islunch, currentDate):
    resp = dynamodb.query(
        TableName = "itugurme",
        IndexName = "currentMeal",
        KeyConditionExpression = "#open = :openPk",
        ExpressionAttributeNames = {'#open': "open"},
        ExpressionAttributeValues = {":openPk": {"S":f"MEAL#{islunch}#{currentDate}"}}
    )
    return deserialize(resp['Items'])

def handle_error(error):
    error_code = error.response['Error']['Code']
    error_message = error.response['Error']['Message']

    error_help_string = ERROR_HELP_STRINGS[error_code]

    print('[{error_code}] {help_string}. Error message: {error_message}'
          .format(error_code=error_code,
                  help_string=error_help_string,
                  error_message=error_message))

