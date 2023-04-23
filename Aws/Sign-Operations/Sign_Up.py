from passlib.hash import pbkdf2_sha256 as hasher
import boto3
import json
import decimal
from datetime import datetime
from foompus_utilities import *
import jwt

dynamodb = boto3.client('dynamodb', endpoint_url="http://localhost:8000")


def lambda_handler(event, context):
    event = json.loads(event)
    body = event.get('body')
    if body is None:
        body = '{}'
    
    #body = json.loads(body)
    validated, message = validate(body, ["username", "password"])
    if not validated:
        return response(400, message)
    
    currentDateAndTime = datetime.now()
    currentTime = currentDateAndTime.strftime("%H:%M:%S")
    currentDate = currentDateAndTime.strftime("%Y-%m-%d")
    username = body['username']
    password_ = hasher.hash(body['password'])

    Item_ = {
        'PK': {'S':username},
        'SK': {'S':f'{currentDate}#{currentTime}'},
        'password_':{'S': password_}
    }

    resp = dynamodb.query(
        TableName = 'itugurme-users',
        KeyConditionExpression = "PK = :user",
        ExpressionAttributeValues = {":user": {"S":username}}
    )

    if len(resp['Items']) > 0:
        print("User exists....")
        return response(403, {'message':'Username exists'})

    resp = dynamodb.put_item(
                        TableName = 'itugurme-users', 
                        Item = Item_,
                        ConditionExpression = "attribute_not_exists(PK)"
                        )
    print("resp: ", resp)
    if resp['ResponseMetadata']['HTTPStatusCode'] == 200:
        token = jwt.encode({'user' : username}, "", algorithm="")
        return response(200,{'Authorization':token})


