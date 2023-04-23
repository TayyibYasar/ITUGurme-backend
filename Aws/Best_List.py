import boto3
import json
from foompus_utilities import *

dynamodb = boto3.client('dynamodb', region_name="eu-central-1")

def lambda_handler(event, context):
    
    if event['queryStringParameters'] is None:
        entity_type = 'USER'
    else:
        validated, message = validate(event['queryStringParameters'],["entity"])
        if not validated:
            return response(400, message)
        
        entity_type = event['queryStringParameters']['entity']

    
    user = event['requestContext']['authorizer']['user']

    resp = dynamodb.query(
        TableName = "itugurme",
        IndexName = "GSI2",
        KeyConditionExpression = "type_ = :type",
        ExpressionAttributeValues = {":type": {"S":entity_type}},
        ScanIndexForward = False,
        Limit = 100,
    )

    data = deserialize(resp['Items'])
    
    if entity_type == 'MEAL':
        remove_key_list = ["SK"]
    else:
        remove_key_list = ["PK1", "SK"]
    
    for item in data:
        for key in remove_key_list:
            item.pop(key)
        item['name'] = item.pop('PK').split('#')[1]
    
    if entity_type == 'USER':
        gurmes = []
        userGurmeScore = 0
        userRank = 0
        for i,gurme in enumerate(data):
            if i < 5:
                gurmes.append({
                    "username":gurme['name'],
                    "gurmeScore":gurme['average']
                })
            if gurme['name'] == user:
                userGurmeScore = gurme['average']
                userRank = i + 1
        
        if userRank == 0:
            userRank = len(data) + 1

        return response(200, {'gurmes':gurmes, 'usersGurmeScore':userGurmeScore, 'usersRank':userRank})

    return response(200, {"best_list":data})
