import boto3
import json
from urllib.parse import unquote
from foompus_utilities import *

dynamodb = boto3.client('dynamodb', region_name="eu-central-1")

def lambda_handler(event, context):
    body = event.get('body')
    if body is None:
        body = '{}'
    
    body = json.loads(body)
    validated, message = validate(body, ["foodName"])
    
    if not validated:
        return response(400, message)

    food_name = body['foodName']
    #food_name = unquote(food_name)
    username = event['requestContext']['authorizer']['user']


    resp = dynamodb.query(
        TableName = "itugurme",
        IndexName = "GSI1",
        KeyConditionExpression = "PK1 = :food",
        ExpressionAttributeValues = {":food": {"S":'FOOD#' + food_name}},
        ScanIndexForward = False
    )
    
    metadata = None
    history_data = []

    data = deserialize(resp['Items'])
    comments = []

    for foodData in data:
        if foodData['SK'].startswith('METADATA'):
            foodData.pop('PK')
            foodData.pop('SK')
            foodData.pop('PK1')
            metadata = foodData
        elif foodData['SK'].startswith('COMMENT'):
            comments.append(
                {
                    "commenter":foodData['SK'].split('#')[1],
                    "text":foodData['text_'],
                    "score":foodData['average'],
                    "commentId":foodData['SK'],
                    "isLiked":False,
                    "isDisliked":False
                }
            )
        else:
            foodData.pop('PK1')
            foodData['meal_date'] = foodData.pop('SK').split('#')[0]
            foodData['meal_type'] = foodData.pop('PK').split('#')[1]
            history_data.append(foodData)
    

    resp = dynamodb.query(
        TableName = "itugurme",
        IndexName = "currentMeal",
        KeyConditionExpression = "#open = :openPk",
        ExpressionAttributeNames = {'#open': "open"},
        ExpressionAttributeValues = {":openPk": {"S":"USER#" + username}}
    )
    current_user = deserialize(resp['Items'])
    for item in current_user:
        if item['SK'].startswith('CVOTE'):
            foodName = item['PK'].split('#')[1]
            if foodName == food_name:
                for comment in comments:
                    if comment['commenter'] == item['SK'].split('#')[2]:
                        if item['isLiked']:
                            comment['isLiked'] = True
                        else:
                            comment['isDisliked'] = True
                        break

    return response(200, comments)
    #return response(200, {'metadata':metadata, 'history_data':history_data})
    
    