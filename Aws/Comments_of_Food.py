import boto3
import json
from foompus_utilities import *
from datetime import datetime

dynamodb = boto3.client('dynamodb', region_name="eu-central-1")

def fetch_user(user):
    resp = dynamodb.query(
            TableName = "itugurme",
            IndexName = "currentMeal",
            KeyConditionExpression = "#open = :openPk",
            ExpressionAttributeNames = {'#open': "open"},
            ExpressionAttributeValues = {":openPk": {"S":"USER#" + user}}
        )
    return deserialize(resp['Items'])

def lambda_handler(event, context):
    body = event.get('body')
    if body is None:
        body = '{}'
    
    body = json.loads(body)
    validated, message = validate(body, ["foodName"])

    username = event['requestContext']['authorizer']['user']

    
    if not validated:
        return response(400, message)
    
    currentDateAndTime = datetime.now()
    currentTime = currentDateAndTime.strftime("%H:%M:%S")
    currentDate = currentDateAndTime.strftime("%Y-%m-%d")
    if(currentTime < '12'):
        islunch = 'L'
    else:
        islunch = 'D'
    
    food = body['foodName']
    food_pk =f'FOOD#{food}#{currentDate}' 
    
    resp = dynamodb.query(
        TableName = "itugurme",
        KeyConditionExpression = "PK = :food And begins_with(SK, :sk)",
        ExpressionAttributeValues = {":food": {"S":food_pk}, ":sk":{"S":"COMMENT"}},
        ScanIndexForward = False
    )
    
    data = sorted(deserialize(resp['Items']), key=lambda d: d['average'])
    
    comments = []

    for comment in data:
        comments.append({
            "commenter":comment['SK'].split('#')[1],
            "text":comment['text_'],
            "score":comment['average'],
            "commentId":comment['SK'],
            "isLiked":False,
            "isDisliked":False
        })
    
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
            if foodName == food:
                for comment in comments:
                    if comment['commenter'] == item['SK'].split('#')[2]:
                        if item['isLiked']:
                            comment['isLiked'] = True
                        else:
                            comment['isDisliked'] = True
                        break
                

    return response(200, comments)
    