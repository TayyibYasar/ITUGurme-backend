import boto3
import json
import decimal
from datetime import datetime
from foompus_utilities import *
from boto3.dynamodb.types import TypeSerializer

serializer__ = TypeSerializer()
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
    validated, message = validate(body, ["isLiked"])

    if not validated:
        return response(400, message)
    
    currentDateAndTime = datetime.now()
    currentTime = currentDateAndTime.strftime("%H:%M:%S")
    currentDate = currentDateAndTime.strftime("%Y-%m-%d")
    
    user = event['requestContext']['authorizer']['user']

    isLiked = body['isLiked']

    if(currentTime < '12'):
        islunch = 'L'
    else:
        islunch = 'D'

    #Gets current meal informations from open GSI.
    current_meal = get_current_meal(dynamodb, islunch, currentDate)
    
    #User activity on open meal.    
    current_user = fetch_user(user)
    
    transact_items = []

    #Find meal metadata from current meal informations.
    meal_data = None
    for data in current_meal:
        if 'PK1' not in data:
            meal_data = data

    if meal_data is None:
        return response(400, {"message":"Meal is not available..."})
         
    increment = 1

    for data in current_user:
        if data['openSK'].startswith('MVOTE#'):
            if data['isLiked'] != isLiked:
                if isLiked:
                    meal_data['like_count'] += 1
                    meal_data['dislike_count'] -= 1
                    meal_data['average'] += 2
                else:
                    meal_data['like_count'] -= 1
                    meal_data['dislike_count'] += 1
                    meal_data['average'] -= 2
            increment = 0
            break
    
    if increment == 1:
        if isLiked:
            meal_data['like_count'] += 1
            meal_data['average'] += 1
        else:
            meal_data['dislike_count'] += 1
            meal_data['average'] -= 1
        
    transact_items.append(
        {
        "Put": {
            "TableName":"itugurme",
            "Item" :{
                "PK": {"S":meal_data['PK']}, 
                "SK": {"S":f"MVOTE#{user}#{currentDate}"}, 
                "ts": {"S":"{}T{}".format(currentDate, currentTime)}, 
                'isLiked':{"BOOL":isLiked},
                "PK1": {"S":"USER#" + user},
                "open":{"S":f"USER#{user}"},
                "openSK":{"S":f"MVOTE#{user}#{currentDate}"}
                }
            }
        })
    transact_items.append(
        {
            "Put": {
                "TableName":"itugurme",
                "Item": {k: serializer__.serialize(v) for k, v in meal_data.items()}
            }
        }
    )
    try:
        resp = dynamodb.transact_write_items(
            TransactItems = transact_items
        )
    except Exception as e:
        return response(400, {'message': 'Transaction error...'})
    
    return response(resp['ResponseMetadata']['HTTPStatusCode'], {'message': 'Operation succesfull.'})
