import boto3
import json
from datetime import datetime
from foompus_utilities import *

dynamodb = boto3.client('dynamodb', region_name="eu-central-1")

def lambda_handler(event, context):
    username = event['requestContext']['authorizer']['user']
    
    currentDateAndTime = datetime.now()
    currentTime = currentDateAndTime.strftime("%H:%M:%S")
    currentDate = currentDateAndTime.strftime("%Y-%m-%d")
    if(currentTime < '12'):
        islunch = 'L'
    else:
        islunch = 'D'
    

    current_meal = get_current_meal(dynamodb, islunch, currentDate)
    resp = dynamodb.query(
        TableName = "itugurme",
        IndexName = "currentMeal",
        KeyConditionExpression = "#open = :openPk",
        ExpressionAttributeNames = {'#open': "open"},
        ExpressionAttributeValues = {":openPk": {"S":"USER#" + username}}
    )
    current_user = deserialize(resp['Items'])
    

    foods = []
    meal = None
    mealVote = 0
    for item in current_meal:
        if item['SK'] != 'METADATA':
            foods.append({
                    'name': item['PK1'].split('#')[1],
                    'score': item['average'],
                    'voteCount': item['rate_count'],
                    'commentCount':item['comment_count'],
                    'userVote':0,
                    'userComment':""
                    }
            )
        else:
            meal = item
    for item in current_user:
        if item['SK'].startswith('COMMENT'):
            foodName = item['PK'].split('#')[1]
            for food in foods:
                if food['name'] == foodName:
                    food['userComment'] = item['text_']
        elif item['SK'].startswith('FVOTE'):
            foodName = item['PK'].split('#')[1]
            for food in foods:
                if food['name'] == foodName:
                    food['userVote'] = item['rate']
        elif item['SK'].startswith('MVOTE'):
            mealVote = 2 if item['isLiked'] else 1
    
    if meal is None:
        meal = {'like_count':0, 'dislike_count': 0}
    
    return response(200, {"foods":foods, "likeCount":meal['like_count'], "dislikeCount":meal['dislike_count'], 'mealVote':mealVote, 'username':username, 'isLunch': True if islunch == 'L' else False})
