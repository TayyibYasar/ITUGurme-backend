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
    validated, message = validate(body, ["operation","commentId","commenter"])

    if not validated:
        return response(400, message)
    
    currentDateAndTime = datetime.now()
    currentTime = currentDateAndTime.strftime("%H:%M:%S")
    currentDate = currentDateAndTime.strftime("%Y-%m-%d")

    user = event['requestContext']['authorizer']['user']

    commentId = body['commentId']
    commenter = body['commenter']
    isLiked = body['operation'] == 'like'
    undo = body['operation'].startswith('un')
    delete_old_comment = False

    #User activity on open meal.    
    current_user = fetch_user(user)

    #comment metadata
    food_name = commentId.split('#')[2] + '#' + commentId.split('#')[3]
    food_pk = f'FOOD#{food_name}'

    comment_metadata = deserialize(dynamodb.query(
        TableName = "itugurme",
        KeyConditionExpression = "PK = :food And SK = :sk",
        ExpressionAttributeValues = {":food": {"S":food_pk}, ":sk":{"S":commentId}},
        ScanIndexForward = False
    )['Items'])[0]

    transact_items = []
         
    if not undo:
        increment = 1
        for data in current_user:
            if data['openSK'] == f'CVOTE#{user}#{commentId[8:]}':
                if data['isLiked'] != isLiked:
                    if isLiked:
                        comment_metadata['likeCount'] += 1
                        comment_metadata['dislikeCount'] -= 1
                        comment_metadata['average'] += 2
                    else:
                        comment_metadata['likeCount'] -= 1
                        comment_metadata['dislikeCount'] += 1
                        comment_metadata['average'] -= 2
                increment = 0
                break
        
        if increment == 1:
            if isLiked:
                comment_metadata['likeCount'] += 1
                comment_metadata['average'] += 1
            else:
                comment_metadata['dislikeCount'] += 1
                comment_metadata['average'] -= 1
    else:
        is_unLike = body['operation'] == 'unlike'
        for data in current_user:
            if data['openSK'] == f'CVOTE#{user}#{commentId[8:]}':
                if data['isLiked'] == is_unLike:
                    delete_old_comment = True
                    if data['isLiked']:
                        comment_metadata['likeCount'] -= 1
                        comment_metadata['average'] -= 1
                    else:
                        comment_metadata['dislikeCount'] -= 1
                        comment_metadata['average'] += 1
                increment = -1

    if not undo:
        transact_items.append(
            {
            "Put": {
                "TableName":"itugurme",
                "Item" :{
                    "PK": {"S":comment_metadata['PK']}, 
                    "SK": {"S":f"CVOTE#{user}#{commentId[8:]}"}, 
                    "ts": {"S":"{}T{}".format(currentDate, currentTime)}, 
                    'isLiked':{"BOOL":isLiked},
                    "PK1": {"S":"USER#" + user},
                    "open":{"S":f"USER#{user}"},
                    "openSK":{"S":f"CVOTE#{user}#{commentId[8:]}"}
                    }
                }
            })
    elif delete_old_comment:
        transact_items.append(
            {
            "Delete": {
                "TableName":"itugurme",
                "Key" :{
                    "PK": {"S":comment_metadata['PK']}, 
                    "SK": {"S":f"CVOTE#{user}#{commentId[8:]}"}, 
                    }
                }
            })
    
    transact_items.append(
        {
            "Put": {
                "TableName":"itugurme",
                "Item": {k: serializer__.serialize(v) for k, v in comment_metadata.items()}
            }
        }
    )
    try:
        resp = dynamodb.transact_write_items(
            TransactItems = transact_items
        )
    except Exception as e:
        print(e)
        return response(400, {'message': 'Transaction error...'})
    
    return response(resp['ResponseMetadata']['HTTPStatusCode'], {'message': 'Operation succesfull.'})
