import boto3
from datetime import datetime
from foompus_utilities import *
from botocore.exceptions import ClientError

#TODO CloudWatch ı takip et hata alıp almadığımıza

dynamodb = boto3.client('dynamodb', region_name="eu-central-1")

def get_user(userPK, desrlze = True):
    resp = dynamodb.get_item(
        TableName = "itugurme",
        Key = {
            "PK": {"S":userPK},
            "SK": {"S":'METADATA'}
        }
    )
    if 'Item' not in resp:
        return None

    return deserialize(resp['Item']) if desrlze else None

def create_put_item_input(userPK, others, updateAt):
    
    return {
        "TableName": "itugurme",
        "Item": {
            "PK": {"S":userPK}, 
            "SK": {"S":"METADATA"}, 
            "ts": {"S":others['ts']}, 
            "type_": {"S":"USER"}, 
            "PK1": {"S":userPK}, 
            "f_rate_count": {"N":str(others['f_rate_count'])}, 
            "m_like_count": {"N":str(others['m_like_count'])}, 
            "f_rate_average": {"N":str(others['f_rate_average'])},
            "m_dislike_count": {"N":str(others['m_dislike_count'])},   
            "comment_count": {"N":str(others['comment_count'])},
            "lastOperationTime": {"S":updateAt},
            "average":{"N":str(others['average'])}  #Gurme-score for user
        }
    }

def lambda_handler(event, context):
    
    if event['caller'] != "EventBridge":
        return False
        
    currentDateAndTime = datetime.now()
    currentTime = currentDateAndTime.strftime("%Y-%m-%d")
    currentTime = currentDateAndTime.strftime("%H:%M:%S")
    
    if not (((currentTime > '6') and (currentTime < '8')) or ((currentTime > '12') and (currentTime < '14'))):
        return "Not Fetch Time"
    
    resp = dynamodb.scan(
            TableName = "itugurme",
            IndexName = "currentMeal"
            )
    data = resp['Items']
    while 'LastEvaluatedKey' in resp:
        resp = dynamodb.scan(
            TableName = "itugurme",
            IndexName = "currentMeal",
            ExclusiveStartKey = resp['LastEvaluatedKey']
        )
        data.extend(resp['Items'])
    
    data = deserialize(data)
    
    saved_users = {}
    saved_foods = {}

    for item in data:
        if not item['open'].startswith('MEAL'):
            userPK = item['open'] #USER#xxxx

            if item['openSK'].startswith('COMMENT'):
                if userPK in saved_users:
                    userPK = saved_users[userPK]
                    userPK['comment_count'] += 1
                    userPK['average'] += 3  + item['average'] #Comment on meal increases 3 point of user gurme_score + like or dislike count of comment
                    
                else:
                    user_metadata = get_user(userPK)
                    if user_metadata is None:    #User's first interaction with application
                        saved_users[userPK] = {'f_rate_count':0, 'f_rate_average':0, 'm_like_count':0, 'm_dislike_count': 0, 'comment_count':1, 'ts': currentTime, 'average':3 + item['average']}
                    else:
                        user_metadata['comment_count'] += 1
                        user_metadata['average'] += 3 + item['average']
                        saved_users[userPK] = user_metadata
            
            elif item['openSK'].startswith('FVOTE'):
                if userPK in saved_users:
                    userPK = saved_users[userPK]
                    userPK['average'] += 1 #Food vote increases 1 point of user gurme_score
                    userPK['f_rate_average'] = userPK['f_rate_count'] * userPK['f_rate_average'] + decimal.Decimal(str(item['rate']))
                    userPK['f_rate_count'] += 1
                    userPK['f_rate_average'] /= userPK['f_rate_count']
                    
                else:
                    user_metadata = get_user(userPK)
                    if user_metadata is None:
                        saved_users[userPK] = {'f_rate_count':1, 'f_rate_average':decimal.Decimal(str(item['rate'])), 'm_like_count':0, 'm_dislike_count': 0, 'comment_count':0, 'ts': currentTime, 'average': 1}
                    else:
                        saved_users[userPK] = user_metadata
                        userPK = saved_users[userPK]
                        userPK['average'] += 1
                        userPK['f_rate_average'] = userPK['f_rate_count'] * userPK['f_rate_average'] + decimal.Decimal(str(item['rate']))
                        userPK['f_rate_count'] += 1
                        userPK['f_rate_average'] /= userPK['f_rate_count']
            
            elif item['openSK'].startswith('MVOTE'):

                if userPK in saved_users:
                    userPK = saved_users[userPK]
                    userPK['average'] += 2  #Meal Vote increases 2 of user gurme score
                    if item['isLiked']:
                        userPK['m_like_count'] += 1
                    else:
                        userPK['m_dislike_count'] += 1
                    
                else:
                    user_metadata = get_user(userPK)
                    if user_metadata is None:
                        saved_users[userPK] = {'f_rate_count':0, 'f_rate_average':0, 'm_like_count':0, 'm_dislike_count':0 , 'comment_count':0, 'ts': currentTime, 'average':2}
                    else:
                        saved_users[userPK] = user_metadata
                        userPK = saved_users[userPK]
                        userPK['average'] += 2
                        if item['isLiked']:
                            userPK['m_like_count'] += 1
                        else:
                            userPK['m_dislike_count'] += 1
            
            elif item['openSK'].startswith('CVOTE'):
                if userPK in saved_users:
                    userPK = saved_users[userPK]
                    userPK['average'] += 1  #Comment Vote increases 2 of user gurme score
                    
                else:
                    user_metadata = get_user(userPK)
                    if user_metadata is None:
                        saved_users[userPK] = {'f_rate_count':0, 'f_rate_average':0, 'm_like_count':0, 'm_dislike_count':0 , 'comment_count':0, 'ts': currentTime, 'average':1}
                    else:
                        saved_users[userPK] = user_metadata
                        userPK = saved_users[userPK]
                        userPK['average'] += 1
        
        elif not item['openSK'].startswith('METADATA'):   #Food metadata update
            foodPK = item['PK1']

            food_metadata = get_user(foodPK, False) #It is true, dont worry..., False is not to deserialize
            
            if food_metadata is None:
                saved_foods[foodPK] = {
                    "TableName": "itugurme",
                    "Item": {
                        "PK": {"S":foodPK}, 
                        "SK": {"S":"METADATA"}, 
                        "ts": {"S":currentTime}, 
                        "type_": {"S":"FOOD"}, 
                        "PK1": {"S":foodPK},  
                        "average": {"N":str(item['average'])}, 
                        "rate_count": {"N":str(item['rate_count'])},
                        "comment_count":{"N":str(item['comment_count'])}
                    }
                }
                
            else:
                food_metadata['average']['N'] = (food_metadata['average']['N'] * food_metadata['rate_count']['N']) + (item['average'] * item['rate_count'])
                food_metadata['rate_count']['N'] += item['rate_count']
                food_metadata['average']['N'] /= food_metadata['rate_count']['N']
                food_metadata['comment_count']['N'] += item['comment_count']
                
                saved_foods[foodPK] = {
                    "TableName":"itugurme",
                    "Item":food_metadata
                }

        #Now everything done for item, so we can delete it from current meal.
        try:
            resp = dynamodb.update_item(
                TableName = "itugurme",
                Key = {
                    "PK":{'S':item['PK']},
                    "SK":{'S':item['SK']}
                },
                UpdateExpression = "REMOVE #open, openSK",
                ConditionExpression = "attribute_exists(#open)",
                ExpressionAttributeNames = {"#open":"open"}
            )
        except Exception as e:
            print(e)    
        
    
    currentDateAndTime = datetime.now()
    currents = currentDateAndTime.strftime("%Y-%m-%dT%H:%M:%S")
    for userPK in saved_users:
        put_item_input = create_put_item_input(userPK,saved_users[userPK], currents)
        try:
            resp = dynamodb.put_item(**put_item_input)
        except ClientError as error:
            handle_error(error)
        except Exception as e:
            print(e)

    for foodPK in saved_foods:
        try:
            resp = dynamodb.put_item(**saved_foods[foodPK])
        except ClientError as error:
            print(e)
            handle_error(error)
        except Exception as e:
            print("Unknown error....")
    
    return response(200, {"message": "Success or unsuccess, unknown.."})


