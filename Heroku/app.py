from flask import Flask, request, json, jsonify
from flask_cors import CORS
from scrape import Scrape
import database
import jwt
from functools import wraps
import datetime

from settings import SECRET_KEY, HASH_ALGORITHM


app = Flask(__name__)
CORS(app)
app.config.from_object("settings")
db = database.Database()
port = app.config.get("PORT",5000)
app.config["db"] = db

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        try:
            data = jwt.decode(token, SECRET_KEY,algorithms=HASH_ALGORITHM)
        except:
            return jsonify({'message':'Token is invalid or missing'}), 403
        return f(*args, *kwargs, data["user"])
    return decorated

@app.route('/', methods = ["GET", "POST"])
def home():
    return '<h1>İtüGurme Official Server</h1>'


@app.route('/get-all-meals', methods =["GET"])
def get_all_meals():
    meals = db.get_all_meals()
    return jsonify(meals)

@app.route('/get-meal', methods =["POST"])
def get_meal():
    date = request.get_json().get('date')
    date = date.split("-")
    date = date[1] + "-" + date[0] + "-" + date[2]
    meal = db.get_meal(date)
    return meal

@app.route('/retrieve-meals', methods =["POST"])
def retrieve_meals():
    dates = request.get_json().get('dates')
    success = Scrape(dates)
    return jsonify({"message": success})

@app.route('/get-gurmes', methods = ["GET"])
@token_required
def get_gurmes(username):
    gurmes, userGurmeScore, userRank = db.get_gurmes(username)
    return jsonify({"gurmes":gurmes, "usersGurmeScore":userGurmeScore, "usersRank":userRank})

@app.route('/main_page', methods =["GET"])
@token_required
def main_page(username):
    print(username)
    islunch = True
    current_hour = datetime.datetime.now().hour
    if(current_hour + 3 >= 16):
        islunch = False
    foods,like_count, dislike_count, mealVote = db.get_homepage_infos(username, islunch)
    return jsonify({"foods": foods, "likeCount":like_count, "dislikeCount":dislike_count, "mealVote":mealVote, "username":username, "isLunch":islunch})

@app.route('/meal-vote', methods =["POST"])
@token_required
def meal_vote(username):
    addData_json = request.get_json()
    meal_vote = addData_json.get('isLiked')
    islunch = addData_json.get('isLunch')
    success = db.meal_vote(username, meal_vote, islunch)
    return jsonify(success)

@app.route('/vote', methods =["POST"])
@token_required
def vote(username):
    addData_json = request.get_json()
    score = addData_json.get('score')
    food_name = addData_json.get('foodName')
    success = db.vote(username, score, food_name)
    return jsonify(success)

@app.route('/comment', methods =["POST"])
@token_required
def comment(username):
    addData_json = request.get_json()
    text_ = addData_json.get('text')
    food_name = addData_json.get('foodName')
    success = db.comment(username, text_, food_name)
    return jsonify(success)

@app.route('/get-food-comments', methods =["POST"])
@token_required
def get_food_comments(username):
    food_name = request.get_json().get('foodName')
    return json.dumps(db.get_food_comments(food_name, False, username), indent = 4, ensure_ascii=False, separators=(',', ': '))

@app.route('/get-food-info', methods =["POST"])
@token_required
def get_food_info(username):
    food_name = request.get_json().get('foodName')
    return jsonify(db.get_food_comments(food_name, True, username))

@app.route('/signup', methods =["POST"])
def signup():
    addData_json = request.get_json()
    username = addData_json.get('username').lower()
    password_ = addData_json.get('password')
    success = db.signup(username, password_)
    if(success):
        token = jwt.encode({'user' : username}, SECRET_KEY, algorithm=HASH_ALGORITHM)
        return jsonify({'Authorization' : token})
    return "Username exists", 403

@app.route('/signin', methods =["POST"])
def signin():
    addData_json = request.get_json()
    username = addData_json.get('username').lower()
    password_ = addData_json.get('password')
    success = db.signin(username, password_)
    if(success):
        token = jwt.encode({'user' : username}, SECRET_KEY, algorithm=HASH_ALGORITHM)
        return jsonify({'Authorization' : token})
    return "Incorrect username or password", 403

@app.route('/comment-vote', methods =["POST"])
@token_required
def comment_vote(username):
    data_json = request.get_json()
    comment_id = data_json.get('commentId')
    operation = data_json.get('operation')
    commenter = data_json.get('commenter')
    return jsonify(db.comment_vote(comment_id, username, operation, commenter))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=port)

