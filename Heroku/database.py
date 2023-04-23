import psycopg2 as dbapi2
from passlib.hash import pbkdf2_sha256 as hasher

class Database:
    def __init__(self, host="", user="", 
                password="", dbname=""):
        self.host = host
        self.user = user
        self.password = password
        self.db = dbname

    def signup(self,username,password_):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            password_ = hasher.hash(password_)
            try:
                statement = """INSERT INTO users_(username,password_) VALUES(%s,%s)"""
                cursor.execute(statement,(username,password_))
                connection.commit()
                cursor.close()
            except dbapi2.IntegrityError:
                connection.rollback()
                return False
        return True

    def signin(self, username,password_):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            statement = """SELECT password_ FROM users_ WHERE username = %s"""
            cursor.execute(statement,(username,))
            fetch = cursor.fetchall()
            cursor.close()
        if len(fetch) > 0:
            if(hasher.verify(password_, fetch[0][0])):
                return True
        return False

    def get_all_meals(self):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            statement = """select meal_date from meals GROUP BY meal_date HAVING COUNT( meal_date )> 1 ORDER BY meal_date;"""
            cursor.execute(statement)
            fetch = cursor.fetchall()
            cursor.close()
        meals = []
        for element in fetch:
            meals.append(element[0].strftime("%d-%m-%y"))
        return meals

    def get_gurmes(self, username):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            statement = """SELECT username, gurme_score FROM users_ order by gurme_score desc limit 5"""
            cursor.execute(statement)
            fetch = cursor.fetchall()
            statement = """select * 
                            from (
                                select username,gurme_score, row_number() over(order by gurme_score desc) as position 
                                from users_
                            ) result 
                            where username = %s;"""
            cursor.execute(statement, (username,))
            current_user = cursor.fetchall()
            cursor.close()
        gurmes = []
        for gurme in fetch:
            gurmes.append({"username":gurme[0], "gurmeScore":gurme[1]})
        return gurmes,current_user[0][1],current_user[0][2]

    def get_meal(self, date):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            statement = """select * from meals where meal_date = %s;"""
            cursor.execute(statement, (date,))
            fetch = cursor.fetchall()
            cursor.close()
        lunch = []
        dinner = []
        for element in fetch:
            if(element[2]):
                lunch.append(element[1])
            else:
                dinner.append(element[1])
        return {"lunch":lunch, "dinner":dinner}

    def delete_past_meals(self, date):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            try:
                statement = """DELETE FROM meals where meal_date < %s"""
                cursor.execute(statement, (date,))
                connection.commit()
                cursor.close()
            except dbapi2.IntegrityError:
                connection.rollback()
                return False
        return True

    def insert_food(self, food_names, islunch, date):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            connection.set_client_encoding('UTF8')
            cursor = connection.cursor()
            for food_name in food_names:
                try:
                    statement = """INSERT INTO foods_(food_name) VALUES(%s)"""
                    cursor.execute(statement, (food_name,))
                    connection.commit()
                except dbapi2.IntegrityError:
                    connection.rollback()
                try:
                    statement = """INSERT INTO meals(food_name, islunch, meal_date) VALUES(%s, %s, %s)"""
                    cursor.execute(statement, (food_name, islunch, date))
                    connection.commit()
                except dbapi2.IntegrityError:
                    connection.rollback()
                    return False
            cursor.close()
            return True
 
    def vote(self, username, vote, food_name):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            statement = """SELECT vote_id FROM votes_ WHERE food_name = %s and username = %s and vote_date = CURRENT_DATE"""
            cursor.execute(statement,(food_name, username))
            fetch = cursor.fetchall()
            if(len(fetch) > 0):
                try:
                    statement = """UPDATE votes_ SET vote = %s WHERE vote_id = %s;"""
                    cursor.execute(statement, (vote, fetch[0][0]))
                    connection.commit()
                    cursor.close()
                except dbapi2.IntegrityError:
                    connection.rollback()
                    return False
            else:
                try:
                    statement = """INSERT INTO votes_(username, vote, food_name) VALUES(%s, %s, %s)"""
                    cursor.execute(statement, (username, vote, food_name))
                    connection.commit()
                    cursor.close()
                except dbapi2.IntegrityError:
                    connection.rollback()
                    return False
                self.update_gurmescore(username, 1, connection)
            return True
    
    def meal_vote(self,username, meal_vote, islunch):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            statement = """SELECT meal_vote_id FROM meal_votes WHERE username = %s and vote_date = CURRENT_DATE and islunch = %s"""
            cursor.execute(statement,(username,islunch))
            fetch = cursor.fetchall()
            if(len(fetch) > 0):
                try:
                    statement = """UPDATE meal_votes SET meal_vote = %s WHERE meal_vote_id = %s;"""
                    cursor.execute(statement, (meal_vote, fetch[0][0]))
                    connection.commit()
                    cursor.close()
                except dbapi2.IntegrityError:
                    connection.rollback()
                    return False
            else:
                try:
                    statement = """INSERT INTO meal_votes(username, meal_vote, islunch) VALUES(%s, %s, %s)"""
                    cursor.execute(statement, (username, meal_vote, islunch))
                    connection.commit()
                    cursor.close()
                except dbapi2.IntegrityError:
                    connection.rollback()
                    return False
                self.update_gurmescore(username, 1, connection)
            return True

    def comment(self, username, text_, food_name):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            statement = """SELECT comment_id FROM comments_ WHERE food_name = %s and username = %s and comment_date = CURRENT_DATE"""
            cursor.execute(statement,(food_name, username))
            fetch = cursor.fetchall()
            if(len(fetch) > 0):
                try:
                    statement = """UPDATE comments_ SET text_ = %s WHERE comment_id = %s;"""
                    cursor.execute(statement, (text_, fetch[0][0],))
                    connection.commit()
                    cursor.close()
                except dbapi2.IntegrityError:
                    connection.rollback()
                    return False
            else:
                try:
                    statement = """INSERT INTO comments_(username, text_, food_name) VALUES(%s, %s, %s)"""
                    cursor.execute(statement, (username, text_, food_name))
                    connection.commit()
                    cursor.close()
                except dbapi2.IntegrityError:
                    connection.rollback()
                    return False
                self.update_gurmescore(username,1,connection)
            return True
    
    def get_gurme_score(self, username, connection):
        cursor = connection.cursor()
        statement = """SELECT gurme_score FROM users_ WHERE username = %s"""
        cursor.execute(statement,(username,))
        fetch = cursor.fetchall()[0][0]
        cursor.close()
        return fetch
    
    def get_food_comments(self, food_name ,isAll, username):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            if(not isAll):
                statement = """select * from comments_ where food_name = %s and comment_date = CURRENT_DATE order by comment_score desc limit 10"""
            else:
                statement = """select * from comments_ where food_name = %s and comment_date = CURRENT_DATE order by comment_score desc"""
            cursor.execute(statement,(food_name,))
            fetch = cursor.fetchall()
            cursor.close()
            comments = []
            i = 0
            n = cursor.rowcount
            while(i < n):
                gurme_point = self.get_gurme_score(fetch[i][3], connection)
                liked, disliked = self.get_comment_isLiked(username, fetch[i][0] ,connection)
                score = fetch[i][5]
                if(score is None):
                    score = 0
                comments.append({"commenter":fetch[i][3], "gurmeScore":gurme_point, "text":fetch[i][1],"score":score, "commentId":fetch[i][0], "isLiked":liked, "isDisliked":disliked})
                i+=1
            return comments
    
    def get_comment_isLiked(self, username, comment_id, connection):
        cursor = connection.cursor()
        statement = """SELECT isliked FROM comments_votes_ WHERE username = %s and comment_id = %s"""
        cursor.execute(statement,(username,comment_id))
        fetch = cursor.fetchall()
        cursor.close()
        isliked = False
        disliked = False
        if(len(fetch) > 0):
            if(fetch[0][0] is True):
                isliked = True
            else:
                disliked = True
        return isliked, disliked
    
    def get_homepage_infos(self,username,islunch):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            statement = """SELECT food_name FROM meals where meal_date = CURRENT_DATE and islunch = %s"""
            cursor.execute(statement, (islunch,))
            fetch = cursor.fetchall()
            i = 0
            foods = []
            while(i < len(fetch)):
                statement = """SELECT AVG(vote) FROM foods_ INNER JOIN votes_ ON foods_.food_name = votes_.food_name WHERE foods_.food_name = %s and vote_date = CURRENT_DATE;"""
                cursor.execute(statement, (fetch[i][0],))
                average_rate = cursor.fetchall()[0][0]
                if(average_rate is None):
                    average_rate = 0
                average_rate = round(average_rate, 1)
                statement = """SELECT COUNT(*) FROM foods_ INNER JOIN comments_ ON foods_.food_name = comments_.food_name WHERE foods_.food_name = %s and comments_.comment_date = CURRENT_DATE;"""
                cursor.execute(statement, (fetch[i][0],))
                comment_count = cursor.fetchall()[0][0]
                statement = """SELECT COUNT(*) FROM votes_ WHERE food_name = %s and vote_date = CURRENT_DATE"""
                cursor.execute(statement, (fetch[i][0],))
                vote_count = cursor.fetchall()[0][0]
                statement = """SELECT vote FROM votes_ WHERE username = %s and food_name = %s and vote_date = CURRENT_DATE"""
                cursor.execute(statement,(username,fetch[i][0]))
                userVote = cursor.fetchall()
                statement = """SELECT text_ FROM comments_ WHERE username = %s and food_name = %s and comment_date = CURRENT_DATE"""
                cursor.execute(statement,(username,fetch[i][0]))
                userComment = cursor.fetchall()
                if(len(userVote) > 0):
                    userVote = userVote[0][0]
                else:
                    userVote = 0
                if(len(userComment) > 0):
                    userComment = userComment[0][0]
                else:
                    userComment = ""
                foods.append({"name":fetch[i][0], "score":float(average_rate), "commentCount":comment_count, "voteCount":vote_count, "userVote":userVote, "userComment":userComment})
                i+=1
            statement = """SELECT COUNT(*) FROM MEAL_votes WHERE vote_date = CURRENT_DATE and islunch = %s and meal_vote = True"""
            cursor.execute(statement, (islunch,))
            like_count = cursor.fetchall()[0][0]
            statement = """SELECT COUNT(*) FROM MEAL_votes WHERE vote_date = CURRENT_DATE and islunch = %s and meal_vote = False"""
            cursor.execute(statement, (islunch,))
            dislike_count = cursor.fetchall()[0][0]
            statement = """SELECT meal_vote FROM meal_votes WHERE username = %s and vote_date = CURRENT_DATE and islunch = %s"""
            cursor.execute(statement,(username,islunch))
            meal_vote = cursor.fetchall()
            if(len(meal_vote) > 0):
                if(meal_vote[0][0]):
                    meal_vote = 2
                else:
                    meal_vote = 1
            else:
                meal_vote = 0
            return foods, like_count, dislike_count, meal_vote

    def update_comment_score(self,comment_id, operation, connection):
        cursor = connection.cursor()
        try:
            statement = """UPDATE comments_ SET comment_score = comment_score + %s WHERE comment_id = %s"""
            cursor.execute(statement, (operation,comment_id,))
            connection.commit()
            cursor.close()
            return True
        except dbapi2.IntegrityError:
            connection.rollback()
            return False
    
    def update_gurmescore(self,username,operation, connection):
        cursor = connection.cursor()
        try:
            statement = """UPDATE users_ SET gurme_score = gurme_score + %s WHERE username = %s"""
            cursor.execute(statement, (operation, username,))
            connection.commit()
            cursor.close()
            return True
        except dbapi2.IntegrityError:
            connection.rollback()
            return False
    
    def comment_vote(self, comment_id, user, operation, commenter, connection = None):
        with dbapi2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db) as connection:
            cursor = connection.cursor()
            if(operation == "unlike" or operation == "undislike"):
                try:
                    statement = """DELETE FROM comments_votes_ WHERE comment_id = %s and username = %s;"""
                    cursor.execute(statement, (comment_id, user))
                    connection.commit()
                    cursor.close()
                    if(operation == "unlike"):
                        operation = -1
                    else:
                        operation = +1
                    self.update_comment_score(comment_id, operation, connection)
                    self.update_gurmescore(commenter,operation,connection)
                    return "Oy silindi"
                except dbapi2.IntegrityError:
                    connection.rollback()
                    return False
            if(operation == "dislike"):
                operation = False
            elif(operation == "like"):
                operation = True
            statement = """SELECT comment_vote_id FROM comments_votes_ WHERE comment_id = %s and username = %s"""
            cursor.execute(statement,(comment_id, user))
            fetch = cursor.fetchall()
            if(len(fetch) > 0):
                try:
                    statement = """UPDATE comments_votes_ SET isliked = %s WHERE comment_vote_id = %s;"""
                    cursor.execute(statement, (operation, fetch[0][0]))
                    connection.commit()
                    cursor.close()
                    if(operation):
                        operation = 2
                    else:
                        operation = -2
                    self.update_comment_score(comment_id, operation, connection)
                    self.update_gurmescore(commenter,operation,connection)
                    return "Oy güncellendi"
                except dbapi2.IntegrityError:
                    connection.rollback()
                    return False
            else:
                try:
                    statement = """INSERT INTO comments_votes_(comment_id ,username, isliked) VALUES(%s, %s, %s)"""
                    cursor.execute(statement, (comment_id, user, operation,))
                    connection.commit()
                    cursor.close()
                    if(operation):
                        operation = 1
                    else:
                        operation = -1
                    self.update_comment_score(comment_id, operation, connection)
                    self.update_gurmescore(commenter,operation,connection)
                    return "Oy verildi"
                except dbapi2.IntegrityError:
                    connection.rollback()
                    return False