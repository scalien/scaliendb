import os, sys, time
from datetime import datetime
import scaliendb
from scaliendb import Util
#from pickle import loads, dumps
from simplejson import loads, dumps

client = scaliendb.Client(["localhost:7080"])
# scaliendb.set_trace(True)

def setup_schema():
	if not client.exists_database("twitter"):
		client.create_database("twitter")
	db = client.get_database("twitter")
	db.create_table_cond("indices")
	db.create_table_cond("users")
	db.create_table_cond("tweets")
	db.create_table_cond("followers")
	db.create_table_cond("following")
	db.create_table_cond("index_users_username")
	db.create_table_cond("index_tweets_creator_datetime")
	db.create_table_cond("index_tweets_follower_datetime")
	db.create_table_cond("cached_tweets")
	indices = db.get_table("indices")
	indices.set("users", 0)
	indices.set("tweets", 0)

setup_schema()
db = client.get_database("twitter")
indices = db.get_table("indices")
users = db.get_table("users")
tweets = db.get_table("tweets")
followers = db.get_table("followers")
followings = db.get_table("following")
index_users_username = db.get_table("index_users_username")
index_tweets_creator_datetime = db.get_table("index_tweets_creator_datetime")
index_tweets_follower_datetime = db.get_table("index_tweets_follower_datetime")
cached_tweets = db.get_table("cached_tweets")

def truncate_all():
	users.truncate()
	tweets.truncate()
	followers.truncate()
	followings.truncate()
	index_users_username.truncate()
	index_tweets_creator_datetime.truncate();
	cached_tweets.truncate()
	index_tweets_follower_datetime.truncate()

def new_id(table):
	return indices.add(table, 1)

def set_user(user):
	users.set(user["user_id"], dumps(user))
	index_users_username.set(user["username"], user["user_id"])
	followers.set(user["user_id"], dumps([]))
	followings.set(user["user_id"], dumps([]))
	cached_tweets.set(user["user_id"], dumps([]))

def get_user_by_id(user_id):
	user = loads(users.get(user_id))
	return user

def get_user_by_username(username):
	user_id = index_users_username.get(username)
	user = loads(users.get(user_id))
	return user
	
def authenticate_user(username, password):
	user = get_user_by_username(username)
	if user is None: return False
	if user["password"] != password: return False
	return True

def follow(follower, following):
	follower_id = follower["user_id"]
	following_id = following["user_id"]
	li = loads(followers.get(following_id))
	li.append(follower_id)
	followers.set(following_id, dumps(li))
	li = loads(followings.get(follower_id))
	li.append(following_id)
	followings.set(follower_id, dumps(li))

def get_follower_ids(user_id):
	return loads(followers.get(user_id))

def tweet_message(user, message):
	# save tweet
	tweet = {}
	tweet["tweet_id"] = new_id("tweets")
	tweet["user_id"] = user["user_id"]
	tweet["username"] = user["username"]
	tweet["datetime"] = Util.typemap(datetime.now())
	tweet["message"] = message
	tweets.set(tweet["tweet_id"], dumps(tweet))
	# save tweet index by creator/datetime --- to list the latest tweets tweeted by user
	index_tweets_creator_datetime.set(Util.composite(tweet["user_id"], tweet["datetime"]), tweet["tweet_id"])
	# save tweet index by follower/datetime for each follower --- to list the latest tweets per stream
	follower_ids = get_follower_ids(tweet["user_id"])
	for follower_id in follower_ids:
		index_tweets_follower_datetime.set(Util.composite(follower_id, tweet["datetime"]), tweet["tweet_id"])
	# cache last 200
	li = loads(cached_tweets.get(tweet["user_id"]))
	li.append(tweet["tweet_id"])
	if len(li) > 200:
		li.pop()
	cached_tweets.set(tweet["user_id"], dumps(li));
	# cache last 200 tweets for followers
	for follower_id in follower_ids:
		li = loads(cached_tweets.get(follower_id))
		li.append(tweet["tweet_id"])
		if len(li) > 200:
			li.pop()
		cached_tweets.set(follower_id, dumps(li))
	
def print_stream(user):
	print("%s's stream:" % user["username"])
	tweet_ids = loads(cached_tweets.get(user["user_id"]))
	for tweet_id in tweet_ids:
		tweet = loads(tweets.get(tweet_id))
		print("%s: %s" % (tweet["username"], tweet["message"]))
	print()

truncate_all()

mtrencseni = {}
mtrencseni["user_id"] = new_id("users")
mtrencseni["username"] = "mtrencseni"
mtrencseni["password"] = "password1"
mtrencseni["interests"] = "Foo and Bar!"
mtrencseni["datetime"] = Util.typemap(datetime.now())
set_user(mtrencseni)

agazso = {}
agazso["user_id"] = new_id("users")
agazso["username"] = "agazso"
agazso["password"] = "password2"
agazso["interests"] = "Foo and Bar!"
agazso["datetime"] = Util.typemap(datetime.now())
set_user(agazso)

follow(mtrencseni, agazso)
#follow(agazso, mtrencseni)

tweet_message(mtrencseni, "Hello world!")
tweet_message(mtrencseni, "Foo Bar")
tweet_message(agazso, "Me too")
tweet_message(mtrencseni, "Acme")

print(authenticate_user("mtrencseni", "hello"))
print(authenticate_user("mtrencseni", "password1"))
print_stream(agazso)
print_stream(mtrencseni)
