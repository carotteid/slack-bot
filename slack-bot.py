import json
import sys
from pip._vendor import requests
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
import logging
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pymongo import MongoClient


# post_request
# Description: Function to make a http call using POST
# Params: url (string) : URL to make the call
#         data (dict)  : data information to post
# Return: response (dict) : JSON with the result
def post_request(url, data):
  byte_length = str(sys.getsizeof(data))
  headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
  response = requests.post(url, data=json.dumps(data), headers=headers)

  if response.status_code != 200:
    raise Exception(response.status_code, response.text)

  return response


# get_request
# Description: Function to make a http call using GET
# Params: url (string) : URL to make the call
#         headers (dict) : header information to access
#         payload (dict) : data information to get
# Return: response (dict) : JSON with the result
def get_request(url, headers, payload = ""):
  response = requests.get(url, headers=headers, data=payload)

  if response.status_code != 200:
    raise Exception(response.status_code, response.text)

  return response


# yesterday
# Description: Function to get yesterday date
# Params: Nothing
# Return: yesterday date on date type
def yesterday():  
  return today() - dt.timedelta(days=1)


# today
# Description: Function to get today date
# Params: Nothing
# Return: today date on date type
def today():  
  return dt.date.today()


# date_time
# Description: Function to get date with min time
# Params: date (datetime)
# Return: date with min time on datetime type
def date_time(date):
  date_with_time = False
  if date:
    date_with_time = dt.datetime.combine(date, dt.datetime.now().time())
  else:
    print("date_time : Date not recieved")
  
  return date_with_time


# validate_date
# Description: Function to validate string date format
# Params: date (string) : date in string type
# Return: validation (bool)
def validate_date(date):
  validation = False
  try:
    validation = bool(dt.datetime.strptime(date, '%Y-%m-%d'))
  except ValueError:
    raise ValueError("validate_date: Incorrect data format, should be YYYY-MM-DD")

  return validation


# slack_message
# Description: Function to generate a JSON with a section Slack message
# Params: text (string)      : Message, if it is empty is for use fields for the section
#         fields (list)      : A list with fields for a message, it could be empty. By default is a empty list
#         type_text (string) : Message type, can be mrkdwn or plain_text. By default is mrkdwn
#         type (string)      : Type of message, could be section or header. By default is section
# Return: section (dict) : JSON with section generated
def slack_message(text, fields = [], type_text = "mrkdwn", type = "section"):
  section = {}
  if text:
    section = {
      "type" : type,
      "text" : {
        "type" : type_text,
        "text" : text
      }
    }
  elif fields:
    section = {
      "type" : type,
      "fields" : fields
    }
  else:
    print("slack_message: Text or Fields are required")
  
  return section


# slack_field
# Description: Function to generate a JSON with a field Slack message
# Params: text (string) : Field message
#         type (string) : Message type, can be mrkdwn or plain_text. By default is mrkdwn
# Return: field (dict)  : JSON with field generated
def slack_field(text, type = "mrkdwn"):
  field = {}
  if text:
    field = {
      "type" : type,
      "text" : text
    }
  else:
    print("slack_field: Text is required")

  return field


# send_slack
# Description: Function to send the message generated to Slack channel
# Params: data (dict) : JSON with the message to send
# Return: Nothing
def send_slack(data):
  url = "https://hooks.slack.com/services/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"

  message = post_request(url, data)
  if not(message.status_code == 200):
    print("send_slack: Message cannot be sent")


# send_slack_file
# Description: Send files to Slack using API
# Params: filename (string) : File name and route if is neccessary
# Return: Nothing
def send_slack_file(filename):
  # Slack api bot token
  token = "xoxb-3030484363377-3020169758148-vGjNBfPLSLZIGFxEHBB83V4Z"
  client = WebClient(token)
  logger = logging.getLogger(__name__)
  channel_id = "C03036S46AK"

  try:
    result = client.files_upload(
        channels = channel_id,
        file = filename,
    )
    logger.info(result)

  except SlackApiError as e:
    logger.error("send_slack_file : Error uploading file: {}".format(e))


# connect_mongo
# Function to get credentials to connect to database in MongoDB
# Params: Nothing
# Return: db (Mongo) database
def connect_mongodb():
  db = False
  # Connection string to use mongodb
  connection_string = "mongodb+srv://idavilacamargo:EhgI8UZlOFboeRBp@cluster.vbqgcel.mongodb.net/"

  try:
    client = MongoClient(connection_string)
    db = client['sample_analytics']
  except ValueError:
    raise ValueError("connect_mongodb : Cannot connect to MongoDB, check Connection String or the database existence.")

  return db


# sort_two_lists
# Sort two lists ordering one like header
# Params: list_1 (list)
#         list_2 (list)
# Return: new_lists (list) with the two lists ordered
def sort_two_lists(list_1, list_2):
  list_1, list_2 = zip(*sorted(zip(list_1, list_2)))
  new_list_1 = list(list_1)
  new_list_2 = list(list_2)
  new_lists = [new_list_1, new_list_2]
  return new_lists


# write_labels
# Method to write summary upper bar in a graph
# Params: counts (list)
#         numeration (list)
#         padding (float) optional
# Return: Nothing
def write_labels(counts, numeration, padding=0):
  for y,x in zip(counts, numeration):
    label = "{}".format(y)
    plt.annotate(label, (x+padding,y), textcoords="offset points", xytext=(0,5), ha='center')


def create_data():
  db = connect_mongodb();
  title = slack_message(":arrow_right: 10 random transactions")
  message = []

  try:
    plot_name = "/tmp/Random_Transactions.png"
    documents = db["transactions"]
    documents_filtered = documents.aggregate(
      [
        {
          '$group': {
            '_id': '$account_id'
          }
        }
      ]
    )
    documents_list = list(documents_filtered)
    print(documents_list)

  except ValueError:
    raise ValueError("get_documents : Error to get documents")


# main
# Description: Main function to organize message and send it to Slack
# Params: Nothing
# Return: Nothing
def main():
  date = str(yesterday())
  if date and validate_date(date):
    div = [{ "type": "divider" }]

    message = {
      "blocks" : div
    }

    send_slack(message)
    create_data();

  else:
    print("main: There was an error getting yesterday date")


# lambda_handler
# Description: Main function to use script on AWS Lambda
# Params: Nothing
# Return: statusCode (int), body (string) 
def lambda_handler(event, context):  
  main()
  return {
    'statusCode': 200,
    'body': json.dumps('Hello! Slack bot report is working :D')
  }


# If you want to test locally, use this function
if __name__ == "__main__":
  main()