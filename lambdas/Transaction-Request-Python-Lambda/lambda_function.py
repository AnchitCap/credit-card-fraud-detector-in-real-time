import json
import base64
import requests
import random
import boto3
from boto3.dynamodb.conditions import Key, Attr

url = "https://9q700gqkbb.execute-api.us-east-1.amazonaws.com/api-v1/topics/topic2transactions"
headers = {
  'Content-Type': 'application/vnd.kafka.json.v2+json'
}

dynamodb = boto3.resource('dynamodb', "us-east-1") 
tableName = 'cctransactions'
table = dynamodb.Table(tableName)

confidence_points = 0
confidence_threshold = 65

def calculate_points(user_id):
    global confidence_points

    response = table.scan(
        TableName=tableName,
        FilterExpression=Attr('user_id').eq(user_id)
    )

    print(response['Items'])

    sum_purchases = 0
    num_purchases = 0
    transaction_points = 28
    transaction_occurences = {}
    vendor_points = 17
    vendor_visits = {}

    for record in response['Items']:
        # increment dict of transaction_types to determine which transaction categories the user makes purchases most often
        # and vendor_visits to determine which business(es) the user frequents most often
        transaction_occurences[record['transaction_type']] = transaction_occurences.get(record['transaction_type'], 0) + 1
        vendor_visits[record['vendor']] = vendor_visits.get(record['vendor'], 0) + 1
        sum_purchases += float(record['amount'])
        num_purchases += 1

    # if the user has made a purchase from this business category previously, allocate confidence points
    if current_transaction['transaction_type'] in transaction_occurences.keys():
        confidence_points += 25
    
    # if the user has made a purchase from this vendor previously, allocate confidence points
    if current_transaction['vendor'] in vendor_visits.keys():
        confidence_points += 15

    # if the current purchase isn't more than double the average spent on all purchases, allocate points
    if (float(current_transaction['amount']) / (sum_purchases/num_purchases)) < 2:
        confidence_points += 15

    # allocate more confidence points if the current purchase is from a frequently purchased category
    for types in sorted(transaction_occurences.items(), key=lambda x: x[1], reverse=True):
        if current_transaction['transaction_type'] == types[0]:
            confidence_points += transaction_points
            break
        else:
            transaction_points = transaction_points/1.25
    
    # allocate more confidence points if the current purchase is from a frequently visited vendor
    for business in sorted(vendor_visits.items(), key=lambda x: x[1], reverse=True):
        if current_transaction['vendor'] == business[0]:
            confidence_points += vendor_points
            break
        else:
            vendor_points = vendor_points/1.1

def determine_approval(python_obj):
    calculate_points(user_id)

    #coin_toss = random.randint(0,1)
    #if (coin_toss == 0):
    if (confidence_points >= confidence_threshold):
        python_obj['approval_status'] = 'APPROVED'
    else:
        python_obj['approval_status'] = 'REJECTED'

def lambda_handler(event, context):
    # test data
    current_transaction = {
        'amount': '50.05',
        'card_number': '821387325492',
        'transaction_id': '999',
        'user_id': '41',
        'description': 'description',
        'transaction_type': 'Dining'
    }

    for key in event['records']:
        for record in event['records'][key]:
            message = base64.b64decode(record['value'].encode('ascii')).decode('ascii')
            print(message)
            #python_obj = json.loads(message)
            python_obj = current_transaction # test data until dynamodb populated with more records
            determine_approval(python_obj)
            print(python_obj)
            final_obj = {'records': [{'value': {'transaction_id': python_obj['transaction_id'], 'user_id': python_obj['user_id'], 'card_number': python_obj['card_number'], 'amount': python_obj['amount'], 'description': python_obj['description'], 'transaction_type': python_obj['transaction_type'], 'vendor': python_obj['vendor'], 'approval_status': python_obj['approval_status']}}]}
            json_obj = json.dumps(final_obj)
            r = requests.request("POST", url, headers=headers, data=json_obj)
            print(r.text)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }