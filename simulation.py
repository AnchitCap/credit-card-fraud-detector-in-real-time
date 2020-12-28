#!/usr/bin/python
import csv
import requests
import json

url = "https://3yjle7fkel.execute-api.us-east-1.amazonaws.com/api-v1/topics/amazonmskapigwblog"
headers = {
  'Content-Type': 'application/vnd.kafka.json.v2+json'
}

def simulate():
	with open('transactiondata.csv', newline='') as csvfile:
		csvreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
		next(csvreader)
		for row in csvreader:
			#print(row)
			transaction = {'records': [{'value': {'transaction_id': row[0], 'user_id': row[1], 'card_number': row[2], 'amount': row[3], 'description': row[4], 'transaction_type':row[5], 'vendor':row[6]}}]}
			json_obj = json.dumps(transaction)
			#print(json_obj)
			r = requests.request("POST", url, headers=headers, data=json_obj)
			print(r.text)

if __name__ == "__main__":
	simulate()
