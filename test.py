import json

message = '{"transaction_id":"4","user_id":"4","card_number":"339284521297","amount":"145.37","description":"description","transaction_type":"Dining","vendor":"UberEats","approval_status":"APPROVED"}'

trxn = ''
python_obj = json.loads(message)
print(python_obj)
for key in python_obj:
    trxn += python_obj[key] + ','
trxn = trxn[:-1]
print("Hello World")
print(trxn)
