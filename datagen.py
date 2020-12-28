#!/usr/bin/python
import csv
import random
import os

types = ['Dining', 'Transportation', 'Leisure', 'Rent', 'Other']
diningVendors = ['Seamless','GrubHub','UberEats','DoorDash']
transportationVendors = ['Uber','Lyft','MTA','NYCTLC']
leisureVendors = ['Amazon', 'Apple', 'Walmart', 'Alibaba', 'Microsoft', 'Ebay', 'Rakuten', 'Target', 'BestBuy', 'Nike']
rentVendors = ['Cozy', 'ClickPay', 'RadPad', 'PayLease']
otherVendors = ['Walgreens', 'HomeDepot', 'CVS', 'Costco', 'WholeFoods', 'TraderJoes']

def gen_cardnumber():
	card_num = ''
	for i in range(0,12):
		card_num += str(random.randrange(0,10))
	return card_num

def gen_data():
	if os.path.exists('transactiondata.csv'):
		os.remove('transactiondata.csv')

	with open('transactiondata.csv', 'w', newline='') as csvfile:
		random.seed()
		csvwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
		csvwriter.writerow(['transaction_id', 'user_id', 'card_number', 'amount', 'description', 'transaction_type', 'vendor'])
		for i in range(0,1000):
			type = types[random.randrange(0,5)]
			
			if type == 'Dining':
				vendor = random.choice(diningVendors)
			elif type == 'Transportation':
				vendor = random.choice(transportationVendors)
			elif type == 'Leisure':
				vendor = random.choice(leisureVendors)
			elif type == 'Rent':
				vendor = random.choice(rentVendors)
			elif type == 'Other':
				vendor = random.choice(otherVendors)

			csvwriter.writerow([i, random.randrange(0,100), gen_cardnumber(), round((random.random() * 200 + 1), 2), 'description', type, vendor])

if __name__ == "__main__":
	gen_data()
