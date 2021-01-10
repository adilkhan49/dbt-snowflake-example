import pandas as pd
import boto3
from datetime import datetime
import random
import numpy as np
import json
from time import sleep

# product and customer dims

products = [
    {'product_id': 1001,
     'price': 40.99},
    {'product_id': 1002,
     'price': 100.0},
    {'product_id': 1003,
     'price': 12.50},
    {'product_id': 1004,
     'price': 40.99},
    {'product_id': 1005,
     'price': 4.20},
    {'product_id': 1006,
     'price': 84.39},
    {'product_id': 1007,
     'price': 299.99},
    {'product_id': 1008,
     'price': 8.0},
    {'product_id': 1009,
     'price': 33.0},
    {'product_id': 1010,
     'price': 10.0},
]

customers = np.arange(100,201,1).tolist()

# define functions

def generate_quantity():
    quantity = 0
    while not quantity:
        quantity = np.random.poisson(1)
    return quantity

def generate_data():
    global order_id
    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    customer_id = random.choice(customers)
    product = products[random.choice(range(len(products)))]
    product_id = product['product_id']
    quantity = generate_quantity()
    order_id += 1

    return {
        'datetime': dt,
        'order_id': order_id,
        'customer_id': customer_id,
        'product_id': product_id,
        'quantity': quantity}

def write_to_s3(data,filename):
    bucket = 'adil-test'
    client = boto3.client('s3',
        aws_access_key_id='ASDFASDFADFSASDF',
        aws_secret_access_key='asdfasdfasdfasdfasdfasdfasdfasdfasdfaasdf')
    res = client.put_object(
        Body=json.dumps(data), 
        Bucket=bucket,
        Key=filename)
    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f'{dt}\t{filename}\t{len(data)}')
# load product and customer dims

ts = datetime.now().strftime("%Y%m%d%H%M%S")
write_to_s3(products,f'dbt/products/products_{ts}.json')
write_to_s3(customers,f'dbt/customers/customers_{ts}.json')

records_per_file = 100
delay_mean = 3 # seconds
delay_sigma = 1 # seconds

try:
    order_id = int(open('order_id.txt','r').read())
except:
    order_id = 100000001
    

data = []
while True:
    data.append(generate_data())
    
    if order_id%records_per_file == 0:
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f'dbt/orders/orders_{ts}.json'
#         with open(filename,'w') as f:
#             f.write(json.dumps(data))
        try:
            write_to_s3(data,filename)
            open('order_id.txt','w').write(str(order_id))
            data = []
        except:
            print(f'{ts}\tFailed')
            raise
        
    sleep(round(abs(np.random.normal(delay_mean, delay_sigma)),2))