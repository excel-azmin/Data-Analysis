import pandas as pd
import json
all_data = pd.read_json("/home/azmin/Desktop/pending_jobs.json", lines=True)
inv_list = []
for index, row in list(all_data.iterrows()):
    inv_list.append(dict(row))
    
sales_invoice_name = []
set_warehouse = []
serial_no = []
item_code = []
item_name = []

for i in range(0, len(inv_list)):
    if 'sales_invoice_name' in inv_list[i]['data']['payload']:
        sales_invoice_name.append(inv_list[i]['data']['payload']["sales_invoice_name"])
    if 'set_warehouse' in inv_list[i]['data']['payload']:
        set_warehouse.append(inv_list[i]['data']['payload']["set_warehouse"])
    if 'items' in inv_list[i]['data']['payload']:
        items = inv_list[i]['data']['payload']['items']
        for item in items:
            if 'serial_no' in item:
                serial_no.append(str(item['serial_no']).replace("\n", ","))
            if 'item_code' in item:
                item_code.append(item['item_code'])    
            if 'item_name' in item:
                item_name.append(item['item_name'])     
df = pd.DataFrame(list(zip(sales_invoice_name, set_warehouse, serial_no,item_code,item_name)),
               columns =['sales_invoice_name', 'set_warehouse', 'serial_no', 'item_code','item_name' ])

import os  
os.makedirs('/home/azmin/Desktop/queue_data', exist_ok=True)  
df.to_csv('/home/azmin/Desktop/queue_data/pending_jobs.csv')
