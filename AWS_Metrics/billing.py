##########################
#                        #
#  Made by Caio Chiuchi  #
#                        #
##########################

import boto3
import mysql.connector
import csv
import sys
from datetime import datetime
import time
import os
import json
from datetime import datetime, timedelta
import datetime
import re

#formatacao da data
start_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
end_date = datetime.datetime.now().strftime("%Y-%m-%d")

#escrita da conta no arquivo credentials
with open("/home/ec2-user/.aws/credentials", 'w') as g:
    g.write("[default]\n")
    g.write("aws_access_key_id = {} \n")
    g.write("aws_secret_access_key = {} \n")
    g.close()
    
    #conexao com o cost explorer
    client = boto3.client('ce')

    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date 
        },
        Metrics=["BlendedCost"],
        Granularity='DAILY',
        GroupBy=[
            {'Type': 'TAG','Key': 'grafana'},
        ]
    )
    #laco para captura do valor diario gasto da instancia
    for res in response['ResultsByTime']:
        for gr in res['Groups']:
            info = []
            inst_id = gr['Keys'][0].split('$')[1]
            amount = gr['Metrics']['BlendedCost']['Amount']
            if inst_id != "":
                info.append([inst_id, amount])
                with open("/home/ec2-user/billing/billing.csv", "a+") as f:
                    #laco para escrita no arquivo
                    for a in info:
                        arq = csv.writer(f)
                        arq.writerow([res['TimePeriod']['Start'], inst_id, amount])
                    f.close()

#conexao com o banco
db = mysql.connector.connect(
    user='',
    host='',
    password='',
    database=''
)

cursor = db.cursor()
    
cursor.execute("CREATE TABLE IF NOT EXISTS billing( \
                        Data VARCHAR(255), \
                        ID int PRIMARY KEY AUTO_INCREMENT, \
                        InstanceId varchar(255),\
                        Billing varchar(255)\
                    )"
                )

csv_data = csv.reader(file("/home/ec2-user/billing/billing.csv"))
#escrita no banco
for row in csv_data:
    cursor.execute("INSERT INTO billing(Data,InstanceId,Billing) VALUES(%s,%s,%s)",row)

db.commit()
db.close()
