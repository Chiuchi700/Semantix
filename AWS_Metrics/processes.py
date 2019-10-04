##########################
#                        #
#  Made by Caio Chiuchi  #
#                        #
##########################

import mysql.connector
import boto3
import csv
import sys
from datetime import datetime
import time
import os
import json
import boto.ec2.cloudwatch
from datetime import datetime, timedelta
import datetime

#variavel para identificar a conta e id da instancia
archive = sys.argv[1]
inst_id = sys.argv[2]

info = []

#conexao com o cloudwatch
client = boto3.client('cloudwatch')
response = client.get_metric_statistics(
    Namespace='AWS/EC2',
    MetricName='CPUUtilization',
    Dimensions=[
        {
            'Name': 'InstanceId',
            'Value': inst_id
        },
    ],
    StartTime=datetime.datetime.now() - datetime.timedelta(days=1),
    EndTime=datetime.datetime.now(),
    Period=86400,
    Statistics=[
        'Average',
    ],
    Unit='Percent'
)
#laco para para captura da media de proces da cpu
for cpu in response['Datapoints']:
    if 'Average' in cpu:
        dat = datetime.datetime.now() - datetime.timedelta(days=1)
        info.append([dat, inst_id, cpu['Average']])
        with open("/home/ec2-user/process/" + archive + "_process.csv", "a+") as g:
            #realiza a escrita no arquivo
            for arq in info:
                arq = csv.writer(g)
                arq.writerow([dat, inst_id, cpu['Average']])
        #conexao com o banco
        db = mysql.connector.connect(
            user='',
            host='',
            password='',
            database=''
        )

        cursor = db.cursor()

        cursor.execute("CREATE TABLE IF NOT EXISTS processamento( \
                                Timestamp VARCHAR(255), \
                                ID int PRIMARY KEY AUTO_INCREMENT, \
                                InstanceId varchar(255),\
                                ProcessAVG varchar(255)\
                            )"

                    )
        csv_data = csv.reader(file("/home/ec2-user/process/"+archive+"_process.csv"))
        #escrita no banco
        for row in csv_data:
            cursor.execute("INSERT INTO processamento(Timestamp,InstanceId,ProcessAVG) VALUES(%s,%s,%s)",row)

        db.commit()
        db.close()

