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
from botocore.client import Config

try:
    # variavel para identificar a conta
    archive = sys.argv[1]
    try:
        archive = archive.replace("-","_")
    except:
        pass
    dat = datetime.now()
    #dat = str(time.mktime(now.timetuple())).split(".")

    # realiza a captura das instancias de determinada regiao
    config = Config(connect_timeout=15, read_timeout=15)
    ec2 = boto3.client('ec2', config=config)
    response = ec2.describe_instances()
    info = response.get('Reservations')

    instance = []
    # laco para acessar lista reservation
    for res in info:
        info2 = res.get('Instances')
        # laco para acessar lista instances e realizar a captura dos dados
        for inst in info2:
            try:
                tag = inst["Tags"]
                for t in tag:
                    if t.get('Key') == 'Name':
                        name = t.get('Value')
            except:
                name = ""
            instance_id = inst["InstanceId"]
            os.system("python processes.py " + archive + " " + instance_id)
            instance_type = inst["InstanceType"]
            image_id = inst["ImageId"]
            status = inst.get('State').get('Name')
            launch_time = inst["LaunchTime"]
            t = launch_time.strftime("%Y-%m-%d %H:%M:%S")
            place_az = inst.get('Placement').get('AvailabilityZone')
            place_tenancy = inst.get('Placement').get('Tenancy')
            private_ip = inst.get('PrivateIpAddress')
            info3 = inst['NetworkInterfaces']
            if len(info3) > 0:
                for i in info3:
                    info4 = i['PrivateIpAddresses']
                    for ip in info4:
                        try:
                            public_ip = ip.get('Association').get('PublicIp')
                        except:
                            public_ip = 'None'
            else:
                public_ip = "None"
            private_dns = inst.get('PrivateDnsName')
            public_dns = inst.get('PublicDnsName')
            state_transition_reason = inst.get('StateTransitionReason')
            #concatena as info das instancias
            instance.append([name, instance_id, instance_type, image_id, status, t, place_az, place_tenancy, private_ip, public_ip, private_dns, public_dns, state_transition_reason])

    
    # realiza escrita no arquivo
    with open("/home/ec2-user/instances/"+ archive +".csv", 'a+') as g:
        for aux in instance:
            arq = csv.writer(g)
            arq.writerow([dat, archive, aux[0], aux[1], aux[2], aux[3], aux[4], aux[5], aux[6], aux[7], aux[8], aux[9], aux[10], aux[11], aux[12]])

    # conexao com BD
    db = mysql.connector.connect(
        user='',
        host='',
        password='',
        database=''
    )

    cursor = db.cursor()
    #criacao da tabela
    cursor.execute("CREATE TABLE IF NOT EXISTS semantix( \
                           Timestamp VARCHAR(255), \
                           AccountName varchar(255),\
                           ID int PRIMARY KEY AUTO_INCREMENT, \
                           InstanceName varchar(255), \
                           InstanceId varchar(255),\
                           InstanceType varchar(255),\
                           ImageId varchar(255),\
                           StateName varchar(255),\
                           LaunchTime varchar(255),\
                           PlacementAvailabilityZone varchar(255),\
                           PlacementTenancy varchar(255),\
                           PrivateIpAddress varchar(255),\
                           PublicIpAddress varchar(255),\
                           PrivateDnsName varchar(255),\
                           PublicDnsName varchar(255),\
                           StateTransitionReason varchar(255)\
                       )"
               )
    #abertura do arquivo para leitura
    csv_data = csv.reader(file("/home/ec2-user/instances/"+archive+".csv"))
    #escrita dos valores na tabela
    for row in csv_data:
        cursor.execute("INSERT INTO semantix(Timestamp,AccountName,InstanceName,InstanceId,InstanceType,ImageId,StateName,LaunchTime,PlacementAvailabilityZone,PlacementTenancy,PrivateIpAddress,PublicIpAddress,PrivateDnsName,PublicDnsName,StateTransitionReason) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",row)

    db.commit()
    db.close()

except Exception as e:
#    print(str(e))
    pass
