##########################
#                        #
#  Made by Caio Chiuchi  #
#                        #
##########################

import csv
import os
import json
import time

#array das regioes
regions = [
    "sa-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "ca-central-1",
    "eu-central-1",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-north-1",
    "ap-east-1",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-south-1",
    "us-east-1"
]

#arquivo que captura as credenciais das contas
with open("/home/ec2-user/credentials.json", 'r') as f:
    data = json.load(f)
    for i in data.keys():
        #escreve credenciais da conta
        with open("/home/ec2-user/.aws/credentials", 'w') as g:
            g.write("[default]\n")
            g.write("aws_access_key_id = "+data[i]['access_id']+"\n")
            g.write("aws_secret_access_key = "+data[i]['secret_key']+"\n")
        g.close()
        #itera sobre as regioes
        for reg in regions:
            with open("/home/ec2-user/.aws/config", 'w') as h:
                h.write("[default]\n")
                h.write("region = "+reg+"\n")
            h.close()
            #chamado do python que realiza a captura da metricas das instancias
            os.system("python instances.py " + i)
    f.close()
