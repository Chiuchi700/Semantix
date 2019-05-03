import requests
from datetime import datetime
import csv
import os
import time

def moeda(html):
	#verifica o titulo da moeda
	aux = html.find("instrumentH1inlineblock") + 30
	#coloca o titulo em um formato sem espaços
	tipo_moeda = html[aux:aux+31]

	return tipo_moeda

def cotacao(html):
	#localiza cotacao
	aux = html.find("lastInst pid-2103-last") + 30
	#retira os espacos do valor obtido
	cot = html[aux:aux+29].strip()

	return cot

def mudanca(html):
	#localiza mudanca
	aux = html.find("pid-2103-pc") + 30
	#retira os espaços da variavel
	mud = html[aux:aux+20].strip()

	return mud;

def percentual(html):
	#localiza percentual
	aux = html.find("pid-2103-pcp") + 30
	#retira os espaços da variavel
	perc = html[aux:aux+10].strip()

	return perc

def timestamp(r):
    now = datetime.now()
    time = datetime.timestamp(now)

    return time

def gravar(saida):
	#abertura do arquivo com append
    arq = csv.writer(open('dolar_timestamp.csv', 'a+'), delimiter = ';')
	#escrita da linha
    if os.stat('dolar_timestamp.csv').st_size == 0:
        arq.writerow(['currency', 'value', 'change', 'perc', 'timestamp'])

    arq.writerow(saida)

def main():
	r = requests.get(url="https://m.investing.com/currencies/usd-brl", headers={'User-Agent':'curl/7.52.1'})
	#r.text -> html todo
	html = r.text
	#armazena as variaveis em uma linha
	saida = [moeda(html), cotacao(html), mudanca(html), percentual(html), timestamp(r)]

	#chamada do metodo para armazenar no arquivo
	gravar(saida)

#if '__name__' == __main__:

main()
