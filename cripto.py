from bs4 import BeautifulSoup
from datetime import datetime
import requests
import csv
import os

link = requests.get(url="https://m.investing.com/crypto/", headers={'User-Agent':'curl/7.52.1'})

now = datetime.now()
time = datetime.timestamp(now)

soup = BeautifulSoup(link.content, 'html.parser')

s = soup.find_all('tr')
	
text = []
saida = []

#realiza a captura das moedas e armazena em uma string saida
for i in s[1:]:
    #realiza substituição dos tabs por espaço vazio e separa moedas por linha
    text = i.getText().replace('\t', "").split("\n")

    pos = list(filter(None,text))
    #print(pos)

    saida = csv.writer(open('caioChiuchi/crawler_crypto/crypto_timestamp.csv','a+'), delimiter=',')
    if os.path.getsize('caioChiuchi/crawler_crypto/crypto_timestamp.csv') == 0:
        saida.writerow(['code', 'name', 'priceUSD', 'change24H', 'change7D', 'symbol', 'priceBTC', 'marketCap', 'volume24H', 'totalVolume', 'timestamp'])

        saida.writerow([pos[0],pos[1],pos[2],pos[3],pos[4],pos[5],pos[6],pos[7],pos[8],pos[9],time])
    else:
        saida.writerow([pos[0],pos[1],pos[2],pos[3],pos[4],pos[5],pos[6],pos[7],pos[8],pos[9],time])

