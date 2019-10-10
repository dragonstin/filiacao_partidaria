import os
import csv
import json
import zipfile
import urllib.request 
from bs4 import BeautifulSoup
from pymongo import MongoClient

# pega as informacoes de usuario e password do arquivo json
with open("password.json", "r") as read_file:
       data = json.load(read_file)

user = data["user"]
password = data["password"]


# lista de partidos
partidos = ['mdb', 'ptb']
#partidos = ['mdb', 'ptb', 'pdt', 'pt', 'dem', 'pcdob', 'psb', 'psdb', 'ptc', 'psc', 
#            'pmn', 'cidadania', 'pv', 'avante', 'pp', 'pstu', 'pcb', 'prtb', 'dc', 
#            'pco', 'pode', 'psl', 'republicanos', 'psol', 'pl', 'psd', 'patriota', 
#            'pros', 'solidariedade', 'novo', 'rede', 'pmb']


# lista de UFs
ufs = ['ac', 'al']
#ufs = ['ac', 'al', 'am', 'ap', 'ba', 'ce', 'df', 'es', 'go', 'ma', 'mg', 'ms', 'mt',
#       'pa', 'pb', 'pe', 'pi', 'pr', 'rj', 'rn', 'ro', 'rr', 'rs', 'sc', 'se', 'sp', 'to']

url = 'http://agencia.tse.jus.br/estatistica/sead/eleitorado/filiados/uf/'

downloadZipFiliados(url, partidos, ufs)



def downloadZipFiliados(url, partidos, ufs):
       "Faz download dos arquivos .zip para a pasta Files - Monta a url com as informacoes do partido e da UF"
       # Faz download dos arquivos
       for partido in partidos:
              for uf in ufs:
                     print('baixando '+partido+ '_' + uf)
                     arquivo = 'filiados_'+partido + '_' + uf
                     #Exemplo de url = http://agencia.tse.jus.br/estatistica/sead/eleitorado/filiados/uf/filiados_avante_ac.zip
                     urllib.request.urlretrieve(url+arquivo+'.zip', 'files/'+arquivo+'.zip')
       return

def carregaListaArquivosBaixados():
       "Carrega lista arquivos baixados"
       return os.listdir("Files/")
       

def extraiArquivo():
       "Extrai os arquivo para a pasta Extract"
       arquivos = carregaListaArquivosBaixados()
       for arquivo in arquivos:
              with zipfile.ZipFile('Files/'+arquivo, "r") as zip_ref:
                     zip_ref.extractall("Extract/")
       return

def leArquivos():
       "le os arquivos extraidos"
       # le os arquivos
       #print(arquivos[0].replace('.zip', '.csv'))

       ler = 'Extract/aplic/sead/lista_filiados/uf/'+arquivos[0].replace('.zip', '.csv')
       arquivoLido = csv.reader(open(ler, 'r', newline=''), delimiter=';', quotechar='"')
       return

       
def conectaBancoDados():
       "Conecta no banco de dados"
       client = MongoClient('mongodb://'+user+':'+password+'@cluster-shard-00-00-sgjhn.azure.mongodb.net: 27017, cluster-shard-00-01-sgjhn.azure.mongodb.net: 27017, cluster-shard-00-02-sgjhn.azure.mongodb.net: 27017/test?ssl=true & replicaSet=cluster-shard-0 & authSource=admin & retryWrites=true & w=majority')
       db = client.pymongo_test
       posts = db.posts

       #print('One post: {0}'.format(result.inserted_id))

       # 0-DATA DA EXTRACAO; 1-HORA DA EXTRACAO; 2-NUMERO DA INSCRICAO; 3-NOME DO FILIADO; 
       # 4-SIGLA DO PARTIDO; 5-NOME DO PARTIDO; 6-UF; 7-CODIGO DO MUNICIPIO; 8-NOME DO MUNICIPIO; 
       # 9-ZONA ELEITORAL; 10-SECAO ELEITORAL; 11-DATA DA FILIACAO; 12-SITUACAO DO REGISTRO; 
       # 13-TIPO DO REGISTRO; 14-DATA DO PROCESSAMENTO; 15-DATA DA DESFILIACAO; 
       # 16-DATA DO CANCELAMENTO; 17-DATA DA REGULARIZACAO; 18-MOTIVO DO CANCELAMENTO
       for row in arquivoLido:
              post_data = {
                     'data_atualizacao': row[0],
                     'hora_atualizacao': row[1],
                     'inscricao': row[2],
                     'nome_filiado': row[3],
                     'sigla_partido': row[4],
                     'nome_partido': row[5],
                     'uf': row[6],
                     'codigo_municipio': row[7],
                     'nome_municipio': row[8],
                     'zona_eleitoral': row[9],
                     'secao_eleitoral': row[10],
                     'data_filiacao': row[11],
                     'situacao_registro': row[12],
                     'tipo_registro': row[13],
                     'data_processamento': row[14],
                     'data_desfiliacao': row[15],
                     'data_cancelamento': row[16],
                     'data_regularizacao': row[17],
                     'motivo_cancelamento': row[18]
              }
              result = posts.insert_one(post_data)
       
       return
