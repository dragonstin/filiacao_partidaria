import os
import csv
import json
import zipfile
import urllib.request 
from bs4 import BeautifulSoup
from pymongo import MongoClient

"""
# pega as informacoes de usuario e password do arquivo json
with open("credenciais.json", "r") as read_file:
       data = json.load(read_file)

user = data["user"]
password = data["password"]

# tentei usar o mongoDB Atlas, mas por causa do limite de espaço não rolou
#conecxaoBanco = MongoClient('mongodb://'+user+':'+password+'@cluster-shard-00-00-sgjhn.azure.mongodb.net:27017,cluster-shard-00-01-sgjhn.azure.mongodb.net:27017,cluster-shard-00-02-sgjhn.azure.mongodb.net:27017/test?ssl=true&replicaSet=cluster-shard-0&authSource=admin&retryWrites=true&w=majority')
"""
# ----------------------------------
print('Conecta no banco de dados...')
conecxaoBanco = MongoClient("mongodb://localhost:27017/")

# Cria o banco de dados com o nome "dados"
db = conecxaoBanco.dados
# Cria a tabela "movimentacoes"
posts = db.movimentacoes
# ----------------------------------

# lista de partidos
partidos = ['mdb']
#partidos = ['mdb', 'ptb', 'pdt', 'pt', 'dem', 'pcdob', 'psb', 'psdb', 'ptc', 'psc',
#            'pmn', 'cidadania', 'pv', 'avante', 'pp', 'pstu', 'pcb', 'prtb', 'dc', 
#            'pco', 'pode', 'psl', 'republicanos', 'psol', 'pl', 'psd', 'patriota', 
#            'pros', 'solidariedade', 'novo', 'rede', 'pmb']

# lista de UFs
ufs = ['ac']
#ufs = ['ac', 'al', 'am', 'ap', 'ba', 'ce', 'df', 'es', 'go', 'ma', 'mg', 'ms', 'mt',
#       'pa', 'pb', 'pe', 'pi', 'pr', 'rj', 'rn', 'ro', 'rr', 'rs', 'sc', 'se', 'sp', 'to']

def criarRepositorio(repositorio):
       "Verifica se o repositório existe, senão existir, cria repositório"
       if not os.path.exists(repositorio):
              os.makedirs(repositorio)


def baixarArquivo(arquivo):
       "Faz download do arquivo .zip para o diretório Files"
       url = 'http://agencia.tse.jus.br/estatistica/sead/eleitorado/filiados/uf/'
       
       # Verifica se o diretorio existe, senão existir cria diretorio Files
       criarRepositorio('Files')

       print('baixando '+arquivo)
       #Exemplo de url = http://agencia.tse.jus.br/estatistica/sead/eleitorado/filiados/uf/filiados_avante_ac.zip
       urllib.request.urlretrieve(url+arquivo+'.zip', 'Files/'+arquivo+'.zip')


def extrairArquivo(arquivo):
       "Extrai arquivo .zip do diretório 'Files' para o diretório 'Extract'"
       print('Extraindo arquivo...' + arquivo)

       #Verifica se o diretorio existe, senão existir cria diretorio Extract
       criarRepositorio('Extract')

       #Extrai arquivo .zip
       with zipfile.ZipFile('Files/'+arquivo+'.zip', "r") as zip_ref:
              zip_ref.extractall("Extract/")


def lerArquivo(pathArquivosExtraidos, arquivo):
       "Le arquivo dentro do diretório indicado e retorna o arquivo lido"
       print('Lendo arquivo...'+arquivo)

       ler = pathArquivosExtraidos +arquivo + '.csv'
       return csv.reader(open(ler, 'r', newline=''), delimiter=';', quotechar='"')


def insereDadosNoBanco(arquivoLido, posts):
       "Insere Dados no Banco"
       # 0-DATA DA EXTRACAO; 1-HORA DA EXTRACAO; 2-NUMERO DA INSCRICAO; 3-NOME DO FILIADO;
       # 4-SIGLA DO PARTIDO; 5-NOME DO PARTIDO; 6-UF; 7-CODIGO DO MUNICIPIO; 8-NOME DO MUNICIPIO;
       # 9-ZONA ELEITORAL; 10-SECAO ELEITORAL; 11-DATA DA FILIACAO; 12-SITUACAO DO REGISTRO;
       # 13-TIPO DO REGISTRO; 14-DATA DO PROCESSAMENTO; 15-DATA DA DESFILIACAO;
       # 16-DATA DO CANCELAMENTO; 17-DATA DA REGULARIZACAO; 18-MOTIVO DO CANCELAMENTO
       print('Insert dados no banco...')
       # Este comando list(arquivoLido)[1:] transforma o arquivo csv em uma lista e exclui a primeira linha (primeira linha apenas contem o nome das colunas)
       for row in list(arquivoLido)[1:]:

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

              # insert_one - este método automaticamente evita gravar dois registros iguais
              result = posts.insert_one(post_data)

              # limpa memória
              post_data = None
              mydoc = None


# Função principal
def controleInsercaoDadosBanco(partidos, ufs, posts):
       "Cordena as funções anteriores, baixando, extraindo, lendo e inserindo dados no banco"
       for partido in partidos:
              for uf in ufs:
                     arquivo = 'filiados_'+partido + '_' + uf
                     
                     # baixa arquivo
                     baixarArquivo(arquivo)
                     
                     # extrai arquivo
                     extrairArquivo(arquivo)
                     
                     # delete o .zip que acabou de ser extraído
                     os.remove('Files/'+arquivo+'.zip')
              
                     # diretórios gerados ao extrair o arquivo .zip
                     pathArquivosExtraidos = 'Extract/aplic/sead/lista_filiados/uf/'
                     
                     # Ao extrair arquivos, gera uns arquivos a mais, esse processo exclui os
                     # arquivos desnecessário, deixando apenas o arquivo que iremos ler
                     dir = os.listdir(pathArquivosExtraidos)
                     for file in dir:
                            if(file != arquivo + '.csv'):
                                   os.remove(pathArquivosExtraidos+file)

                     # Le o arquivo e retorna o arquivo lido
                     arquivoLido = lerArquivo(pathArquivosExtraidos, arquivo)

                     insereDadosNoBanco(arquivoLido, posts)




# -------------------------------
# Controla as funções até a inserção de dados no banco
controleInsercaoDadosBanco(partidos, ufs, posts)

# -------------------------------
# consulta dados no banco 
print('Realiza consulta no banco de dados')
result = posts.find({'inscricao': '000671372410'})
for registro in result:
       print(registro)

