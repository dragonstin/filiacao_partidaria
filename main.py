import zipfile
import urllib.request 
from bs4 import BeautifulSoup


partidos = ['mdb', 'ptb']
#partidos = ['mdb', 'ptb', 'pdt', 'pt', 'dem', 'pcdob', 'psb', 'psdb', 'ptc', 'psc', 
#            'pmn', 'cidadania', 'pv', 'avante', 'pp', 'pstu', 'pcb', 'prtb', 'dc', 
#            'pco', 'pode', 'psl', 'republicanos', 'psol', 'pl', 'psd', 'patriota', 
#            'pros', 'solidariedade', 'novo', 'rede', 'pmb']

ufs = ['ac', 'al']

#ufs = ['ac', 'al', 'am', 'ap', 'ba', 'ce', 'df', 'es', 'go', 'ma', 'mg', 'ms', 'mt',
#       'pa', 'pb', 'pe', 'pi', 'pr', 'rj', 'rn', 'ro', 'rr', 'rs', 'sc', 'se', 'sp', 'to']


arquivos = []

# baixando arquivos
for partido in partidos:
       for uf in ufs:
              print('baixando '+partido+ '_' + uf)
              arquivo = 'filiados_'+partido + '_' + uf
              arquivos.append(arquivo)
              #Exemplo de url = http://agencia.tse.jus.br/estatistica/sead/eleitorado/filiados/uf/filiados_avante_ac.zip
              url = 'http://agencia.tse.jus.br/estatistica/sead/eleitorado/filiados/uf/'+arquivo+'.zip'
              urllib.request.urlretrieve(url, 'files/'+arquivo+'.zip')

for arquivo in arquivos:
       with zipfile.ZipFile('Files/'+arquivos+'.zip', "r") as zip_ref:
            zip_ref.extractall("Extract/")
