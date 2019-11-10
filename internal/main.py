# -*- coding: utf-8 -*-

#internal packages
import os
import urllib.request
import zipfile
import csv

#project packages
from memory_affiliated_storage import MemoryAffiliatedStorage
from mongo_affiliated_storage import MongoAffiliatedStorage

from const import *

def log( *infos ):
	for info in infos:
		print(info,end=""),
	print()

def fileNameNoExt( file, no_ext = False ):
	return os.path.splitext( os.path.basename( file ) )[0]

def downloadAllAffiliateData( root_path , files ):
	log("Baixando todos dados de filiados:")

	for file_name in files:
		downloadAffiliatedFile(root_path , file_name )

def downloadAffiliatedFile( root_path, file_name ):
	url         = URL["AFILIADOS"] + file_name 
	download_to = os.path.join( root_path , file_name )

	log("Baixando:", url )
	urllib.request.urlretrieve( url, download_to )

	log("Pronto!")


def unzipAllFiles( root_path , files ):
	log("Unzip todos arquivos:")

	for file in files:
		unzipFile( os.path.join( root_path , file), root_path )

def unzipFile( file, extract_to ):
	log("Unzip:", file)

	no_ext = True
	with zipfile.ZipFile( file , "r") as zip_ref:
		zip_ref.extractall( os.path.join( extract_to , fileNameNoExt( file ) ) )
	os.remove( file )

	log("Pronto!")


def createFolderStructure( folders ):
	log("Criando estrutura básica de pastas")

	for folder in folders:
		if not os.path.exists( folder ):
			os.makedirs( folder );	


def readCsvFile( file ):
	return csv.reader(open( file , 'r', newline=''), delimiter=';', quotechar='"')

def readAndSaveData( storage, files ):
	log("Salvando dados:")

	for file in files:
		log("Salvando arquivo ", file)
		file_data = readCsvFile( file )
		next( file_data ) #ignora primeira linha com titulos
		for line in file_data:
			storage.insert( line )
		log("Pronto!")
	
def captureAllData( storage, ufs, partidos ):
	log("Inicio do processo de capture de dados: UFS(",len(ufs),"), Partidos(",len(partidos),")")

	tmp_dir = "tmp"

	#criar estrutura basica de pastas
	createFolderStructure([ tmp_dir ])

	#cria todas combinacoes dos nomes de arquivos
	all_files     = [ "filiados_"+partido+"_"+uf for uf in ufs for partido in partidos ]
	all_zip_files = list( map(lambda file : file + ".zip", all_files) )
	all_csv_files = list( map(lambda file : os.path.join( tmp_dir, file,"aplic","sead","lista_filiados","uf", file + ".csv"), all_files) )

	#baixa todos arquivos de filiados
	downloadAllAffiliateData( tmp_dir, all_zip_files )

	#unzip todos arquivos
	unzipAllFiles( tmp_dir, all_zip_files )

	#TODO: funcao muito abrangente, talvez mudar
	#ler todos csvs
	readAndSaveData( storage, all_csv_files )

def main():

	#	Storage em memoria. Sem a necessidade de ter bancos instalados,
	#	bom para testes
	storage = MemoryAffiliatedStorage()

	#	Mongo como storage
	#storage = MongoAffiliatedStorage("mongodb://localhost:27017/")

	one_uf    = [ UFS[0] ] 
	one_party = [ PARTIDOS[0] ]

	captureAllData( storage, one_uf, one_party );

	result = mem_storage.getOne( "inscricao", "005591942453" )
	print(result)

	result = mem_storage.getAll( "nome_municipio", "SENA MADUREIRA" )
	for r in result:
		print(r["nome_filiado"])

if __name__ == "__main__":
	main()

#	TODO: mais uma abrastração faltando: o Model dos dados do csv. 
#	para que os detalhes de colunas fique fora dos Storage.