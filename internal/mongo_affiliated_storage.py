from interface import implements
from affiliated_storage import IAffiliatedStorage

#TODO: Não teste!!!

class MongoAffiliatedStorage(implements(IAffiliatedStorage)):
	def __init__(self, db_path):
		self.connection    = MongoClient( db_path )
		self.movimentacoes = self.connection.movimentacoes

	def insert(self, data ):
		post_data = {
			'data_atualizacao': data[0],
			'hora_atualizacao': data[1],
			'inscricao': data[2],
			'nome_filiado': data[3],
			'sigla_partido': data[4],
			'nome_partido': data[5],
			'uf': data[6],
			'codigo_municipio': data[7],
			'nome_municipio': data[8],
			'zona_eleitoral': data[9],
			'secao_eleitoral': data[10],
			'data_filiacao': data[11],
			'situacao_registro': data[12],
			'tipo_registro': data[13],
			'data_processamento': data[14],
			'data_desfiliacao': data[15],
			'data_cancelamento': data[16],
			'data_regularizacao': data[17],
			'motivo_cancelamento': data[18]
		}

		self.movimentacoes.insert_one( post_data )

	def getOne( self, field, value ):
		results = self.getAll( field, value )
		if results:
			return results[0]
		else 
			return []
		
	def getAll( self, field, value ):
		return self.movimentacoes.find( { field : value } )
