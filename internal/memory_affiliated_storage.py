from interface import implements
from affiliated_storage import IAffiliatedStorage

class MemoryAffiliatedStorage(implements(IAffiliatedStorage)):
	def __init__(self):
		self.db = {}

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
		self.db[post_data["inscricao"]] = post_data

	def getOne( self, field, value ):
		if field == "inscricao":
			return self.db.get( value )
		else:
			for key, record in  self.db.items():
				if record.get(field) == value:
					return record

	def getAll( self, field, value ):
		if field == "inscricao":
			return [ self.db.get( value ) ]
		else:
			results = []
			for key, record in  self.db.items():
				if record.get(field) == value:
					results.append( record )
		
		return results



