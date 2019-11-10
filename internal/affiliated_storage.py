from interface import Interface

class IAffiliatedStorage(Interface):
	def insert( self, data ): pass
	def getOne( self, field, value ): pass
	def getAll( self, field, value ): pass