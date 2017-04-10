import hashlib
import math


def sha1(_object):
	_str=str(_object)
	_byte=_str.encode('utf8')
	return int(hashlib.sha1(_byte).hexdigest(),16)


class ADDR:
	def __init__(self,addr=None):
		if addr!=None:
			self.ip=addr[0]
			self.port=int(addr[1])
	def to_tuple(self):
		return (self.ip,self.port)
	def to_string(self):
		return ','.join((self.ip,str(self.port)))
	def from_string(self,_str):
		self.ip,port=_str.split(',')
		self.port=int(port)
	def __str__(self):
		return '('+','.join((self.ip,str(self.port)))+')'
	def get_NID(self):
		return sha1(self.to_tuple())
	def get_index(self,_NID):
		d=self.get_d(_NID)
		if d==0:
			return None
		index=math.floor(math.log(d,2))
		return index
	def get_d(self,_NID):
		NID=self.get_NID()
		d=(NID-_NID)%16**40
		return d


class Message:
	def __init__(self,code=None,mes=[]):
		if code==None:
			return

		self.code=code
		self.message=mes

	def to_bytes(self):
		if self.message==[]:
			_bytes=self.code
		else:
			_bytes=self.code+'\n'+'\n'.join(self.message)
		_bytes=bytes(_bytes,encoding='utf-8')
		return _bytes

	def from_bytes(self,_bytes):
		data=str(_bytes,encoding='utf-8')
		data=data.split('\n')
		self.code=data[0]
		self.message=data[1:]

class Finger_Table:
	def __init__(self,n=0):
		if n==0:
			return
		self.table=[None]*n
	def __setitem__(self,k,v):
		self.table[k]=v
	def __getitem__(self,k):
		return self.table[k]
	def get_not_none(self):
		return [_ADDR for _ADDR in self.table if _ADDR != None]
	def get_num(self):
		return len(self.get_not_none())

	def to_bytes(self):
		not_none=self.get_not_none()
		table=[]
		for _ADDR in not_none:
			addr_str=_ADDR.to_string()
			table.append(addr_str)
		table_str='\n'.join(table)
		return bytes(table_str,encoding='utf-8')

	def to_string(self):
		"""
		for print
		"""
		table=[]
		for i in range(len(self.table)):
			_ADDR=self.table[i]
			if _ADDR!=None:
				addr_str=','.join((str(i),_ADDR.to_string()))
				table.append(addr_str)
		table_str='\n'.join(table)
		return 'Finger Table\n'+table_str

	def from_bytes(self,_bytes):
		_str=str(_bytes,encoding='utf-8')
		_table=_str.split('\n')
		if _table==['']:
			_table=[]
		self.table=[]
		for addr_str in _table:
			_ADDR=ADDR()
			_ADDR.from_string(addr_str)
			self.table.append(_ADDR)

	def get_successor(self):
		try:
			return self.get_not_none()[0]
		except:
			return None

if __name__=='__main__':
	for port in (1001,1002,1003,1004):
		print (port,"%50d"%ADDR(('127.0.0.1',port)).get_NID())
	i=ADDR(('127.0.0.1',1001)).get_index(1459299520012201582652553095622947857220265431173)
	print (i)