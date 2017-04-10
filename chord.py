import socket
import _thread
import time
import sys
import math
from _lib import ADDR,Message,Finger_Table,sha1
import os

"""
协议
'table',source_addr			# 请求finger表
'insert',k,v 				# 询问-插入(k,v)的节点
'look up',source_addr,k 	# 询问-存储(k,v)的节点
'kv',k,v 					# 将(k,v)发送给源
'request kvs',source_addr	# 请求(k,v)s
'kvs'	 					# 发送(k,v)s
'leave',source_addr,successor_addr		# 节点离开
"""

class Chord_Node_0:
	"""
	不考虑节点失效
	"""
	def __init__(self,port,time_interval=5):
		
		self.ADDR=self._create_recv_socket(port)
		if self.ADDR==None:
			print ('此端口已经被使用。按回车退出')
			input ()
			sys.exit(0)

		self.NID=self.ADDR.get_NID()

		print ('NID:',self.NID)
		print ('ADDR:',self.ADDR,'\n')

		self.hash_table={}
		self.Finger_Table=Finger_Table(160)
		self.successor_ADDR=None
		self._update_finger_table_flag=True
		self.time_interval=time_interval
		_thread.start_new_thread(self._update_finger_table_at_regular_time,(time_interval,))

	def _update_finger_table_at_regular_time(self,time_interval):
		"""
		按时更新finger表
		"""
		while 1:
			if self._update_finger_table_flag:
				self._update_finger_table()
			time.sleep(time_interval)

	def join(self,port):
		_ADDR=ADDR(('127.0.0.1',port))
		self._insert_to_finger_table(_ADDR)

		table=self._request_for_table(self.ADDR,_ADDR)
		if table==None: # 节点已失效
			return 
		for _ADDR2 in table:
			self._insert_to_finger_table(_ADDR2)

		time.sleep(self.time_interval*2)
		# 2 由网络规模决定
		self._request_kv_from_successor()

	def leave(self):
		# leave_num 为 0时 退出
		self._update_finger_table_flag=False
		self.leave_num=-self.Finger_Table.get_num()-1

		self._inform_node_in_finger_table_for_my_leave()
		self._deliver_kvs_to_successor()
		_thread.start_new_thread(self._leave_judge,())

	def insert(self,k,v,count=-40):
		# 设置 count 是为了便于调试，以及增强系统的鲁棒性
		# 转发一次，count+1，当count==0时，丢弃kv
		if count==0:
			print ("达到最大转发次数，丢弃纪录",k,v)
			return
		status,dest_ADDR=self._find_kv_location(k)
		if status==0:
			print ("将kv",k,v,'发送给',dest_ADDR)
			self._ask_to_insert(dest_ADDR,k,v,count)
		else:
			self._infrom_node_direct_insert(dest_ADDR,k,v)

	def look_up(self,k):
		self._look_up(self.ADDR,k)

	def _look_up(self,source_ADDR,k):
		# ...
		status,dest_ADDR=self._find_kv_location(k)
		if status==0:
			self._ask_to_look_up(source_ADDR,dest_ADDR,k)
		else:
			self._infrom_node_direct_look_up(source_ADDR,dest_ADDR,k)

			# v=self._direct_look_up(k)
			# if source_ADDR==self.ADDR:
			# 	print ("查询结果：",k,v)
			# else:
			# 	self._send_look_up_result(source_ADDR,k,v)

	def update(self,k,new_v,count=-40):
		if count==0:
			print ("达到最大转发次数，丢弃纪录",k,v)
		status,dest_ADDR=self._find_kv_location(k)

		if status==0:
			print ("将kv",k,v,'发送给',dest_ADDR)
			self._ask_to_update(dest_ADDR,k,v,count)
		else:
			self._infrom_node_direct_update(dest_ADDR,k,v)

	def _ask_to_update(self,dest_addr,k,v,count):
		self._ask_to_insert(dest_addr,k,v,count)

	def _infrom_node_direct_update(self,k,v):
		self._infrom_node_direct_insert(k,v)

	def mainloop(self):
		while 1:
			cmd=input()
			if cmd=='exit':
				sys.exit(0)

	def _leave_judge(self):
		while 1:
			if self.leave_num==0:
				print ('-leave-')
				os._exit(0)
	################################################

	def _create_recv_socket(self,port):
		pass

	def _wait_for_message(self):
		pass

	def _insert_to_finger_table(self,_ADDR):
		pass

	def _update_finger_table(self):
		pass

	def _inform_node_in_finger_table_for_my_leave(self):
		pass

	def _deliver_kvs_to_successor(self):
		pass

	def _find_kv_location(self,k):
		return 

	def _ask_to_insert(self,dest_addr,k,v,count):
		pass

	def _infrom_node_direct_insert(self,_ADDR,k,v):
		pass

	def _infrom_node_direct_look_up(self,source_ADDR,dest_ADDR,k):
		pass

	def _direct_insert(self,k,v):
		pass

	def _send_look_up_result(self,source_ADDR,k,v):
		pass

	def _ask_to_look_up(self,source_ADDR,dest_addr,k):
		pass

	def _direct_look_up(self,k):
		return None

	def _request_kv_from_successor(self):
		pass



class Chord_Node_1(Chord_Node_0):
	"""
	这部分主要负责 chord 中节点结构的维护 ： join 和 leave,
	按时更新finger表和后继节点
	"""
	def __init__(self,port,time_interval=5):
		Chord_Node_0.__init__(self,port,time_interval)
		self.finger_message_show_flag=True
		self.show_code_flag=False

	def _create_recv_socket(self,port):
		"""
		创建接受数据的socket
		"""
		self.recv_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		addr=('127.0.0.1',port)
		try:
			self.recv_socket.bind(addr)
		except:
			return None
		print ("设置监听")
		self.recv_socket.listen(10) # 40
		_thread.start_new_thread(self._wait_for_message,())
		return ADDR(addr)

	def _wait_for_message(self):
		"""
		--[核心]接受数据并进行处理--
			# 'table',source_addr		# 请求finger表
			# 'request kvs',source_addr	# 请求(k,v)s
			# 'kvs'	 					# 发送(k,v)s
			# 'leave',source_addr,successor_addr	# 节点离开
		"""
		while 1:
			conn,addr=self.recv_socket.accept()
			data=conn.recv(2048)
			message=Message()
			message.from_bytes(data)
			# 得到 消息的操作码 和 消息内容
			code=message.code
			mes=message.message
			if self.show_code_flag:
				print ("接受到消息",code)
			if code=='table':
				source_ADDR=ADDR()
				source_ADDR.from_string(mes[0])
				if self.finger_message_show_flag:
					print("<< finger",source_ADDR)
				# 响应请求，发送finger表
				self._send_my_finger_table(conn)
				# 将消息的源地址，插入到finger表中
				self._insert_to_finger_table(source_ADDR)

			elif code=='request kvs':
				source_ADDR=ADDR()
				source_ADDR.from_string(mes[0])
				# 响应请求，发送(k,v)s [这是由于新节点的加入，导致kv存储位置的变化]
				self._send_kvs(conn,source_ADDR)

			elif code=='kvs':
				# 响应请求，准备接受(k,v)s
				self._recv_kvs(conn)

			elif code=='leave':
				# 响应请求，处理源节点离开带来的变化
				source_ADDR=ADDR()
				source_ADDR.from_string(mes[0])
				print ('<< 收到离开消息，来自',source_ADDR)
				if len(mes)==2:
					successor_ADDR=ADDR()
					successor_ADDR.from_string(mes[1])
					# 将源节点的后继节点插入到finger表中源地址的位置
					self._update_finger_table_for_node_leave(source_ADDR,successor_ADDR)
				else:
					self._update_finger_table_for_node_leave(source_ADDR)
				conn.sendall(bytes('leave ok',encoding='utf-8'))

			conn.close()

	def _update_finger_table(self):
		"""
		更新finger表
		"""
		if self.finger_message_show_flag:
			print ("更新finger表")
		self.successor_ADDR=self.Finger_Table.get_successor()
		for _ADDR in self.Finger_Table.get_not_none():
			if self.finger_message_show_flag:
				print(">> finger",_ADDR)
			table=self._request_for_table(self.ADDR,_ADDR)
			if table==None: # 节点已失效
				continue 
			for _ADDR2 in table:
				self._insert_to_finger_table(_ADDR2)
		if self.finger_message_show_flag:
			print ()

	def _insert_to_finger_table(self,_ADDR):
		"""
		将_ADDR插入本地finger表中(包括插入和更新)
		"""
		NID=_ADDR.get_NID()
		index=_ADDR.get_index(self.NID)
		if index==None:
			return
		# 锁定finger表
		if self._update_finger_table_flag==False:
			return 
		# 写入
		if self.Finger_Table[index]==None:
			self.Finger_Table[index]=_ADDR
		# 替换
		# print ("for debug",_ADDR,_ADDR.get_d(self.NID),'|',self.Finger_Table[index],self.Finger_Table[index].get_d(self.NID))
		elif _ADDR.get_d(self.NID)<self.Finger_Table[index].get_d(self.NID):
			self.Finger_Table[index]=_ADDR

	def _request_for_table(self,source_ADDR,dest_ADDR):
		"""
		向dest_ADDR请求finger表
		"""
		mes=Message('table',[source_ADDR.to_string()])
		dest_addr=dest_ADDR.to_tuple()

		_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		try:
			_socket.connect(dest_addr)
		except:
			print ("此节点已失效",dest_addr)
			self._delete_addr_in_finger_table(dest_ADDR)
			_socket.close()
			return
		_socket.sendall(mes.to_bytes())
		data=_socket.recv(2048)

		table=Finger_Table()
		table.from_bytes(data)

		_socket.close()
		return table


	def _delete_addr_in_finger_table(self,_ADDR):
		index=_ADDR.get_index(self.NID)
		self.Finger_Table[index]=None

	def _send_my_finger_table(self,conn):
		"""
		响应请求，发送finger表
		"""
		_bytes=self.Finger_Table.to_bytes()
		conn.sendall(_bytes)

	def _inform_node_in_finger_table_for_my_leave(self):
		"""
		通知finger表中所有元素，我将离开
		"""
		for _ADDR in self.Finger_Table.get_not_none():
			self._inform_node_for_my_leave(_ADDR)

	def _inform_node_for_my_leave(self,_ADDR):
		"""
		通知目标地址，我将离开
		"""
		if self.successor_ADDR==None:
			mes=Message('leave',[self.ADDR.to_string()])
		else:
			mes=Message('leave',[self.ADDR.to_string(),self.successor_ADDR.to_string()])
		dest_addr=_ADDR.to_tuple()

		_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		_socket.connect(dest_addr)
		_socket.sendall(mes.to_bytes())
		
		data=_socket.recv(1024)
		if str(data,encoding='utf-8')=='leave ok':
			print ("<< 收到leave ok，leave_num+1")
			self.leave_num+=1
		_socket.close()

	def _deliver_kvs_to_successor(self):
		"""
		通知后继节点接受kvs[由于我将离开]
		"""
		if self.successor_ADDR == None:
			print ("无后继节点，leave_num+1")
			self.leave_num+=1
			return
		mes=Message('kvs',[])
		dest_addr=self.successor_ADDR.to_tuple()

		_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		try:
			_socket.connect(dest_addr)
			print ("已连接后继节点")
		except:
			print ("找不到后继节点，这意味着这个网络上已经没有第二个节点，信息将丢失")
			self.leave_num+=1
			_socket.close()
			return
		_socket.sendall(mes.to_bytes())
		print("将所有kv纪录，发送给后继结点",self.successor_ADDR)
		for kv in self.hash_table.items():
			kv_str=','.join(kv)
			_socket.sendall(bytes(kv_str,encoding='utf-8'))
		_socket.sendall(bytes('END',encoding='utf-8'))
		print("发送完毕，等待回复",self.successor_ADDR)
		data=_socket.recv(1024)
		if str(data,encoding='utf-8')=='ok':
			print ("<< kv ok，leave_num+1")
			self.leave_num+=1
		_socket.close()

	def _recv_kvs(self,conn):
		"""
		接受来自前驱节点的kvs,并写入到hash表中[由于前驱节点将离开]
		"""
		print("开始接受来自前驱节点的kv纪录")
		while 1:
			data=conn.recv(1024)
			_str=str(data,encoding='utf-8')
			if _str=='END':
				conn.sendall(bytes('ok',encoding='utf-8'))
				print("已收到所有kv纪录，你可以离开了，发送确认信息")
				return
			k,v=_str.split(',')
			self.hash_table[k]=v

	def _send_kvs(self,conn,_ADDR):
		"""
		将属于目标地址的kvs,发送给目标地址
		"""
		for kv in self.hash_table.items():
			_kv=','.join(kv)
			_bytes=bytes(_kv,encoding="utf-8")
			conn.sendall(_bytes)
		conn.sendall(bytes("END",encoding='utf-8'))

	def _update_finger_table_for_node_leave(self,source_ADDR,successor_ADDR=None):
		index=source_ADDR.get_index(self.NID)
		if index==None:
			return
		if successor_ADDR==None:
			self.Finger_Table[index]=None
		else:
			self._insert_to_finger_table(successor_ADDR)

	def _request_kv_from_successor(self):
		if self.successor_ADDR==None:
			return
		else:
			mes=Message('request kvs',[self.ADDR.to_string()])
		dest_addr=self.successor_ADDR.to_tuple()

		_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		_socket.connect(dest_addr)
		_socket.sendall(mes.to_bytes())
		
		while 1:
			data=_socket.recv(1024)
			if str(data,encoding='utf-8')=='END':
				break
			print (str(data,encoding='utf-8'))
			k,v=str(data,encoding='utf-8').split(',')
			self.hash_table[k]=v

		_socket.close()

	def mainloop(self):
		while 1:
			cmd=input()
			if cmd=='exit':
				self.recv_socket.close()
				#sys.exit(0)
				os._exit(0)
			elif cmd=='id':
				print(self.NID)
				print(self.ADDR)
			elif cmd=='finger':
				print (self.Finger_Table.to_string())
			elif cmd=='successor':
				print (self.successor_ADDR)
			# show finger message
			elif cmd=='shfmes':
				self.finger_message_show_flag=True
			elif cmd=='noshfmes':
				self.finger_message_show_flag=False
			elif cmd=='shcode':
				self.show_code_flag=True
			elif cmd=='noshcode':
				self.show_code_flag=False
			elif cmd=='index':
				port=int(input('port:'))
				addr=ADDR(('127.0.0.1',port))
				index=addr.get_index(self.NID)
				print (index)
			elif cmd=='join':
				port=int(input("已知的节点："))
				self.join(port)
			elif cmd=="leave":
				self.leave()
			else:
				print("未知的命令")



class Chord_Node_2(Chord_Node_1):

	def __init__(self,port,time_interval=5):
		Chord_Node_1.__init__(self,port,time_interval)

	def _wait_for_message(self):
		"""
		--[核心]接受数据并进行处理--
			# 'table',source_addr		# 请求finger表
			# 'request kvs',source_addr	# 请求(k,v)s
			# 'kvs'	 					# 发送(k,v)s
			# 'leave',source_addr,successor_addr	# 节点离开
		============================================================
			'insert',k,v 				# 询问-插入(k,v)的节点
			'insertkv',k,v				# 通知节点直接插入
			'look up',source_addr,k 	# 询问-存储(k,v)的节点
			'look up k',source_addr,k 	# 通知节点直接查询
			'kv',k,v 					# 将(k,v)发送给源
		"""
		while 1:
			conn,addr=self.recv_socket.accept()
			data=conn.recv(2048)
			message=Message()
			message.from_bytes(data)
			# 得到 消息的操作码 和 消息内容
			code=message.code
			mes=message.message

			if self.show_code_flag:
				print ("接受到消息",code)

			if code=='insert':
				k,v,count=mes
				count=int(count)
				print ("收到待插入的kv：",k,v)
				self.insert(k,v,count+1) 

			if code=='insertkv':
				k,v=mes
				self._direct_insert(k,v) 

			elif code=='look up':
				source_ADDR=ADDR()
				source_ADDR.from_string(mes[0])
				k=mes[1]
				self._look_up(source_ADDR,k)

			elif code=='look up k':
				source_ADDR=ADDR()
				source_ADDR.from_string(mes[0])
				k=mes[1]
				v=self._direct_look_up(k)
				if source_ADDR==self.ADDR:
					print ("查询结果：",k,v)
				else:
					self._send_look_up_result(source_ADDR,k,v)

			elif code=='kv':
				k,v=mes
				print ("查询结果：",k,v)

			elif code=='table':
				source_ADDR=ADDR()
				source_ADDR.from_string(mes[0])
				if self.finger_message_show_flag:
					print("<< finger",source_ADDR)
				# 响应请求，发送finger表
				self._send_my_finger_table(conn)
				# 将消息的源地址，插入到finger表中
				self._insert_to_finger_table(source_ADDR)

			elif code=='request kvs':
				source_ADDR=ADDR()
				source_ADDR.from_string(mes[0])
				# 响应请求，发送(k,v)s [这是由于新节点的加入，导致kv存储位置的变化]
				self._send_kvs(conn,source_ADDR)

			elif code=='kvs':
				# 响应请求，准备接受(k,v)s
				self._recv_kvs(conn)

			elif code=='leave':
				# 响应请求，处理源节点离开带来的变化
				source_ADDR=ADDR()
				source_ADDR.from_string(mes[0])
				print ('<< 收到离开消息，来自',source_ADDR)
				if len(mes)==2:
					successor_ADDR=ADDR()
					successor_ADDR.from_string(mes[1])
					# 将源节点的后继节点插入到finger表中源地址的位置
					self._update_finger_table_for_node_leave(source_ADDR,successor_ADDR)
				else:
					self._update_finger_table_for_node_leave(source_ADDR)
				conn.sendall(bytes('leave ok',encoding='utf-8'))

			conn.close()
	
	###################################################################
	
	def _find_kv_location(self,k):
		KID=sha1(k)
		# 当前节点与后继节点的距离
		if self.successor_ADDR.get_d(KID)<self.successor_ADDR.get_d(self.NID):
			# 通知 后继结点 插入 kv
			return 1,self.successor_ADDR
		else:
			# 通知 下个节点 继续寻找
			return 0,self._find_kv_precursor_in_my_table(k)

	def _find_kv_precursor_in_my_table(self,k):
		"""
		在当前finger表中查询，纪录kv的前驱结点[最靠近kv并且在kv之前的节点]
		"""
		KID=sha1(k)
		min_d=2**160
		precursor=None
		for _ADDR in self.Finger_Table.get_not_none():
			new_d=2**160-_ADDR.get_d(KID)
			if new_d<min_d:
				precursor=_ADDR
		return precursor

	def _ask_to_insert(self,dest_addr,k,v,count):
		message=Message('insert',[k,v,str(count)])
		
		_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		_socket.connect(dest_addr.to_tuple())
		_socket.sendall(message.to_bytes())
		_socket.close()

	def _infrom_node_direct_insert(self,_ADDR,k,v):
		message=Message('insertkv',[k,v])
		
		_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		_socket.connect(_ADDR.to_tuple())
		_socket.sendall(message.to_bytes())
		_socket.close()

	def _infrom_node_direct_look_up(self,source_ADDR,dest_ADDR,k):
		message=Message('look up k',[source_ADDR.to_string(),k])
		
		_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		_socket.connect(dest_ADDR.to_tuple())
		_socket.sendall(message.to_bytes())
		_socket.close()

	def _direct_insert(self,k,v):
		print ("在本地插入数据",k,v)
		self.hash_table[k]=v

	def _send_look_up_result(self,source_ADDR,k,v):
		message=Message('kv',[k,v])
		
		_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		_socket.connect(source_ADDR.to_tuple())
		_socket.sendall(message.to_bytes())
		_socket.close()

	def _ask_to_look_up(self,source_ADDR,dest_ADDR,k):
		message=Message('look up',[source_ADDR.to_string(),k])
		
		_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		_socket.connect(dest_ADDR.to_tuple())
		_socket.sendall(message.to_bytes())
		_socket.close()

	def _direct_look_up(self,k):
		a=self.hash_table.get(k)
		print ("在本地查询",k,"得到",a)
		if a==None:
			return 'no found'
		else:
			return self.hash_table.get(k)

	def mainloop(self):
		while 1:
			cmd=input()
			if cmd=='exit':
				self.recv_socket.close()
				#sys.exit(0)
				os._exit(0)
			elif cmd=='id':
				print(self.NID)
				print(self.ADDR)
			elif cmd=='finger':
				print (self.Finger_Table.to_string())
			elif cmd=='successor':
				print (self.successor_ADDR)
			# show finger message
			elif cmd=='shfmes':
				self.finger_message_show_flag=True
			elif cmd=='noshfmes':
				self.finger_message_show_flag=False
				print ("已取消关于finger表消息的提示")
			elif cmd=='shcode':
				self.show_code_flag=True
			elif cmd=='noshcode':
				self.show_code_flag=False
			elif cmd=='kv':
				print ('kv')
				for kv in self.hash_table.items():
					print (kv)
			elif cmd=='index':
				port=int(input('port:'))
				addr=ADDR(('127.0.0.1',port))
				index=addr.get_index(self.NID)
				print (index)
			elif cmd=='kid':
				data=input("k:")
				print (sha1(data))
			elif cmd=='join':
				port=int(input("已知的节点："))
				self.join(port)
			elif cmd=="leave":
				self.leave()
			elif cmd=='insert':
				k=input('k:')
				v=input('v:')
				self.insert(k, v)
			elif cmd=='update':
				k=input('k:')
				v=input('v:')
				self.update(k, v)
			elif cmd=='look up':
				k=input('k:')
				self.look_up(k)
			else:
				print("未知的命令")


if __name__=="__main__":
	port1=int(input("port:"))
	port2=input("已知的节点：")
	a=Chord_Node_2(port1)
	if port2:
		a.join(int(port2))
	a.mainloop()

"""
 有时会遇到如下问题
 	节点加入时，选择了错误的已知节点，致使未成功地其他节点被探测到，这时应重新选择已知节点，即重新运行join
"""


