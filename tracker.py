import asyncio
import json
import struct
from typing import Dict, List, Tuple, Optional

def pack_message(obj: dict) -> bytes:
	#pack the message into a bytes object
	data = json.dumps(obj).encode("utf-8")
	return struct.pack(">I", len(data)) + data

async def read_message(reader: asyncio.StreamReader) -> dict:
	#read the message from the reader and return the message as a dictionary
	raw_len = await reader.readexactly(4)
	(msg_len,) = struct.unpack(">I", raw_len)
	raw = await reader.readexactly(msg_len)
	return json.loads(raw.decode("utf-8"))

class Tracker:

	#Tracker manages peer registry, file metadata with integrity hashes, and chunk ownership.
	#Tracker has functions: REGISTER, PUBLISH, FILE_LIST, FILE_LOCATION, CHUNK_REGISTER.

	def __init__(self):
		#initialize the tracker
		self._server: Optional[asyncio.base_events.Server] = None
		self.serverAddress: Optional[Tuple[str, int]] = None
		self._peers: Dict[asyncio.StreamWriter, str] = {}
		self.fileList: Dict[str, Dict] = {}
		self.chunkInfo: Dict[str, Dict[str, List[int]]] = {}

	def address(self) -> Optional[Tuple[str, int]]:
		#return the address of the tracker
		return self.serverAddress

	def peers(self) -> List[str]:
		#return the list of peers
		return list(self._peers.values())

	def file_list(self) -> Dict[str, Dict[str, int]]:
		#return the list of files
		return dict(self.fileList)

	def chunkinfo(self) -> Dict[str, Dict[str, List[int]]]:
		#return the list of chunks
		return {k: dict(v) for k, v in self.chunkInfo.items()}

	async def start(self, host: str, port: int):
		#start the tracker
		if self._server is not None:
			raise RuntimeError("Tracker already running")
		self._server = await asyncio.start_server(self.handle_conn, host, port)
		sock = self._server.sockets[0]
		self.serverAddress = sock.getsockname()[:2]

	async def stop(self):
		#stop the tracker
		server, self._server = self._server, None
		if server is not None:
			server.close()
			await server.wait_closed()
		for writer in list(self._peers.keys()):
			try:
				writer.close()
				await writer.wait_closed()
			except Exception:
				pass
		self._peers.clear()
		self.fileList.clear()
		self.chunkInfo.clear()
		self.serverAddress = None
		
	async def write(self, writer: asyncio.StreamWriter, obj: dict):
		#write the message to the writer
		writer.write(pack_message(obj))
		await writer.drain()

	async def handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
		#handle the connection from the peer
		peername = writer.get_extra_info("peername")
		self._peers[writer] = None
		
		try:
			while not reader.at_eof():
				msg = await read_message(reader)
				type = msg.get("type")
				if type == "REQUEST_REGISTER":
					#register the peer to the tracker
					addr = json.dumps(msg["address"])
					self._peers[writer] = addr
					await self.write(writer, {"type": "REPLY_REGISTER"})
				
				elif type == "REQUEST_PUBLISH":
					#publish the file to the tracker by the peer
					filename = msg["filename"]
					fileinfo = msg["fileinfo"]

					#get the original hashes of the file
					org_hashes = msg.get("org_hashes", [])

					if filename in self.fileList:
						await self.write(writer, {"type": "REPLY_PUBLISH", "result": False})
						continue
					
					total_chunks = int(fileinfo["total_chunknum"])
					
					#check if the original hashes count matches the total chunks
					if len(org_hashes) != total_chunks:
						await self.write(writer, {
							"type": "REPLY_PUBLISH",
							"result": False,
							"error": f"org hashes count mismatch: expected {total_chunks}, got {len(org_hashes)}"
						})
						continue
					
					self.fileList[filename] = {
						"size": int(fileinfo["size"]),
						"total_chunknum": total_chunks,
						"org_hashes": org_hashes,
					}
					owner = self._peers.get(writer)

					if owner is None:
						owner = json.dumps([peername[0], peername[1]])

					self.chunkInfo[filename] = {owner: list(range(total_chunks))}
					await self.write(writer, {"type": "REPLY_PUBLISH", "result": True})
				
				elif type == "REQUEST_FILE_LIST":
					#return the list of files to the peer
					await self.write(writer, {"type": "REPLY_FILE_LIST", "file_list": self.fileList})

				elif type == "REQUEST_FILE_LOCATION":
					#return the location of the file to the peer
					filename = msg["filename"]
					#get the chunk info of the file
					chunkinfo = self.chunkInfo[filename]
					fileinfo = self.fileList[filename]
					
					await self.write(writer, {
						"type": "REPLY_FILE_LOCATION",
						"fileinfo": fileinfo,
						"chunkinfo": chunkinfo,
					})

				elif type == "REQUEST_CHUNK_REGISTER":
					#register the chunk to the tracker
					filename = msg["filename"]
					chunknum = int(msg["chunknum"])
					addr = self._peers.get(writer)

					if addr is None:
						addr = json.dumps([peername[0], peername[1]])

					owners = self.chunkInfo.get(filename)

					if owners is None:
						continue

					if addr in owners:
						if chunknum not in owners[addr]:
							owners[addr].append(chunknum)
					else:
						owners[addr] = [chunknum]
					
		except (asyncio.IncompleteReadError, ConnectionError, RuntimeError):
			#any error in the connection
			pass
		
		finally:
			#pop the peer from the tracker
			addr = self._peers.pop(writer, None)
			to_delete = []
			for filename, owners in self.chunkInfo.items():
				if addr in owners:
					del owners[addr]
				if len(owners) == 0:
					to_delete.append(filename)
			for fn in to_delete:
				self.chunkInfo.pop(fn, None)
				self.fileList.pop(fn, None)
			try:
				writer.close()
				await writer.wait_closed()
			except Exception:
				pass