import asyncio
import json
import struct
from typing import Dict, List, Tuple, Optional


def pack_message(obj: dict) -> bytes:
	data = json.dumps(obj).encode("utf-8")
	return struct.pack(">I", len(data)) + data


async def read_message(reader: asyncio.StreamReader) -> dict:
	raw_len = await reader.readexactly(4)
	(msg_len,) = struct.unpack(">I", raw_len)
	raw = await reader.readexactly(msg_len)
	return json.loads(raw.decode("utf-8"))


class Tracker:
	"""tracker: keeps peers, file list, and per-chunk ownership.
	Protocol (JSON over length-prefixed TCP):
	- REQUEST_REGISTER { address: [host, port] }
	- REQUEST_PUBLISH { filename, fileinfo: { size, total_chunknum } }
	- REQUESTfileList {}
	- REQUEST_FILE_LOCATION { filename }
	- REQUEST_CHUNK_REGISTER { filename, chunknum }
	"""

	def __init__(self):
		self._server: Optional[asyncio.base_events.Server] = None
		self.serverAddress: Optional[Tuple[str, int]] = None
		
		# writer -> peer_address_json
		self._peers: Dict[asyncio.StreamWriter, str] = {}
		
		# filename -> { size: int, total_chunknum: int, org_hashes: List[str] }
		# org_hashes contains the authoritative SHA256 hash for each chunk
		self.fileList: Dict[str, Dict] = {}

		# filename -> { peer_address_json -> List[int] }
		self.chunkInfo: Dict[str, Dict[str, List[int]]] = {}

	def address(self) -> Optional[Tuple[str, int]]:
		return self.serverAddress

	def peers(self) -> List[str]:
		return list(self._peers.values())

	def file_list(self) -> Dict[str, Dict[str, int]]:
		return dict(self.fileList)

	def chunkinfo(self) -> Dict[str, Dict[str, List[int]]]:
		return {k: dict(v) for k, v in self.chunkInfo.items()}

	async def start(self, host: str, port: int):
		if self._server is not None:
			raise RuntimeError("Tracker already running")
		self._server = await asyncio.start_server(self.handle_conn, host, port)
		sock = self._server.sockets[0]
		self.serverAddress = sock.getsockname()[:2]

	async def stop(self):
		server, self._server = self._server, None
		if server is not None:
			server.close()
			await server.wait_closed()
		# disconnect cleanup
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
		writer.write(pack_message(obj))
		await writer.drain()

	async def handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
		peername = writer.get_extra_info("peername")
		self._peers[writer] = None
		
		try:
			while not reader.at_eof():
				msg = await read_message(reader)
				type = msg.get("type")
				if type == "REQUEST_REGISTER":
					addr = json.dumps(msg["address"])  # ensure str key
					self._peers[writer] = addr
					await self.write(writer, {"type": "REPLY_REGISTER"})
				
				elif type == "REQUEST_PUBLISH":
					filename = msg["filename"]
					fileinfo = msg["fileinfo"]
					org_hashes = msg.get("org_hashes", [])

					if filename in self.fileList:
						await self.write(writer, {"type": "REPLY_PUBLISH", "result": False})
						continue
					
					# Validate that org_hashes count matches total_chunknum
					total_chunks = int(fileinfo["total_chunknum"])
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
						"org_hashes": org_hashes,  # Store authoritative hashes
					}
					# seed with publisher owning all chunks
					owner = self._peers.get(writer)

					if owner is None:
						owner = json.dumps([peername[0], peername[1]])

					self.chunkInfo[filename] = {owner: list(range(total_chunks))}
					await self.write(writer, {"type": "REPLY_PUBLISH", "result": True})
				
				elif type == "REQUEST_FILE_LIST":
					await self.write(writer, {"type": "REPLY_FILE_LIST", "file_list": self.fileList})

				elif type == "REQUEST_FILE_LOCATION":
					filename = msg["filename"]
					chunkinfo = self.chunkInfo[filename]
					fileinfo = self.fileList[filename]
					
					await self.write(
						writer,
						{
							"type": "REPLY_FILE_LOCATION",
							"fileinfo": fileinfo,  # Includes org_hashes
							"chunkinfo": chunkinfo,
						},
					)

				elif type == "REQUEST_CHUNK_REGISTER":
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
			pass
		
		finally:
			addr = self._peers.pop(writer, None)
			# remove ownership of this peer; delete files with no owners
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