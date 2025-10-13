import asyncio
import json
import logging
import struct
from typing import Dict, List, Tuple, Optional
from datetime import datetime

try:
    from .logging_config import init_tracker_logger
except ImportError:
    from logging_config import init_tracker_logger


logger = logging.getLogger(__name__)


def _pack_message(obj: dict) -> bytes:

	data = json.dumps(obj).encode("utf-8")
	return struct.pack(">I", len(data)) + data


async def _read_message(reader: asyncio.StreamReader) -> dict:

	raw_len = await reader.readexactly(4)
	(msg_len,) = struct.unpack(">I", raw_len)
	raw = await reader.readexactly(msg_len)
	return json.loads(raw.decode("utf-8"))


class Tracker:
	"""tracker: keeps peers, file list, and per-chunk ownership.

	Protocol (JSON over length-prefixed TCP):
	- REQUEST_REGISTER { address: [host, port] }
	- REQUEST_PUBLISH { filename, fileinfo: { size, total_chunknum } }
	- REQUEST_FILE_LIST {}
	- REQUEST_FILE_LOCATION { filename }
	- REQUEST_CHUNK_REGISTER { filename, chunknum }
	"""

	def __init__(self) -> None:
		self._server: Optional[asyncio.base_events.Server] = None
		self._server_address: Optional[Tuple[str, int]] = None
		# writer -> peer_address_json
		self._peers: Dict[asyncio.StreamWriter, str] = {}
		# filename -> { size: int, total_chunknum: int }
		self._file_list: Dict[str, Dict[str, int]] = {}
		# filename -> { peer_address_json -> List[int] }
		self._chunkinfo: Dict[str, Dict[str, List[int]]] = {}
		# Initialize logging
		self.p2p_logger = init_tracker_logger()
		self.p2p_logger.log_session(f"Tracker initialized at {datetime.now()}")

	def address(self) -> Optional[Tuple[str, int]]:
		return self._server_address

	def peers(self) -> List[str]:
		return list(self._peers.values())

	def file_list(self) -> Dict[str, Dict[str, int]]:
		return dict(self._file_list)

	def chunkinfo(self) -> Dict[str, Dict[str, List[int]]]:
		return {k: dict(v) for k, v in self._chunkinfo.items()}

	async def start(self, host: str, port: int) -> None:
		if self._server is not None:
			raise RuntimeError("Tracker already running")
		self._server = await asyncio.start_server(self._handle_conn, host, port)
		sock = self._server.sockets[0]
		self._server_address = sock.getsockname()[:2]
		
		#log commands to tracker.log
		logger.info("Tracker listening on %s", self._server_address)
		self.p2p_logger.log_session(f"Tracker started on {host}:{port} -> {self._server_address}")

	async def stop(self) -> None:
		#log commands to tracker.log
		self.p2p_logger.log_session(f"Tracker stopping at {datetime.now()}")
		
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
		self._file_list.clear()
		self._chunkinfo.clear()
		self._server_address = None

		#log commands to tracker.log
		self.p2p_logger.log_session("Tracker stopped - session ended")

	async def _handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
		
		peername = writer.get_extra_info("peername")

		#log commands to tracker.log
		logger.info("New connection from %s", peername)
		self.p2p_logger.log_network_event(f"New connection from {peername}")

		self._peers[writer] = None
		
		try:
			while not reader.at_eof():
				msg = await _read_message(reader)
				type = msg.get("type")
				if type == "REQUEST_REGISTER":
					
					addr = json.dumps(msg["address"])  # ensure str key
					self._peers[writer] = addr
					await self._write(writer, {"type": "REPLY_REGISTER"})
					
					#log commands to tracker.log
					self.p2p_logger.log_network_event(f"Peer registered: {addr}")
				
				elif type == "REQUEST_PUBLISH":

					filename = msg["filename"]
					fileinfo = msg["fileinfo"]

					if filename in self._file_list:
						await self._write(writer, {"type": "REPLY_PUBLISH", "result": False})
					
						#log commands to tracker.log
						self.p2p_logger.log_network_event(f"File publish rejected - already exists: {filename}")
						continue
					
					self._file_list[filename] = {
						"size": int(fileinfo["size"]),
						"total_chunknum": int(fileinfo["total_chunknum"]),
					}
					# seed with publisher owning all chunks
					owner = self._peers.get(writer)

					if owner is None:
						owner = json.dumps([peername[0], peername[1]])

					self._chunkinfo[filename] = {owner: list(range(self._file_list[filename]["total_chunknum"]))}
					await self._write(writer, {"type": "REPLY_PUBLISH", "result": True})
                    
					#log commands to tracker.log
					self.p2p_logger.log_network_event(f"File published: {filename} by {owner} ({fileinfo['size']} bytes, {fileinfo['total_chunknum']} chunks)")
				
				elif type == "REQUEST_FILE_LIST":
					await self._write(writer, {"type": "REPLY_FILE_LIST", "file_list": self._file_list})

				elif type == "REQUEST_FILE_LOCATION":
					filename = msg["filename"]
					
					await self._write(
						writer,
						{
							"type": "REPLY_FILE_LOCATION",
							"fileinfo": self._file_list[filename],
							"chunkinfo": self._chunkinfo[filename],
						},
					)

				elif type == "REQUEST_CHUNK_REGISTER":
					filename = msg["filename"]
					chunknum = int(msg["chunknum"])
					addr = self._peers.get(writer)

					if addr is None:
						addr = json.dumps([peername[0], peername[1]])

					owners = self._chunkinfo.get(filename)

					if owners is None:
						continue

					if addr in owners:
						if chunknum not in owners[addr]:
							owners[addr].append(chunknum)

							#log commands to tracker.log
							self.p2p_logger.log_chunk_operation("REGISTER", filename, chunknum, addr)

					else:
						owners[addr] = [chunknum]

						#log commands to tracker.log
						self.p2p_logger.log_chunk_operation("REGISTER", filename, chunknum, addr)

				else:
					#log commands to tracker.log
					logger.warning("Unknown request type: %s", type)
		except (asyncio.IncompleteReadError, ConnectionError, RuntimeError):
			pass
		
		finally:
			addr = self._peers.pop(writer, None)
			if addr:
				self.p2p_logger.log_network_event(f"Peer disconnected: {addr}")
			# remove ownership of this peer; delete files with no owners
			to_delete = []
			for filename, owners in self._chunkinfo.items():
				if addr in owners:
					del owners[addr]
				if len(owners) == 0:
					to_delete.append(filename)
			for fn in to_delete:
				self._chunkinfo.pop(fn, None)
				self._file_list.pop(fn, None)
				self.p2p_logger.log_network_event(f"File removed - no owners: {fn}")
			try:
				writer.close()
				await writer.wait_closed()
			except Exception:
				pass

	async def _write(self, writer: asyncio.StreamWriter, obj: dict) -> None:
		writer.write(_pack_message(obj))
		await writer.drain()


