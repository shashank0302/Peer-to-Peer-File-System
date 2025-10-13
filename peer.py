import asyncio
import hashlib
import json
import logging
import os
import struct
import sys
import time
import uuid
from typing import Dict, List, Optional, Tuple
from datetime import datetime

try:
    from .logging_config import init_peer_logger, get_peer_logger
except ImportError:
    from logging_config import init_peer_logger, get_peer_logger


logger = logging.getLogger(__name__)


CHUNK_SIZE = 1024 * 512  # 512KB


def _print_progress_bar(current: int, total: int, filename: str, speed_mbps: float, bar_length: int = 50) -> None:
	"""Print a progress bar for download operations."""
	if total == 0:
		return
	
	percentage = (current / total) * 100
	filled_length = int(bar_length * current // total)
	bar = '#' * filled_length + '-' * (bar_length - filled_length)
	
	# Clear the line and print progress
	sys.stdout.write(f'\r[{bar}] {percentage:5.1f}% ({current}/{total}) {filename} - {speed_mbps:.2f} MB/s')
	sys.stdout.flush()
	
	# Print newline when complete
	if current == total:
		sys.stdout.write('\n')
		sys.stdout.flush()


def _pack_message(obj: dict) -> bytes:
	data = json.dumps(obj).encode("utf-8")
	return struct.pack(">I", len(data)) + data


async def _read_message(reader: asyncio.StreamReader) -> dict:
	raw_len = await reader.readexactly(4)
	(msg_len,) = struct.unpack(">I", raw_len)
	raw = await reader.readexactly(msg_len)
	return json.loads(raw.decode("utf-8"))


class PeerServer:
	"""peer server serving file chunks to other peers."""

	def __init__(self) -> None:
		self._server: Optional[asyncio.base_events.Server] = None
		self._address: Optional[Tuple[str, int]] = None
		# filename -> absolute path on disk
		self._files: Dict[str, str] = {}
		# Initialize logging
		self.peer_id = str(uuid.uuid4())[:8]
		self.p2p_logger = init_peer_logger(self.peer_id)
		self.p2p_logger.log_session(f"Peer server initialized with ID: {self.peer_id}")

	def address(self) -> Optional[Tuple[str, int]]:
		return self._address

	def add_file(self, filename: str, local_path: str) -> None:
		self._files[filename] = local_path

	async def start(self, host: str, port: int) -> None:
		if self._server is not None:
			raise RuntimeError("Peer server already running")
		self._server = await asyncio.start_server(self._handle, host, port)
		sock = self._server.sockets[0]
		self._address = sock.getsockname()[:2]
		self.p2p_logger.log_session(f"Peer server started on {self._address}")

	async def stop(self) -> None:
		self.p2p_logger.log_session(f"Peer server stopping at {datetime.now()}")
		server, self._server = self._server, None
		if server is not None:
			server.close()
			await server.wait_closed()
		self._address = None
		self._files.clear()
		self.p2p_logger.log_session("Peer server stopped - session ended")

	async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
		try:
			while not reader.at_eof():
				msg = await _read_message(reader)
				if msg.get("type") == "PEER_REQUEST_CHUNK":
					filename = msg["filename"]
					chunknum = int(msg["chunknum"])
					peer_addr = writer.get_extra_info("peername")
					start_time = time.time()
					
					path = self._files[filename]
					with open(path, "rb") as f:
						f.seek(chunknum * CHUNK_SIZE)
						data = f.read(CHUNK_SIZE)
					
					digest = hashlib.sha256(data).hexdigest()
					duration_ms = (time.time() - start_time) * 1000
					
					writer.write(
						_pack_message(
							{
								"type": "PEER_REPLY_CHUNK",
								"filename": filename,
								"chunknum": chunknum,
								"data": list(data),  # JSON-friendly bytes
								"digest": digest,
							}
						)
					)
					await writer.drain()
					
					# Log chunk upload
					self.p2p_logger.log_chunk_operation(
						"UPLOAD", filename, chunknum, 
						f"{peer_addr[0]}:{peer_addr[1]}", 
						len(data), duration_ms
					)
				elif msg.get("type") == "PEER_PING_PONG":
					writer.write(_pack_message(msg))
					await writer.drain()
				else:
					logger.warning("Unknown message: %s", msg.get("type"))
		except (asyncio.IncompleteReadError, ConnectionError, RuntimeError):
			pass
		finally:
			try:
				writer.close()
				await writer.wait_closed()
			except Exception:
				pass


class PeerClient:
	"""peer: connect to tracker, publish, list, download."""

	def __init__(self) -> None:
		self._tracker_reader: Optional[asyncio.StreamReader] = None
		self._tracker_writer: Optional[asyncio.StreamWriter] = None
		self._server = PeerServer()
		# map filename -> local path
		self._local_files: Dict[str, str] = {}
		# Use server's logger
		self.p2p_logger = self._server.p2p_logger

	def server_address(self) -> Optional[Tuple[str, int]]:
		return self._server.address()

	async def start(self, host: str = "127.0.0.1", port: int = 0) -> None:
		await self._server.start(host, port)

	async def stop(self) -> None:
		if self._tracker_writer is not None and not self._tracker_writer.is_closing():
			self._tracker_writer.close()
			await self._tracker_writer.wait_closed()
		self._tracker_reader = None
		self._tracker_writer = None
		await self._server.stop()

	async def connect_tracker(self, host: str, port: int) -> None:
		try:
			self._tracker_reader, self._tracker_writer = await asyncio.open_connection(host, port)
			await self._write_tracker(
				{
					"type": "REQUEST_REGISTER",
					"address": list(self.server_address()),
				}
			)
			await _read_message(self._tracker_reader)
			self.p2p_logger.log_network_event(f"Connected to tracker at {host}:{port}")
		except ConnectionRefusedError:
			raise ConnectionError(f"Could not connect to tracker at {host}:{port} - connection refused")
		except asyncio.TimeoutError:
			raise ConnectionError(f"Connection to tracker at {host}:{port} timed out")
		except Exception as e:
			raise ConnectionError(f"Failed to connect to tracker at {host}:{port}: {e}")

	async def publish(self, abs_path: str) -> None:
		if not os.path.isfile(abs_path):
			raise FileNotFoundError(abs_path)
		filename = os.path.basename(abs_path)
		file_size = os.path.getsize(abs_path)
		total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
		self._local_files[filename] = abs_path
		self._server.add_file(filename, abs_path)
		await self._write_tracker(
			{
				"type": "REQUEST_PUBLISH",
				"filename": filename,
				"fileinfo": {"size": file_size, "total_chunknum": total_chunks},
			}
		)
		await _read_message(self._tracker_reader)
		self.p2p_logger.log_network_event(f"Published file: {filename} ({file_size} bytes, {total_chunks} chunks)")

	async def list_files(self) -> Dict[str, Dict[str, int]]:
		await self._write_tracker({"type": "REQUEST_FILE_LIST"})
		msg = await _read_message(self._tracker_reader)
		return msg["file_list"]

	async def download(self, filename: str, dest_path: str) -> None:
		self.p2p_logger.log_network_event(f"Starting download: {filename} -> {dest_path}")
		download_start = time.time()
		
		# query chunk info
		await self._write_tracker({"type": "REQUEST_FILE_LOCATION", "filename": filename})
		reply = await _read_message(self._tracker_reader)
		fileinfo = reply["fileinfo"]
		chunkinfo = reply["chunkinfo"]  # {addr_json: [chunknums]}

		total = int(fileinfo["total_chunknum"])
		file_size = int(fileinfo["size"])
		self.p2p_logger.log_performance("DOWNLOAD_START", {
			"filename": filename, 
			"total_chunks": total, 
			"file_size_bytes": file_size,
			"available_peers": len(chunkinfo)
		})
		
		# Print initial progress bar
		print(f"\nStarting download: {filename} ({file_size:,} bytes, {total} chunks)")
		_print_progress_bar(0, total, filename, 0.0)
		
		# rareness ordering
		remaining = list(range(total))

		# temp file write
		with open(dest_path + ".temp", "wb") as out:
			# ensure file pre-sized (optional)
			out.truncate(total * CHUNK_SIZE)
			completed_chunks = 0
			while remaining:
				# build map chunk -> owners
				owners_map: Dict[int, List[str]] = {c: [] for c in remaining}
				for addr_json, chunks in chunkinfo.items():
					for c in chunks:
						if c in owners_map:
							owners_map[c].append(addr_json)
					# test RTT for available peers
				rtt: Dict[str, float] = {}
				for addr_json in {a for lst in owners_map.values() for a in lst}:
					h, p = json.loads(addr_json)
					try:
						reader, writer = await asyncio.open_connection(h, p)
						start = time.time()
						writer.write(_pack_message({"type": "PEER_PING_PONG", "peer_address": addr_json}))
						await writer.drain()
						await _read_message(reader)
						rtt[addr_json] = time.time() - start
						writer.close()
						await writer.wait_closed()
					except Exception:
						rtt[addr_json] = float("inf")

				# pick next chunk: rarest first; tiebreaker fastest peer
				remaining.sort(key=lambda c: len(owners_map[c]))
				chunk = remaining.pop(0)
				c_owners = owners_map[chunk]
				if not c_owners:
					raise EOFError("No owners for chunk")
				best_peer = min(c_owners, key=lambda a: rtt.get(a, float("inf")))
				
				# Log rarest-first strategy
				chunk_counts = {c: len(owners_map[c]) for c in remaining[:5]}  # Log top 5 rarest
				self.p2p_logger.log_rarest_first_strategy(filename, chunk_counts, chunk)
				
				h, p = json.loads(best_peer)
				# fetch chunk
				chunk_start = time.time()
				try:
					reader, writer = await asyncio.open_connection(h, p)
					writer.write(_pack_message({"type": "PEER_REQUEST_CHUNK", "filename": filename, "chunknum": chunk}))
					await writer.drain()
					reply = await _read_message(reader)
					data = bytes(reply["data"])  # list->bytes
					digest = reply["digest"]
					chunk_duration = (time.time() - chunk_start) * 1000
					writer.close(); await writer.wait_closed()
					
					if hashlib.sha256(data).hexdigest() != digest:
						self.p2p_logger.log_chunk_operation("CORRUPT", filename, chunk, f"{h}:{p}", len(data), chunk_duration)
						remaining.append(chunk)  # Retry
						continue
				except (ConnectionError, asyncio.TimeoutError, KeyError, json.JSONDecodeError) as e:
					self.p2p_logger.log_chunk_operation("ERROR", filename, chunk, f"{h}:{p}", 0, 0)
					remaining.append(chunk)  # Retry from different peer
					continue
				
				# write chunk
				out.seek(chunk * CHUNK_SIZE)
				out.write(data)
				out.flush()
				writer.close(); await writer.wait_closed()
				
				# Log chunk download
				self.p2p_logger.log_chunk_operation("DOWNLOAD", filename, chunk, f"{h}:{p}", len(data), chunk_duration)
				
				# register chunk possession
				await self._write_tracker({"type": "REQUEST_CHUNK_REGISTER", "filename": filename, "chunknum": chunk})
				
				# Update progress
				completed_chunks += 1
				elapsed = time.time() - download_start
				speed_mbps = (completed_chunks * CHUNK_SIZE) / (elapsed * 1024 * 1024) if elapsed > 0 else 0
				self.p2p_logger.log_download_progress(filename, completed_chunks, total, speed_mbps)
				
				# Print progress bar
				_print_progress_bar(completed_chunks, total, filename, speed_mbps)

		# finalize
		os.replace(dest_path + ".temp", dest_path)
		# add to local map and serve
		self._local_files[filename] = dest_path
		self._server.add_file(filename, dest_path)
		
		# Log completion
		total_duration = time.time() - download_start
		avg_speed_mbps = file_size / (total_duration * 1024 * 1024) if total_duration > 0 else 0
		self.p2p_logger.log_performance("DOWNLOAD_COMPLETE", {
			"filename": filename,
			"total_duration_seconds": total_duration,
			"average_speed_mbps": avg_speed_mbps,
			"chunks_downloaded": completed_chunks
		})
		self.p2p_logger.log_network_event(f"Download completed: {filename}")

	async def _write_tracker(self, obj: dict) -> None:
		assert self._tracker_writer is not None
		self._tracker_writer.write(_pack_message(obj))
		await self._tracker_writer.drain()


