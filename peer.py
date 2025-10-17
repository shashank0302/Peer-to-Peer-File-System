import asyncio
import hashlib
import json
import os
import random
import struct
import sys
import time
import uuid
from typing import Dict, List, Optional, Tuple, Set
import socket
from collections import defaultdict


CHUNK_SIZE = 1024 * 512  # 512KB


def print_progress(current: int, total: int, filename: str, speed_mbps: float, bar_length: int = 50) -> None:
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


def pack_message(obj: dict) -> bytes:
	data = json.dumps(obj).encode("utf-8")
	return struct.pack(">I", len(data)) + data


async def read_message(reader: asyncio.StreamReader) -> dict:
	raw_len = await reader.readexactly(4)
	(msg_len,) = struct.unpack(">I", raw_len)
	raw = await reader.readexactly(msg_len)
	return json.loads(raw.decode("utf-8"))


class PeerServer:
	"""Handles incoming peer requests for file chunks."""

	def __init__(self) -> None:
		# filename -> absolute path on disk
		self._files: Dict[str, str] = {}
		# Initialize peer ID
		self.peer_id = str(uuid.uuid4())[:8]

	def add_file(self, filename: str, local_path: str) -> None:
		self._files[filename] = local_path
	
	def clear_files(self) -> None:
		self._files.clear()

	async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
		try:
			while not reader.at_eof():
				msg = await read_message(reader)
				if msg.get("type") == "PEER_REQUEST_CHUNK":
					filename = msg["filename"]
					chunknum = int(msg["chunknum"])
					
					# Check if we have this file
					if filename not in self._files:
						# Send error response
						writer.write(pack_message({
							"type": "PEER_REPLY_CHUNK",
							"filename": filename,
							"chunknum": chunknum,
							"data": [],  # Empty data indicates error
							"digest": "",
							"error": f"File {filename} not available on this peer"
						}))
						await writer.drain()
						continue
					
					path = self._files[filename]

					# Non-blocking file read using asyncio.to_thread
					def read_chunk():
						with open(path, "rb") as f:
							f.seek(chunknum * CHUNK_SIZE)
							return f.read(CHUNK_SIZE)
					
					data = await asyncio.to_thread(read_chunk)
					
					# Non-blocking hash calculation
					def calculate_hash():
						return hashlib.sha256(data).hexdigest()

					digest = await asyncio.to_thread(calculate_hash)
					
					writer.write(
						pack_message(
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
				
				elif msg.get("type") == "PEER_PING_PONG":
					writer.write(pack_message(msg))
					await writer.drain()

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
		self.tracker_reader: Optional[asyncio.StreamReader] = None
		self._tracker_writer: Optional[asyncio.StreamWriter] = None
		self.handler = PeerServer()
		# Use handler's peer_id
		self.peer_id = self.handler.peer_id
		# Server management
		self._server: Optional[asyncio.base_events.Server] = None
		self._address: Optional[Tuple[str, int]] = None

	async def start(self, host: str = "0.0.0.0", port: int = 0) -> None:
		if self._server is not None:
			raise RuntimeError("Peer server already running")
		self._server = await asyncio.start_server(self.handler.handle, host, port)
		sock = self._server.sockets[0]
		self._address = sock.getsockname()[:2]

	async def stop(self) -> None:
		if self._tracker_writer is not None and not self._tracker_writer.is_closing():
			self._tracker_writer.close()
			await self._tracker_writer.wait_closed()
		self.tracker_reader = None
		self._tracker_writer = None
		
		server, self._server = self._server, None
		if server is not None:
			server.close()
			await server.wait_closed()
		self._address = None
		self.handler.clear_files()

	async def connect_tracker(self, host: str, port: int, peer_host: str = "0.0.0.0") -> None:
		# Check if already connected
		if self._tracker_writer is not None and not self._tracker_writer.is_closing():
			raise RuntimeError("Already connected to tracker. Disconnect first or restart peer.")
		
		try:
			# Restart peer server with correct host
			server, self._server = self._server, None
			if server is not None:
				server.close()
				await server.wait_closed()
			self._address = None
			self.handler.clear_files()
			
			# Start server with new host
			self._server = await asyncio.start_server(self.handler.handle, peer_host, 0)
			sock = self._server.sockets[0]
			self._address = sock.getsockname()[:2]
			
			# Get the actual external IP for registration
			s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			s.connect(("8.8.8.8", 80))
			external_ip = s.getsockname()[0]
			s.close()
			
			self.tracker_reader, self._tracker_writer = await asyncio.open_connection(host, port)
			await self.write_tracker(
				{
					"type": "REQUEST_REGISTER",
					"address": [external_ip, self._address[1]],  # Use external IP
				}
			)
			await read_message(self.tracker_reader)
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
		self.handler.add_file(filename, abs_path)
		await self.write_tracker(
			{
				"type": "REQUEST_PUBLISH",
				"filename": filename,
				"fileinfo": {"size": file_size, "total_chunknum": total_chunks},
			}
		)
		await read_message(self.tracker_reader)

	async def list_files(self) -> Dict[str, Dict[str, int]]:
		await self.write_tracker({"type": "REQUEST_FILE_LIST"})
		msg = await read_message(self.tracker_reader)
		return msg["file_list"]

	async def measure_peer_rtt(self, addr_json: str) -> float:
		"""Measure round-trip time to a peer using PEER_PING_PONG"""
		try:
			h, p = json.loads(addr_json)
			start = time.time()
			reader, writer = await asyncio.open_connection(h, p)
			writer.write(pack_message({"type": "PEER_PING_PONG"}))
			await writer.drain()
			await read_message(reader)
			rtt = time.time() - start
			writer.close()
			await writer.wait_closed()
			return rtt
		except Exception:
			return float('inf')  # Unreachable peer

	async def download(self, filename: str, dest_path: str) -> None:
		download_start = time.time()
		
		# Query chunk info
		await self.write_tracker({"type": "REQUEST_FILE_LOCATION", "filename": filename})
		reply = await read_message(self.tracker_reader)
		fileinfo = reply["fileinfo"]
		chunkinfo = reply["chunkinfo"]  # {addr_json: [chunknums]}

		total = int(fileinfo["total_chunknum"])
		file_size = int(fileinfo["size"])
		
		# Create demo log file
		chunkLog = open("ChunkDownload.log", "a")
		chunkLog.write(f"\n{'='*60}\n")
		chunkLog.write(f"Download started: {filename} at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
		chunkLog.write(f"Peer ID: {self.peer_id}\n")
		chunkLog.write(f"File size: {file_size:,} bytes, {total} chunks\n")
		chunkLog.write(f"{'='*60}\n\n")
		chunkLog.flush()
		
		# Print initial progress bar
		print(f"\nStarting parallel download: {filename} ({file_size:,} bytes, {total} chunks)")
		print_progress(0, total, filename, 0.0)
		
		# Measure RTT to all unique peers
		unique_peers = set(chunkinfo.keys())
		
		print(f"\nMeasuring RTT to {len(unique_peers)} peers...")
		chunkLog.write("=== RTT Measurements ===\n")
		peer_rtt_cache = {}
		for peer_addr in unique_peers:
			rtt = await self.measure_peer_rtt(peer_addr)
			peer_rtt_cache[peer_addr] = rtt
			if rtt < float('inf'):
				print(f"  Peer {peer_addr}: {rtt*1000:.2f}ms")
				chunkLog.write(f"Peer {peer_addr}: {rtt*1000:.2f}ms\n")
			else:
				print(f"  Peer {peer_addr}: UNREACHABLE")
				chunkLog.write(f"Peer {peer_addr}: UNREACHABLE\n")
		print()
		chunkLog.write("\n=== Chunk Downloads ===\n")
		chunkLog.flush()
		
		# Build chunk availability map
		chunk_owners: Dict[int, List[str]] = defaultdict(list)
		for addr_json, chunks in chunkinfo.items():
			for chunk_num in chunks:
				chunk_owners[chunk_num].append(addr_json)
		
		# Parallel download state
		completed_chunks: Set[int] = set()
		in_flight: Set[int] = set()  # Chunks currently being downloaded
		completed_chunks_lock = asyncio.Lock()
		progress_lock = asyncio.Lock()
		
		# Progress tracking
		progress_counter = 0
		
		async def download_worker(worker_id: int):
			"""Worker function for parallel chunk downloads"""
			nonlocal progress_counter
			
			while len(completed_chunks) < total:
				# Dynamically select rarest available chunk
				async with completed_chunks_lock:
					# Find chunks not yet downloaded or in-flight
					available_chunks = [
						c for c in range(total)
						if c not in completed_chunks and c not in in_flight
					]
					
					if not available_chunks:
						break
					
					# Calculate CURRENT rarity for each available chunk
					rarities = []
					for chunk_num in available_chunks:
						owners = chunk_owners.get(chunk_num, [])
						if not owners:
							continue  # Skip if no owners
						rarity = len(owners)
						rarities.append((rarity, chunk_num))
					
					if not rarities:
						break
					
					# Select randomly from rarest chunks (reduces duplicate downloads)
					min_rarity = min(r for r, _ in rarities)
					rarest_chunks = [chunk for r, chunk in rarities if r == min_rarity]
					
					# Randomize selection among equally rare chunks
					next_chunk = random.choice(rarest_chunks)
					in_flight.add(next_chunk)
				
				if next_chunk is None:
					await asyncio.sleep(0.1)  # Wait for other workers
					continue
				
				# Select peer for this chunk
				chunk_peers = chunk_owners[next_chunk]
				if not chunk_peers:
					async with completed_chunks_lock:
						in_flight.discard(next_chunk)
					continue
				
				for peer_addr in chunk_peers:
					if peer_addr not in peer_rtt_cache:
						rtt = await self.measure_peer_rtt(peer_addr)
						peer_rtt_cache[peer_addr] = rtt
						print(f"  Discovered new peer {peer_addr}: {rtt*1000:.2f}ms")

				# Intelligent peer selection: RTT + randomization for load balancing
				if len(chunk_peers) == 1:
					# Only one peer available
					selected_peer = chunk_peers[0]
				else:
					# Filter peers by RTT (< 1 second = responsive)
					available_peers = []
					for peer_addr in chunk_peers:
						rtt = peer_rtt_cache.get(peer_addr, float('inf'))
						if rtt < 1.0:  # Only consider responsive peers
							available_peers.append((peer_addr, rtt))
					
					if not available_peers:
						# Fallback: use first peer if all are slow/unreachable
						selected_peer = chunk_peers[0]
					else:
						# Sort by RTT (best first)
						available_peers.sort(key=lambda x: x[1])
						
						# Pick randomly from top 3 for load balancing
						top_peers = available_peers[:min(3, len(available_peers))]
						selected_peer = random.choice(top_peers)[0]
				
				# Create connection to peer
				h, p = json.loads(selected_peer)
				try:
					reader, writer = await asyncio.open_connection(h, p)
				except Exception:
					async with completed_chunks_lock:
						in_flight.discard(next_chunk)
					continue
				
				try:
					# Send chunk request
					writer.write(pack_message({
						"type": "PEER_REQUEST_CHUNK",
						"filename": filename,
						"chunknum": next_chunk
					}))
					await writer.drain()
					
					# Get reply
					reply = await read_message(reader)
					
					# Validate reply
					if reply.get("type") != "PEER_REPLY_CHUNK" or "data" not in reply:
						async with completed_chunks_lock:
							in_flight.discard(next_chunk)
						continue
					
					# Check for error response
					if "error" in reply or len(reply.get("data", [])) == 0:
						async with completed_chunks_lock:
							in_flight.discard(next_chunk)
						continue
					
					data = bytes(reply["data"])
					digest = reply["digest"]
					
					# Log chunk download to demo file with source and destination
					dest_address = f'["{self._address[0]}", {self._address[1]}]'
					chunkLog.write(f"Chunk {next_chunk}: From {selected_peer} to {dest_address} (RTT: {peer_rtt_cache.get(selected_peer, 0)*1000:.2f}ms)\n")
					chunkLog.flush()
					
					# Close connection after use
					writer.close()
					await writer.wait_closed()
					
					# Verify hash
					def verify_hash():
						return hashlib.sha256(data).hexdigest()
					calculated_digest = await asyncio.to_thread(verify_hash)
					
					if calculated_digest != digest:
						async with completed_chunks_lock:
							in_flight.discard(next_chunk)
							# Will be retried on next worker iteration (dynamic selection)
						continue
					
					# Write chunk to file
					def write_chunk():
						with open(dest_path + ".temp", "r+b") as out:
							out.seek(next_chunk * CHUNK_SIZE)
							out.write(data)
							out.flush()
					
					await asyncio.to_thread(write_chunk)
					
					# Immediately make this chunk available for serving by other peers
					# Add the temp file to server if not already added
					if filename not in self.handler._files:
						self.handler.add_file(filename, dest_path + ".temp")
					
					# Register chunk with tracker (immediate seeding!)
					await self.write_tracker({"type": "REQUEST_CHUNK_REGISTER", "filename": filename, "chunknum": next_chunk})
					
					# Update state
					async with completed_chunks_lock:
						completed_chunks.add(next_chunk)
						in_flight.discard(next_chunk)
					
					# Update progress
					async with progress_lock:
						progress_counter += 1
						elapsed = time.time() - download_start
						speed_mbps = (progress_counter * CHUNK_SIZE) / (elapsed * 1024 * 1024) if elapsed > 0 else 0
						print_progress(progress_counter, total, filename, speed_mbps)
				
				except Exception:
					try:
						writer.close()
						await writer.wait_closed()
					except Exception:
						pass
					async with completed_chunks_lock:
						in_flight.discard(next_chunk)
						# Will be retried on next worker iteration (dynamic selection)

		# Create temp file
		with open(dest_path + ".temp", "wb") as out:
			out.truncate(total * CHUNK_SIZE)
		
		# Fixed worker count
		num_workers = 10
		
		async def refresh_peer_info():
			"""Refresh peer and chunk information from tracker"""
			nonlocal chunkinfo, chunk_owners
			try:
				await self.write_tracker({"type": "REQUEST_FILE_LOCATION", "filename": filename})
				reply = await read_message(self.tracker_reader)
				new_chunkinfo = reply["chunkinfo"]
				
				# Update chunk availability map
				new_chunk_owners: Dict[int, List[str]] = defaultdict(list)
				for addr_json, chunks in new_chunkinfo.items():
					for chunk_num in chunks:
						new_chunk_owners[chunk_num].append(addr_json)
				
				# Update global variables (no queue rebuild needed - dynamic calculation)
				chunkinfo = new_chunkinfo
				chunk_owners = new_chunk_owners
				
				return len(new_chunkinfo)
			except Exception:
				return len(chunkinfo)
		
		async def manage_workers():
			"""Refresh peer information periodically"""
			while len(completed_chunks) < total:
				# Refresh peer information from tracker
				await refresh_peer_info()
				await asyncio.sleep(1.0)  # Refresh every 1 second (dynamic calculation handles staleness)
		
		# Start fixed number of workers
		workers = [asyncio.create_task(download_worker(i)) for i in range(num_workers)]
		
		# Start worker manager (refreshes peer info every 1 second)
		worker_manager = asyncio.create_task(manage_workers())
		
		# Wait for all workers to complete
		await asyncio.gather(*workers, worker_manager, return_exceptions=True)
		
		# Finalize download
		os.replace(dest_path + ".temp", dest_path)
		self.handler.add_file(filename, dest_path)
		
		# Log completion summary
		total_duration = time.time() - download_start
		avg_speed_mbps = file_size / (total_duration * 1024 * 1024) if total_duration > 0 else 0
		chunkLog.write(f"\n{'='*60}\n")
		chunkLog.write(f"Download completed: {filename}\n")
		chunkLog.write(f"Total time: {total_duration:.2f}s\n")
		chunkLog.write(f"Average speed: {avg_speed_mbps:.2f} MB/s\n")
		chunkLog.write(f"Total chunks: {total}\n")
		chunkLog.write(f"{'='*60}\n\n")
		chunkLog.close()

	async def write_tracker(self, obj: dict) -> None:
		assert self._tracker_writer is not None
		self._tracker_writer.write(pack_message(obj))
		await self._tracker_writer.drain()