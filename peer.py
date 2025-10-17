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

CHUNK_SIZE = 1024 * 512


#print the progress of the download
def print_progress(current: int, total: int, filename: str, speed_mbps: float, bar_length: int = 50) -> None:
	if total == 0:
		return
	percentage = (current / total) * 100
	filled_length = int(bar_length * current // total)
	bar = '#' * filled_length + '-' * (bar_length - filled_length)
	sys.stdout.write(f'\r[{bar}] {percentage:5.1f}% ({current}/{total}) {filename} - {speed_mbps:.2f} MB/s')
	sys.stdout.flush()
	if current == total:
		sys.stdout.write('\n')
		sys.stdout.flush()


#pack the message into a bytes object for the tracker
def pack_message(obj: dict) -> bytes:
	data = json.dumps(obj).encode("utf-8")
	return struct.pack(">I", len(data)) + data

#read the message from the reader and return the message as a dictionary
async def read_message(reader: asyncio.StreamReader) -> dict:
	raw_len = await reader.readexactly(4)
	(msg_len,) = struct.unpack(">I", raw_len)
	raw = await reader.readexactly(msg_len)
	return json.loads(raw.decode("utf-8"))


class PeerServer:
	#This class is used to handle the incoming chunk requests from other peers.
	#It has functions: add_file, clear_files, handle.

	def __init__(self) -> None:
		#initialize the peer server
		self._files: Dict[str, str] = {}
		self.peer_id = str(uuid.uuid4())[:8]

	def add_file(self, filename: str, local_path: str) -> None:
		#add the file to the peer server
		self._files[filename] = local_path
	
	def clear_files(self) -> None:
		#clear the files from the peer server
		self._files.clear()

	async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
		#handle the incoming chunk requests from other peers
		try:
			while not reader.at_eof():
				msg = await read_message(reader)
				if msg.get("type") == "PEER_REQUEST_CHUNK":
					filename = msg["filename"]
					chunknum = int(msg["chunknum"])
					
					#check if the file is available on the peer server
					if filename not in self._files:
						writer.write(pack_message({
							"type": "PEER_REPLY_CHUNK",
							"filename": filename,
							"chunknum": chunknum,
							"data": [],
							"digest": "",
							"error": f"File {filename} not available on this peer"
						}))
						await writer.drain()
						continue
					
					path = self._files[filename]

					#read the chunk from the file
					def read_chunk():
						with open(path, "rb") as f:
							f.seek(chunknum * CHUNK_SIZE)
							return f.read(CHUNK_SIZE)
					
					#read the chunk from the file asynchronously
					data = await asyncio.to_thread(read_chunk)
					
					#calculate the hash of the chunk
					def calculate_hash():
						return hashlib.sha256(data).hexdigest()

					digest = await asyncio.to_thread(calculate_hash)
					

					writer.write(pack_message({
						"type": "PEER_REPLY_CHUNK",
						"filename": filename,
						"chunknum": chunknum,
						"data": list(data),
						"digest": digest,
					}))
					await writer.drain()
				
				elif msg.get("type") == "PEER_PING_PONG":
					#ping the peer used for RTT measurement.
					writer.write(pack_message(msg))
					await writer.drain()

		except (asyncio.IncompleteReadError, ConnectionError, RuntimeError):
			pass

		finally:
			#close the writer
			try:
				writer.close()
				await writer.wait_closed()
			except Exception:
				pass


class PeerClient:
	
	#This class is used for tracker communication and file operations
	#It has functions: start, stop, connect_tracker, publish, list_files, measure_peer_rtt, download, write_tracker.

	#initialize the peer client
	def __init__(self) -> None:
		self.tracker_reader: Optional[asyncio.StreamReader] = None
		self.tracker_writer: Optional[asyncio.StreamWriter] = None
		self.handler = PeerServer()
		self.peer_id = self.handler.peer_id
		self._server: Optional[asyncio.base_events.Server] = None
		self._address: Optional[Tuple[str, int]] = None

	async def start(self, host: str = "0.0.0.0", port: int = 0) -> None:
		#start the peer server
		if self._server is not None:
			raise RuntimeError("Peer server already running")
		self._server = await asyncio.start_server(self.handler.handle, host, port)
		sock = self._server.sockets[0]
		self._address = sock.getsockname()[:2]

	async def stop(self) -> None:
		#stop the peer server
		if self.tracker_writer is not None and not self.tracker_writer.is_closing():
			self.tracker_writer.close()
			await self.tracker_writer.wait_closed()
		self.tracker_reader = None
		self.tracker_writer = None
		
		server, self._server = self._server, None
		if server is not None:
			server.close()
			await server.wait_closed()
		self._address = None
		self.handler.clear_files()

	async def connect_tracker(self, host: str, port: int, peer_host: str = "0.0.0.0") -> None:
		#connect the peer to the tracker
		if self.tracker_writer is not None and not self.tracker_writer.is_closing():
			raise RuntimeError("Already connected to tracker. Disconnect first or restart peer.")
		
		try:
			server, self._server = self._server, None
			if server is not None:
				server.close()
				await server.wait_closed()
			self._address = None
			self.handler.clear_files()
			
			self._server = await asyncio.start_server(self.handler.handle, peer_host, 0)
			sock = self._server.sockets[0]
			self._address = sock.getsockname()[:2]
			
			#get the external IP address of the peer to register with the tracker.
			s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #create a socket object
			s.connect(("8.8.8.8", 80)) #google public DNS server
			external_ip = s.getsockname()[0] #get the external IP address of the peer
			s.close()
			
			self.tracker_reader, self.tracker_writer = await asyncio.open_connection(host, port)
			#register the peer with the tracker using the external IP address and the port of the peer server.
			await self.write_tracker({ 
				"type": "REQUEST_REGISTER",
				"address": [external_ip, self._address[1]],
			})
			await read_message(self.tracker_reader)

		except ConnectionRefusedError:
			#connection refused
			raise ConnectionError(f"Could not connect to tracker at {host}:{port} - connection refused")
		except asyncio.TimeoutError:
			#connection timed out
			raise ConnectionError(f"Connection to tracker at {host}:{port} timed out")
		except Exception as e:
			#failed to connect to the tracker
			raise ConnectionError(f"Failed to connect to tracker at {host}:{port}: {e}")

	async def publish(self, abs_path: str) -> None:
		#publish the file to the tracker by the peer
		if not os.path.isfile(abs_path):
			#file not found
			raise FileNotFoundError(abs_path)
		filename = os.path.basename(abs_path)
		file_size = os.path.getsize(abs_path)

		#calculate the total number of chunks in the file
		total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
		
		#calculate the original hashes for the file
		print(f"Calculating original hashes for {filename} ({total_chunks} chunks)...")
		
		def calculate_org_hashes():
			org_hashes = []
			with open(abs_path, "rb") as f:
				for chunk_idx in range(total_chunks):
					f.seek(chunk_idx * CHUNK_SIZE)
					chunk_data = f.read(CHUNK_SIZE)
					chunk_hash = hashlib.sha256(chunk_data).hexdigest()
					org_hashes.append(chunk_hash)
			return org_hashes
		
		org_hashes = await asyncio.to_thread(calculate_org_hashes)
		print(f"Calculated {len(org_hashes)} org hashes")
		
		self.handler.add_file(filename, abs_path)

		#publish the file to the tracker by the peer
		await self.write_tracker({
			"type": "REQUEST_PUBLISH",
			"filename": filename,
			"fileinfo": {"size": file_size, "total_chunknum": total_chunks},
			"org_hashes": org_hashes,
		})
		reply = await read_message(self.tracker_reader)
		
		if not reply.get("result", False):
			error_msg = reply.get("error", "Unknown error")
			raise RuntimeError(f"Failed to publish {filename}: {error_msg}")

	async def list_files(self) -> Dict[str, Dict[str, int]]:
		#list the files available on the tracker
		await self.write_tracker({"type": "REQUEST_FILE_LIST"})
		msg = await read_message(self.tracker_reader)
		return msg["file_list"]

	async def measure_peer_rtt(self, addr_json: str) -> float:
		#measure the Round Trip Time (RTT) to the peer to other peers.
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
			return float('inf')

	async def download(self, filename: str, dest_path: str) -> None:
		#Parallel download with rarest-first, RTT-based peer selection, and integrity verification
		download_start = time.time()
		
		#get the location of the file from the tracker
		await self.write_tracker({"type": "REQUEST_FILE_LOCATION", "filename": filename})
		reply = await read_message(self.tracker_reader)
		fileinfo = reply["fileinfo"]

		#get the chunk info of the file
		chunkinfo = reply["chunkinfo"]

		total = int(fileinfo["total_chunknum"])

		#get the size of the file
		file_size = int(fileinfo["size"])
		
		#get the original hashes of the file and cache them.
		org_hashes = fileinfo.get("org_hashes", [])

		if len(org_hashes) != total:
			raise RuntimeError(f"org hash count mismatch: expected {total}, got {len(org_hashes)}")
		
		print(f"Cached {len(org_hashes)} org hashes for integrity verification")
		
		#log that download has started
		chunkLog = open("ChunkDownload.log", "a")
		chunkLog.write(f"\n{'='*60}\n")
		chunkLog.write(f"Download started: {filename} at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
		chunkLog.write(f"Peer ID: {self.peer_id}\n")
		chunkLog.write(f"File size: {file_size:,} bytes, {total} chunks\n")
		chunkLog.write(f"org hashes cached: {len(org_hashes)} (integrity checking enabled)\n")
		chunkLog.write(f"{'='*60}\n\n")
		chunkLog.flush()
		
		print(f"\nStarting parallel download: {filename} ({file_size:,} bytes, {total} chunks)")
		print_progress(0, total, filename, 0.0)
		
		#get the unique peers from the chunk info
		unique_peers = set(chunkinfo.keys())
		
		print(f"\nMeasuring RTT to {len(unique_peers)} peers...")
		chunkLog.write("=== RTT Measurements ===\n")
		#cache the RTT to the peers
		peer_rtt_cache = {}
		#measure the RTT to the peers
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
		
		#get the chunk owners from the chunk info
		chunk_owners: Dict[int, List[str]] = defaultdict(list)
		for addr_json, chunks in chunkinfo.items():
			for chunk_num in chunks:
				chunk_owners[chunk_num].append(addr_json)
		
		completed_chunks: Set[int] = set() #set of completed chunks
		in_flight: Set[int] = set() #set of chunks in flight
		failed_tries: Dict[int, Set[str]] = defaultdict(set) #set of failed tries for each chunk if chunk is corrupted.
		fatal_error: Optional[RuntimeError] = None #fatal error if the file is corrupted.
		completed_chunks_lock = asyncio.Lock() #lock for the completed chunks
		progress_lock = asyncio.Lock() #lock for the progress
		progress_counter = 0 #counter for the progress
		
		#download worker to download the chunks in parallel using the rarest-first algorithm.
		async def download_worker(worker_id: int):
			#nonlocal variables for the progress counter and fatal error.
			nonlocal progress_counter, fatal_error
			
			while len(completed_chunks) < total and fatal_error is None:
				async with completed_chunks_lock:
					available_chunks = [
						c for c in range(total)
						if c not in completed_chunks and c not in in_flight
					]
					
					if not available_chunks:
						break
					
					rarities = [] #list of rarities for the available chunks
					for chunk_num in available_chunks:
						owners = chunk_owners.get(chunk_num, []) #get the owners of the chunk
						if not owners:
							continue
						rarity = len(owners)
						rarities.append((rarity, chunk_num))
					
					if not rarities:
						break
					

					min_rarity = min(r for r, _ in rarities) #get the rarest chunk
					rarest_chunks = [chunk for r, chunk in rarities if r == min_rarity] #get the rarest chunks with the same min rarity.
					next_chunk = random.choice(rarest_chunks) #choose a random chunk from the rarest chunks.
					in_flight.add(next_chunk)
				
				if next_chunk is None:
					await asyncio.sleep(0.1)
					continue
				
				all_chunk_peers = chunk_owners[next_chunk] #get all the peers that has the chunk that a worker chose from the rarest chunks.
				chunk_peers = [p for p in all_chunk_peers if p not in failed_tries[next_chunk]] #get the peers that have not failed the integrity check for the chunk.
				
				if not chunk_peers:
					async with completed_chunks_lock:
						#if all the peers have failed the integrity check for the chunk, then set the fatal error and return.
						in_flight.discard(next_chunk)
						available_peers_for_chunk = set(all_chunk_peers)
						if failed_tries[next_chunk] >= available_peers_for_chunk:
							chunkLog.write(f"FATAL: All peers have corrupted data for chunk {next_chunk}\n")
							chunkLog.flush()
							fatal_error = RuntimeError(f"File corrupted: No peer has valid data for chunk {next_chunk}. All {len(available_peers_for_chunk)} peers failed integrity check.")
							return
					continue
				
				#measure the RTT to the peers that have the chunk but joined the network later.
				for peer_addr in chunk_peers:
					if peer_addr not in peer_rtt_cache:
						rtt = await self.measure_peer_rtt(peer_addr)
						peer_rtt_cache[peer_addr] = rtt
						print(f"  Discovered new peer {peer_addr}: {rtt*1000:.2f}ms")

				if len(chunk_peers) == 1:
					selected_peer = chunk_peers[0] #if there is only one peer, then choose it.
				else:
					available_peers = [] #list of available peers with the chunk.
					for peer_addr in chunk_peers:
						rtt = peer_rtt_cache.get(peer_addr, float('inf'))
						if rtt < 1.0: #if the RTT is less than 1 second, then add the peer to the list.
							available_peers.append((peer_addr, rtt))
					
					if not available_peers:
						selected_peer = chunk_peers[0] #if there are no available peers, then choose the first peer.
					else:
						available_peers.sort(key=lambda x: x[1]) #sort the peers by the RTT.
						top_peers = available_peers[:min(3, len(available_peers))] #get the top 3 peers.
						selected_peer = random.choice(top_peers)[0] #choose a random peer from the top 3 peers.
					
				h, p = json.loads(selected_peer) #get the IP address and port of the selected peer.
				try:
					reader, writer = await asyncio.open_connection(h, p) #open a connection to the selected peer.
				except Exception:
					async with completed_chunks_lock:
						in_flight.discard(next_chunk) 
					continue
				
				try:
					writer.write(pack_message({
						"type": "PEER_REQUEST_CHUNK",
						"filename": filename,
						"chunknum": next_chunk
					}))
					await writer.drain()
					
					reply = await read_message(reader)
					
					if reply.get("type") != "PEER_REPLY_CHUNK" or "data" not in reply:
						async with completed_chunks_lock:
							in_flight.discard(next_chunk)
						continue
					
					if "error" in reply or len(reply.get("data", [])) == 0:
						async with completed_chunks_lock:
							in_flight.discard(next_chunk)
						continue
					
					data = bytes(reply["data"]) #get the data from the reply.
					digest = reply["digest"] #get the digest from the reply.
					
					dest_address = f'["{self._address[0]}", {self._address[1]}]' #get the destination address of the peer.
					chunkLog.write(f"Chunk {next_chunk}: From {selected_peer} to {dest_address} (RTT: {peer_rtt_cache.get(selected_peer, 0)*1000:.2f}ms)\n")
					chunkLog.flush()
					
					writer.close()
					await writer.wait_closed()
					
					def verify_hash():
						return hashlib.sha256(data).hexdigest() #verify the hash of the chunk.
					calculated_digest = await asyncio.to_thread(verify_hash)
					org_hash = org_hashes[next_chunk] #get the original hash of the chunk.
					
					if calculated_digest != org_hash:
						#integrity violation detected for the chunk. log the error and continue.
						chunkLog.write(f"INTEGRITY VIOLATION - Chunk {next_chunk}: Hash mismatch from {selected_peer}\n")
						chunkLog.write(f"    org hash:    {org_hash[:32]}...\n")
						chunkLog.write(f"    Calculated:     {calculated_digest[:32]}...\n")
						chunkLog.write(f"    Peer claimed:   {digest[:32]}...\n")
						chunkLog.flush()
						print(f"\n[WARNING] Integrity violation detected for chunk {next_chunk} from {selected_peer}")
						async with completed_chunks_lock:
							in_flight.discard(next_chunk)
							failed_tries[next_chunk].add(selected_peer)
						continue
					
					#write the chunk which is verified coming from the selected peer.
					def write_chunk():
						with open(dest_path + ".temp", "r+b") as out:
							out.seek(next_chunk * CHUNK_SIZE)
							out.write(data)
							out.flush()
					
					await asyncio.to_thread(write_chunk)
					
					if filename not in self.handler._files:
						#add the file to the peer server if it is not already added.
						self.handler.add_file(filename, dest_path + ".temp")
					
					#register the chunk to the tracker
					await self.write_tracker({"type": "REQUEST_CHUNK_REGISTER", "filename": filename, "chunknum": next_chunk})
					
					async with completed_chunks_lock:
						completed_chunks.add(next_chunk) #add the chunk to the completed chunks.
						in_flight.discard(next_chunk) #remove the chunk from the in flight chunks.
					
					async with progress_lock:
						progress_counter += 1 #increment the progress counter.
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

		with open(dest_path + ".temp", "wb") as out:
			out.truncate(total * CHUNK_SIZE)
		
		#set the number of workers to 10
		num_workers = 10
		
		async def refresh_peer_info():
			#refresh the peer info from the tracker every second. This is used to get the latest chunk info from the tracker to all the workers.
			nonlocal chunkinfo, chunk_owners
			try:
				await self.write_tracker({"type": "REQUEST_FILE_LOCATION", "filename": filename})
				reply = await read_message(self.tracker_reader)
				new_chunkinfo = reply["chunkinfo"]
				
				new_chunk_owners: Dict[int, List[str]] = defaultdict(list)
				for addr_json, chunks in new_chunkinfo.items():
					for chunk_num in chunks:
						new_chunk_owners[chunk_num].append(addr_json)
				
				chunkinfo = new_chunkinfo
				chunk_owners = new_chunk_owners
				
				return len(new_chunkinfo)
			except Exception:
				return len(chunkinfo)
		
		async def manage_workers():
			#manage the workers to download the chunks in parallel.
			while len(completed_chunks) < total and fatal_error is None:
				await refresh_peer_info()
				await asyncio.sleep(1.0)
		
		workers = [asyncio.create_task(download_worker(i)) for i in range(num_workers)]
		worker_manager = asyncio.create_task(manage_workers())

		#wait for the workers to complete the download.
		await asyncio.gather(*workers, worker_manager, return_exceptions=True)
		
		if fatal_error is not None:
			chunkLog.close()
			raise fatal_error
		
		#truncate the destination file to the size of the file and replace the temporary file with the destination file.
		with open(dest_path + ".temp", "r+b") as f:
			f.truncate(file_size)
		
		os.replace(dest_path + ".temp", dest_path)
		self.handler.add_file(filename, dest_path)
		
		total_duration = time.time() - download_start
		avg_speed_mbps = file_size / (total_duration * 1024 * 1024) if total_duration > 0 else 0
		chunkLog.write(f"\n{'='*60}\n")
		chunkLog.write(f"Download completed: {filename}\n")
		chunkLog.write(f"Total time: {total_duration:.2f}s\n")
		chunkLog.write(f"Average speed: {avg_speed_mbps:.2f} MB/s\n")
		chunkLog.write(f"Total chunks: {total}\n")
		chunkLog.write(f"{'='*60}\n\n")
		chunkLog.close()

	#write the message to the tracker
	async def write_tracker(self, obj: dict) -> None:
		assert self.tracker_writer is not None
		self.tracker_writer.write(pack_message(obj))
		await self.tracker_writer.drain()