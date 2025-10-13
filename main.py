import asyncio
import sys
import logging
from tracker import Tracker
from peer import PeerClient


logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')


async def tracker_main():
	trk = Tracker()
	print('Tracker> start <host> <port> | list_files | list_peers | list_chunkinfo | exit')
	while True:
		try:
			cmd = (await asyncio.to_thread(input, 'Tracker> ')).strip()
		except (EOFError, KeyboardInterrupt):
			cmd = 'exit'
		
		try:
			if cmd.startswith('start '):
				parts = cmd.split(' ', 2)
				if len(parts) != 3:
					print('Error: Usage: start <host> <port>')
					continue
				_, host, port = parts
				try:
					port_num = int(port)
					await trk.start(host, port_num)
					print(f'Tracker started on {trk.address()}')
				except ValueError:
					print('Error: Port must be a number')
				except Exception as e:
					print(f'Error starting tracker: {e}')
			elif cmd == 'list_files':
				try:
					files = trk.file_list()
					if files:
						print('Available files:')
						for filename, info in files.items():
							print(f'  {filename}: {info["size"]} bytes, {info["total_chunknum"]} chunks')
					else:
						print('No files available')
				except Exception as e:
					print(f'Error listing files: {e}')
			elif cmd == 'list_peers':
				try:
					peers = trk.peers()
					if peers:
						print('Connected peers:')
						for peer in peers:
							print(f'  {peer}')
					else:
						print('No peers connected')
				except Exception as e:
					print(f'Error listing peers: {e}')
			elif cmd == 'list_chunkinfo':
				try:
					chunkinfo = trk.chunkinfo()
					if chunkinfo:
						print('Chunk ownership:')
						for filename, owners in chunkinfo.items():
							print(f'  {filename}:')
							for peer, chunks in owners.items():
								print(f'    {peer}: {len(chunks)} chunks')
					else:
						print('No chunk information available')
				except Exception as e:
					print(f'Error listing chunk info: {e}')
			elif cmd == 'exit':
				await trk.stop()
				return
			else:
				print('Unknown command. Available commands:')
				print('  start <host> <port> - Start tracker server')
				print('  list_files - List available files')
				print('  list_peers - List connected peers')
				print('  list_chunkinfo - List chunk ownership')
				print('  exit - Exit the tracker')
		except Exception as e:
			print(f'Unexpected error: {e}')
			print('Tracker continues running...')


async def peer_main():
	peer = PeerClient()
	await peer.start("127.0.0.1", 0)
	print('Peer> connect <ip> <port> | publish <abs_path> | list_files | download <file> <abs_dest> | exit')
	while True:
		try:
			cmd = (await asyncio.to_thread(input, 'Peer> ')).strip()
		except (EOFError, KeyboardInterrupt):
			cmd = 'exit'
		
		try:
			if cmd.startswith('connect '):
				parts = cmd.split(' ', 2)
				if len(parts) != 3:
					print('Error: Usage: connect <ip> <port>')
					continue
				_, ip, port = parts
				try:
					port_num = int(port)
					await peer.connect_tracker(ip, port_num)
					print('connected')
				except ValueError:
					print('Error: Port must be a number')
				except Exception as e:
					print(f'Error connecting: {e}')
			elif cmd.startswith('publish '):
				parts = cmd.split(' ', 1)
				if len(parts) != 2:
					print('Error: Usage: publish <abs_path>')
					continue
				_, path = parts
				if not path.strip():
					print('Error: File path cannot be empty')
					continue
				try:
					await peer.publish(path)
					print('published')
				except FileNotFoundError:
					print(f'Error: File not found: {path}')
				except Exception as e:
					print(f'Error publishing: {e}')
			elif cmd == 'list_files':
				try:
					files = await peer.list_files()
					if files:
						print('Available files:')
						for filename, info in files.items():
							print(f'  {filename}: {info["size"]} bytes, {info["total_chunknum"]} chunks')
					else:
						print('No files available')
				except Exception as e:
					print(f'Error listing files: {e}')
			elif cmd.startswith('download '):
				parts = cmd.split(' ', 2)
				if len(parts) != 3:
					print('Error: Usage: download <file> <abs_dest>')
					continue
				_, filename, dest = parts
				if not filename.strip() or not dest.strip():
					print('Error: Filename and destination cannot be empty')
					continue
				try:
					await peer.download(filename, dest)
					print('downloaded')
				except FileNotFoundError:
					print(f'Error: File not found on network: {filename}')
				except Exception as e:
					print(f'Error downloading: {e}')
			elif cmd == 'exit':
				await peer.stop()
				return
			else:
				print('Unknown command. Available commands:')
				print('  connect <ip> <port> - Connect to tracker')
				print('  publish <abs_path> - Publish a file')
				print('  list_files - List available files')
				print('  download <file> <abs_dest> - Download a file')
				print('  exit - Exit the peer')
		except Exception as e:
			print(f'Unexpected error: {e}')
			print('Peer continues running...')


def main():
	if len(sys.argv) < 2:
		print('Usage: python main.py tracker|peer')
		sys.exit(1)
	opt = sys.argv[1]
	if opt == 'tracker':
		asyncio.run(tracker_main())
	else:
		asyncio.run(peer_main())


if __name__ == '__main__':
	main()