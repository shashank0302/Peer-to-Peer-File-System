import logging
import os
from datetime import datetime
from typing import Optional


class P2PLogger:
    """Centralized logging for P2P operations with consolidated log files."""
    
    def __init__(self, component: str, log_dir: str = None):
        self.component = component
        if log_dir is None:
            # Create logs inside mini_p2pfs folder
            import os
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.log_dir = os.path.join(current_dir, "logs")
        else:
            self.log_dir = log_dir
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._setup_logger()
    
    def _setup_logger(self):
        """Setup consolidated logger for all operations."""
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Single consolidated logger for all operations
        self.logger = self._create_logger(
            self.component,
            f"{self.log_dir}/{self.component}.log"
        )
    
    def _create_logger(self, name: str, filename: str) -> logging.Logger:
        """Create a logger with file handler and time formatting."""
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # File handler (append mode for consolidated logs)
        file_handler = logging.FileHandler(filename, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Console handler (for important messages)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARNING)
        
        # Formatter with timestamp and session info
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S.%f'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def log_session(self, message: str):
        """Log session lifecycle events."""
        self.logger.info(f"[SESSION] {message}")
    
    def log_chunk_operation(self, operation: str, filename: str, chunk_num: int, 
                           peer_address: Optional[str] = None, size_bytes: Optional[int] = None,
                           duration_ms: Optional[float] = None):
        """Log chunk download/upload operations."""
        msg = f"[CHUNK] {operation} | File: {filename} | Chunk: {chunk_num}"
        if peer_address:
            msg += f" | Peer: {peer_address}"
        if size_bytes:
            msg += f" | Size: {size_bytes} bytes"
        if duration_ms:
            msg += f" | Duration: {duration_ms:.2f}ms"
        self.logger.info(msg)
    
    def log_network_event(self, event: str, details: str = ""):
        """Log network connection/disconnection events."""
        msg = f"[NETWORK] {event}"
        if details:
            msg += f" | {details}"
        self.logger.info(msg)
    
    def log_performance(self, operation: str, metrics: dict):
        """Log performance metrics."""
        msg = f"[PERF] {operation}"
        for key, value in metrics.items():
            msg += f" | {key}: {value}"
        self.logger.info(msg)
    
    def log_rarest_first_strategy(self, filename: str, chunk_counts: dict, selected_chunk: int):
        """Log rarest-first chunk selection."""
        msg = f"[STRATEGY] File: {filename} | Selected chunk: {selected_chunk}"
        msg += f" | Chunk availability: {chunk_counts}"
        self.logger.info(msg)
    
    def log_download_progress(self, filename: str, completed_chunks: int, total_chunks: int, 
                            speed_mbps: Optional[float] = None):
        """Log download progress."""
        percentage = (completed_chunks / total_chunks * 100) if total_chunks > 0 else 0
        msg = f"[PROGRESS] {filename} | {completed_chunks}/{total_chunks} chunks ({percentage:.1f}%)"
        if speed_mbps:
            msg += f" | Speed: {speed_mbps:.2f} MB/s"
        self.logger.info(msg)


# Global loggers for easy access
tracker_logger: Optional[P2PLogger] = None
peer_loggers: dict = {}


def init_tracker_logger() -> P2PLogger:
    """Initialize tracker logger."""
    global tracker_logger
    tracker_logger = P2PLogger("tracker")
    return tracker_logger


def init_peer_logger(peer_id: str) -> P2PLogger:
    """Initialize peer logger."""
    global peer_loggers
    peer_loggers[peer_id] = P2PLogger(f"peer_{peer_id}")
    return peer_loggers[peer_id]
