"""
Chunked Download Manager for Large Sequencing Files

Provides streaming/chunked download capabilities to handle files larger than
available memory or disk space. Supports:
- Range-based downloads from MinIO/S3
- Configurable chunk sizes
- Progress tracking
- Automatic cleanup of processed chunks
- Retry logic with exponential backoff for network resilience

This module enables processing 130GB+ files with only 5GB disk/memory available
by downloading, processing, and deleting chunks sequentially.

Thread Safety:
    ChunkedDownloader instances are NOT thread-safe. Each thread/worker should
    create its own ChunkedDownloader instance. The download operations themselves
    are safe to run in parallel on different instances.
"""
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple
from contextlib import contextmanager

from botocore.exceptions import BotoCoreError, ClientError
from minio.error import S3Error

from worker.api.storage.upload_manager import (
    MINIO_BUCKET,
    ensure_bucket,
    minio_client,
    s3_internal_client,
)

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Custom Exceptions
# -----------------------------------------------------------------------------

class ChunkedDownloadError(Exception):
    """Base exception for chunked download operations."""
    pass


class ObjectNotFoundError(ChunkedDownloadError):
    """Raised when the requested object does not exist in storage."""
    pass


class RangeDownloadError(ChunkedDownloadError):
    """Raised when a range download fails after all retries."""
    pass


class ChunkCleanupError(ChunkedDownloadError):
    """Raised when chunk cleanup fails (non-fatal, logged as warning)."""
    pass


# -----------------------------------------------------------------------------
# Retry Configuration
# -----------------------------------------------------------------------------

MAX_DOWNLOAD_RETRIES = 3
INITIAL_RETRY_DELAY_SECONDS = 1.0
MAX_RETRY_DELAY_SECONDS = 30.0


def _is_retryable_error(error: Exception) -> bool:
    """Check if an error is retryable (transient network/server issues)."""
    if isinstance(error, (BotoCoreError, ClientError)):
        error_str = str(error).lower()
        # Retryable conditions
        retryable_patterns = [
            "timeout",
            "connection",
            "throttl",
            "serviceunavailable",
            "internalerror",
            "slowdown",
            "requesttimeout",
        ]
        return any(pattern in error_str for pattern in retryable_patterns)
    return False


def _retry_with_backoff(
    operation: Callable[[], Any],
    operation_name: str,
    max_retries: int = MAX_DOWNLOAD_RETRIES,
) -> Any:
    """
    Execute an operation with exponential backoff retry.
    
    Args:
        operation: Callable to execute
        operation_name: Name for logging
        max_retries: Maximum number of retries
        
    Returns:
        Result of the operation
        
    Raises:
        The last exception if all retries fail
    """
    last_exception: Optional[Exception] = None
    
    for attempt in range(max_retries + 1):
        try:
            return operation()
        except Exception as e:
            last_exception = e
            
            if attempt < max_retries and _is_retryable_error(e):
                delay = min(
                    INITIAL_RETRY_DELAY_SECONDS * (2 ** attempt),
                    MAX_RETRY_DELAY_SECONDS,
                )
                logger.warning(
                    f"{operation_name} failed (attempt {attempt + 1}/{max_retries + 1}), "
                    f"retrying in {delay:.1f}s: {e}"
                )
                time.sleep(delay)
            else:
                raise
    
    # Should not reach here, but just in case
    raise last_exception if last_exception else RuntimeError("Unexpected retry loop exit")

# Default chunk size: 1GB for processing, can be tuned based on worker memory
DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024 * 1024  # 1GB
MIN_CHUNK_SIZE = 64 * 1024 * 1024  # 64MB minimum
MAX_CHUNK_SIZE = 5 * 1024 * 1024 * 1024  # 5GB maximum


@dataclass
class ChunkInfo:
    """Information about a downloaded chunk."""
    chunk_number: int
    start_byte: int
    end_byte: int
    size: int
    local_path: Path
    object_name: str
    is_last: bool = False


@dataclass
class ObjectMetadata:
    """Metadata for a MinIO object."""
    object_name: str
    size: int
    content_type: str
    etag: str
    last_modified: Any = None


@dataclass
class ChunkedDownloadProgress:
    """Progress tracking for chunked downloads."""
    object_name: str
    total_size: int
    chunk_size: int
    total_chunks: int
    chunks_downloaded: int = 0
    bytes_downloaded: int = 0
    chunks_processed: int = 0
    current_chunk: Optional[ChunkInfo] = None
    
    @property
    def download_percent(self) -> float:
        if self.total_size == 0:
            return 100.0
        return (self.bytes_downloaded / self.total_size) * 100
    
    @property
    def process_percent(self) -> float:
        if self.total_chunks == 0:
            return 100.0
        return (self.chunks_processed / self.total_chunks) * 100


class ChunkedDownloader:
    """
    Downloads large files from MinIO in chunks for memory-constrained processing.
    
    Usage:
        downloader = ChunkedDownloader(chunk_size=1*1024*1024*1024)  # 1GB chunks
        
        for chunk in downloader.iter_chunks("uploads/user/file.fast5", "/tmp/workdir"):
            # Process chunk.local_path
            process_file(chunk.local_path)
            # Chunk is automatically cleaned up when next chunk is requested
    """
    
    def __init__(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        auto_cleanup: bool = True,
        progress_callback: Optional[Callable[[ChunkedDownloadProgress], None]] = None,
    ):
        """
        Initialize chunked downloader.
        
        Args:
            chunk_size: Size of each chunk in bytes (default 1GB)
            auto_cleanup: Whether to automatically delete chunks after iteration
            progress_callback: Optional callback for progress updates
        """
        self.chunk_size = max(MIN_CHUNK_SIZE, min(chunk_size, MAX_CHUNK_SIZE))
        self.auto_cleanup = auto_cleanup
        self.progress_callback = progress_callback
        self._current_chunk_path: Optional[Path] = None
    
    def get_object_metadata(self, object_name: str) -> ObjectMetadata:
        """Get metadata for an object without downloading it.
        
        Raises:
            ObjectNotFoundError: If the object does not exist
            ChunkedDownloadError: On other storage errors
        """
        ensure_bucket()
        try:
            stat = minio_client.stat_object(MINIO_BUCKET, object_name)
            return ObjectMetadata(
                object_name=object_name,
                size=stat.size,
                content_type=stat.content_type or "application/octet-stream",
                etag=stat.etag,
                last_modified=stat.last_modified,
            )
        except S3Error as e:
            if "NoSuchKey" in str(e) or "Not Found" in str(e):
                raise ObjectNotFoundError(f"Object not found: {object_name}") from e
            raise ChunkedDownloadError(f"Failed to get object metadata for {object_name}: {e}") from e
    
    def calculate_chunks(self, total_size: int) -> List[Tuple[int, int]]:
        """
        Calculate chunk boundaries for a file.
        
        Args:
            total_size: Total size of the file in bytes
        
        Returns:
            List of (start_byte, end_byte) tuples. For empty files, returns
            an empty list as there's nothing to download.
        """
        if total_size == 0:
            # Empty files have no chunks to download
            return []
        
        chunks = []
        start = 0
        while start < total_size:
            end = min(start + self.chunk_size, total_size)
            chunks.append((start, end - 1))  # end is inclusive for range requests
            start = end
        return chunks
    
    def _download_range(
        self,
        object_name: str,
        start_byte: int,
        end_byte: int,
        local_path: Path,
    ) -> int:
        """
        Download a byte range from MinIO to local file.
        
        Args:
            object_name: Object key in MinIO
            start_byte: Start of range (inclusive)
            end_byte: End of range (inclusive)
            local_path: Local path to save chunk
            
        Returns:
            Number of bytes downloaded
            
        Raises:
            RangeDownloadError: If download fails after all retries
        """
        ensure_bucket()
        
        def do_download() -> int:
            response = s3_internal_client.get_object(
                Bucket=MINIO_BUCKET,
                Key=object_name,
                Range=f"bytes={start_byte}-{end_byte}",
            )
            
            # Stream to file to avoid memory issues
            local_path.parent.mkdir(parents=True, exist_ok=True)
            bytes_written = 0
            
            with open(local_path, "wb") as f:
                # Read in 8MB blocks
                for chunk in response["Body"].iter_chunks(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
                    bytes_written += len(chunk)
            
            return bytes_written
        
        try:
            return _retry_with_backoff(
                do_download,
                f"Download range {start_byte}-{end_byte} of {object_name}",
            )
        except (ClientError, BotoCoreError) as e:
            raise RangeDownloadError(
                f"Failed to download range {start_byte}-{end_byte} of {object_name} "
                f"after {MAX_DOWNLOAD_RETRIES + 1} attempts: {e}"
            ) from e
    
    def _cleanup_chunk(self, chunk_path: Optional[Path]) -> None:
        """Remove a chunk file from disk."""
        if chunk_path and chunk_path.exists():
            try:
                chunk_path.unlink()
                logger.debug(f"Cleaned up chunk: {chunk_path}")
            except OSError as e:
                logger.warning(f"Failed to cleanup chunk {chunk_path}: {e}")
    
    def iter_chunks(
        self,
        object_name: str,
        workdir: Path,
        chunk_prefix: str = "chunk_",
    ) -> Generator[ChunkInfo, None, None]:
        """
        Iterate over chunks of a file, downloading each on demand.
        
        Args:
            object_name: Object key in MinIO
            workdir: Working directory for chunk files
            chunk_prefix: Prefix for chunk filenames
            
        Yields:
            ChunkInfo for each chunk
            
        Note:
            When auto_cleanup is True, each chunk is deleted when the next
            chunk is requested. The final chunk is deleted when the generator
            is exhausted or garbage collected.
        """
        metadata = self.get_object_metadata(object_name)
        chunk_boundaries = self.calculate_chunks(metadata.size)
        total_chunks = len(chunk_boundaries)
        
        progress = ChunkedDownloadProgress(
            object_name=object_name,
            total_size=metadata.size,
            chunk_size=self.chunk_size,
            total_chunks=total_chunks,
        )
        
        workdir = Path(workdir)
        workdir.mkdir(parents=True, exist_ok=True)
        
        previous_chunk_path: Optional[Path] = None
        
        try:
            for i, (start_byte, end_byte) in enumerate(chunk_boundaries):
                # Cleanup previous chunk if auto_cleanup enabled
                if self.auto_cleanup and previous_chunk_path:
                    self._cleanup_chunk(previous_chunk_path)
                    progress.chunks_processed += 1
                
                # Download current chunk
                chunk_filename = f"{chunk_prefix}{i:06d}.bin"
                chunk_path = workdir / chunk_filename
                
                logger.info(f"Downloading chunk {i+1}/{total_chunks} ({start_byte}-{end_byte})")
                bytes_downloaded = self._download_range(
                    object_name, start_byte, end_byte, chunk_path
                )
                
                chunk_info = ChunkInfo(
                    chunk_number=i,
                    start_byte=start_byte,
                    end_byte=end_byte,
                    size=bytes_downloaded,
                    local_path=chunk_path,
                    object_name=object_name,
                    is_last=(i == total_chunks - 1),
                )
                
                progress.chunks_downloaded += 1
                progress.bytes_downloaded += bytes_downloaded
                progress.current_chunk = chunk_info
                
                if self.progress_callback:
                    self.progress_callback(progress)
                
                previous_chunk_path = chunk_path
                self._current_chunk_path = chunk_path
                
                yield chunk_info
            
            # Cleanup final chunk
            if self.auto_cleanup and previous_chunk_path:
                self._cleanup_chunk(previous_chunk_path)
                progress.chunks_processed += 1
                
        except GeneratorExit:
            # Generator was closed early - still cleanup
            if self.auto_cleanup and previous_chunk_path:
                self._cleanup_chunk(previous_chunk_path)
            raise
    
    @contextmanager
    def download_chunk(
        self,
        object_name: str,
        chunk_number: int,
        workdir: Path,
    ):
        """
        Context manager for downloading a single specific chunk.
        
        Usage:
            with downloader.download_chunk("file.fast5", 0, workdir) as chunk:
                process(chunk.local_path)
            # Chunk is automatically cleaned up
        """
        metadata = self.get_object_metadata(object_name)
        chunk_boundaries = self.calculate_chunks(metadata.size)
        
        if chunk_number >= len(chunk_boundaries):
            raise ValueError(f"Chunk {chunk_number} does not exist (total: {len(chunk_boundaries)})")
        
        start_byte, end_byte = chunk_boundaries[chunk_number]
        chunk_path = Path(workdir) / f"chunk_{chunk_number:06d}.bin"
        
        try:
            self._download_range(object_name, start_byte, end_byte, chunk_path)
            
            yield ChunkInfo(
                chunk_number=chunk_number,
                start_byte=start_byte,
                end_byte=end_byte,
                size=end_byte - start_byte + 1,
                local_path=chunk_path,
                object_name=object_name,
                is_last=(chunk_number == len(chunk_boundaries) - 1),
            )
        finally:
            if self.auto_cleanup:
                self._cleanup_chunk(chunk_path)


class ChunkedSessionDownloader:
    """
    Downloads all files in an upload session with chunked processing support.
    
    Provides a higher-level interface for processing entire upload sessions
    file-by-file and chunk-by-chunk.
    """
    
    def __init__(
        self,
        session_id: str,
        user_id: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        auto_cleanup: bool = True,
    ):
        """
        Initialize session downloader.
        
        Args:
            session_id: Upload session ID
            user_id: User who owns the session
            chunk_size: Size of chunks for large files
            auto_cleanup: Whether to cleanup chunks automatically
        """
        self.session_id = session_id
        self.user_id = user_id
        self.chunk_size = chunk_size
        self.auto_cleanup = auto_cleanup
        self.downloader = ChunkedDownloader(
            chunk_size=chunk_size,
            auto_cleanup=auto_cleanup,
        )
    
    def list_files(self) -> List[ObjectMetadata]:
        """List all files in the session with their metadata."""
        from worker.api.storage.upload_manager import UploadSession
        
        object_names = UploadSession.list_session_files(self.session_id, self.user_id)
        return [self.downloader.get_object_metadata(name) for name in object_names]
    
    def needs_chunking(self, metadata: ObjectMetadata) -> bool:
        """Check if a file is large enough to require chunked processing."""
        return metadata.size > self.chunk_size
    
    def iter_session_files(
        self,
        workdir: Path,
        file_filter: Optional[Callable[[str], bool]] = None,
    ) -> Generator[Tuple[ObjectMetadata, Generator[ChunkInfo, None, None]], None, None]:
        """
        Iterate over all files in the session, yielding chunk generators.
        
        Args:
            workdir: Working directory for downloads
            file_filter: Optional filter function for filenames
            
        Yields:
            Tuple of (ObjectMetadata, chunk_generator) for each file
        """
        files = self.list_files()
        
        for metadata in files:
            filename = Path(metadata.object_name).name
            
            if file_filter and not file_filter(filename):
                continue
            
            file_workdir = workdir / f"file_{Path(filename).stem}"
            chunk_gen = self.downloader.iter_chunks(
                metadata.object_name,
                file_workdir,
            )
            
            yield metadata, chunk_gen


def download_file_chunked(
    object_name: str,
    workdir: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    processor: Optional[Callable[[ChunkInfo], Any]] = None,
) -> List[Any]:
    """
    Convenience function to download and process a file in chunks.
    
    Args:
        object_name: Object key in MinIO
        workdir: Working directory
        chunk_size: Size of each chunk
        processor: Optional function to process each chunk
        
    Returns:
        List of results from processor (if provided)
    """
    downloader = ChunkedDownloader(chunk_size=chunk_size, auto_cleanup=True)
    results = []
    
    for chunk in downloader.iter_chunks(object_name, workdir):
        if processor:
            result = processor(chunk)
            results.append(result)
    
    return results


def get_file_chunk_count(object_name: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
    """Get the number of chunks a file will be split into."""
    downloader = ChunkedDownloader(chunk_size=chunk_size)
    metadata = downloader.get_object_metadata(object_name)
    return len(downloader.calculate_chunks(metadata.size))
