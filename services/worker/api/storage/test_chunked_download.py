"""
Test suite for chunked download functionality.

Tests the ChunkedDownloader class for handling large files with memory constraints.
Uses mocking to avoid requiring actual MinIO connections during testing.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from worker.api.storage.chunked_download import (
    ChunkedDownloader,
    ChunkedSessionDownloader,
    ChunkInfo,
    ObjectMetadata,
    ChunkedDownloadProgress,
    ChunkedDownloadError,
    ObjectNotFoundError,
    RangeDownloadError,
    DEFAULT_CHUNK_SIZE,
    MIN_CHUNK_SIZE,
    MAX_CHUNK_SIZE,
    download_file_chunked,
    get_file_chunk_count,
)


class TestChunkCalculation:
    """Test chunk boundary calculations."""
    
    def test_small_file_single_chunk(self):
        """Files smaller than chunk size should be one chunk."""
        downloader = ChunkedDownloader(chunk_size=1024 * 1024 * 1024)  # 1GB
        
        # 100MB file
        chunks = downloader.calculate_chunks(100 * 1024 * 1024)
        assert len(chunks) == 1
        assert chunks[0] == (0, 100 * 1024 * 1024 - 1)
    
    def test_exact_chunk_size(self):
        """File exactly chunk size should be one chunk."""
        chunk_size = 100 * 1024 * 1024  # 100MB
        downloader = ChunkedDownloader(chunk_size=chunk_size)
        
        chunks = downloader.calculate_chunks(chunk_size)
        assert len(chunks) == 1
        assert chunks[0] == (0, chunk_size - 1)
    
    def test_multiple_chunks(self):
        """Large file should be split into multiple chunks."""
        chunk_size = 100 * 1024 * 1024  # 100MB
        downloader = ChunkedDownloader(chunk_size=chunk_size)
        
        # 350MB file -> 4 chunks (100, 100, 100, 50)
        file_size = 350 * 1024 * 1024
        chunks = downloader.calculate_chunks(file_size)
        
        assert len(chunks) == 4
        assert chunks[0] == (0, chunk_size - 1)
        assert chunks[1] == (chunk_size, 2 * chunk_size - 1)
        assert chunks[2] == (2 * chunk_size, 3 * chunk_size - 1)
        assert chunks[3] == (3 * chunk_size, file_size - 1)
    
    def test_empty_file(self):
        """Empty file should have no chunks (nothing to download)."""
        downloader = ChunkedDownloader(chunk_size=1024 * 1024)
        chunks = downloader.calculate_chunks(0)
        assert len(chunks) == 0
    
    def test_chunk_size_clamping(self):
        """Chunk size should be clamped to min/max bounds."""
        # Too small - should be clamped to minimum
        small_downloader = ChunkedDownloader(chunk_size=1024)  # 1KB
        assert small_downloader.chunk_size == MIN_CHUNK_SIZE
        
        # Too large - should be clamped to maximum
        large_downloader = ChunkedDownloader(chunk_size=100 * 1024 * 1024 * 1024)  # 100GB
        assert large_downloader.chunk_size == MAX_CHUNK_SIZE


class TestChunkedDownloadProgress:
    """Test progress tracking."""
    
    def test_download_percent_calculation(self):
        """Download percentage should be calculated correctly."""
        progress = ChunkedDownloadProgress(
            object_name="test.fast5",
            total_size=1000,
            chunk_size=100,
            total_chunks=10,
            bytes_downloaded=250,
        )
        assert progress.download_percent == 25.0
    
    def test_process_percent_calculation(self):
        """Process percentage should be calculated correctly."""
        progress = ChunkedDownloadProgress(
            object_name="test.fast5",
            total_size=1000,
            chunk_size=100,
            total_chunks=10,
            chunks_processed=3,
        )
        assert progress.process_percent == 30.0
    
    def test_zero_size_file_progress(self):
        """Zero-size file should report 100% progress."""
        progress = ChunkedDownloadProgress(
            object_name="empty.fast5",
            total_size=0,
            chunk_size=100,
            total_chunks=0,
        )
        assert progress.download_percent == 100.0
        assert progress.process_percent == 100.0


class TestChunkedDownloaderWithMocks:
    """Test downloader with mocked MinIO operations."""
    
    @patch('worker.api.storage.chunked_download.minio_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_get_object_metadata(self, mock_ensure, mock_minio):
        """Should retrieve object metadata correctly."""
        # Setup mock
        mock_stat = MagicMock()
        mock_stat.size = 5 * 1024 * 1024 * 1024  # 5GB
        mock_stat.content_type = "application/octet-stream"
        mock_stat.etag = "abc123"
        mock_stat.last_modified = None
        mock_minio.stat_object.return_value = mock_stat
        
        downloader = ChunkedDownloader()
        metadata = downloader.get_object_metadata("test/file.fast5")
        
        assert metadata.object_name == "test/file.fast5"
        assert metadata.size == 5 * 1024 * 1024 * 1024
        assert metadata.content_type == "application/octet-stream"
        mock_ensure.assert_called_once()
    
    @patch('worker.api.storage.chunked_download.s3_internal_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_download_range(self, mock_ensure, mock_s3):
        """Should download a byte range correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup mock response
            mock_body = MagicMock()
            mock_body.iter_chunks.return_value = [b"test data chunk"]
            mock_s3.get_object.return_value = {"Body": mock_body}
            
            downloader = ChunkedDownloader()
            local_path = Path(tmpdir) / "chunk.bin"
            
            bytes_written = downloader._download_range(
                "test/file.fast5",
                0,
                999,
                local_path,
            )
            
            assert bytes_written == 15  # len("test data chunk")
            assert local_path.exists()
            mock_s3.get_object.assert_called_once()
            
            # Check range header
            call_kwargs = mock_s3.get_object.call_args[1]
            assert call_kwargs["Range"] == "bytes=0-999"
    
    @patch('worker.api.storage.chunked_download.s3_internal_client')
    @patch('worker.api.storage.chunked_download.minio_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_iter_chunks_full_flow(self, mock_ensure, mock_minio, mock_s3):
        """Should iterate through all chunks of a file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup metadata mock - 300MB file with 100MB chunks = 3 chunks
            mock_stat = MagicMock()
            mock_stat.size = 300 * 1024 * 1024
            mock_stat.content_type = "application/octet-stream"
            mock_stat.etag = "abc123"
            mock_stat.last_modified = None
            mock_minio.stat_object.return_value = mock_stat
            
            # Setup download mock
            mock_body = MagicMock()
            mock_body.iter_chunks.return_value = [b"x" * 1024]  # 1KB fake data
            mock_s3.get_object.return_value = {"Body": mock_body}
            
            downloader = ChunkedDownloader(
                chunk_size=100 * 1024 * 1024,
                auto_cleanup=False,  # Don't cleanup for testing
            )
            
            chunks = list(downloader.iter_chunks(
                "test/file.fast5",
                Path(tmpdir),
            ))
            
            assert len(chunks) == 3
            assert chunks[0].chunk_number == 0
            assert chunks[1].chunk_number == 1
            assert chunks[2].chunk_number == 2
            assert chunks[2].is_last is True
            assert chunks[0].is_last is False
    
    @patch('worker.api.storage.chunked_download.s3_internal_client')
    @patch('worker.api.storage.chunked_download.minio_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_auto_cleanup(self, mock_ensure, mock_minio, mock_s3):
        """Should cleanup previous chunk when auto_cleanup is enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup mocks
            mock_stat = MagicMock()
            mock_stat.size = 200 * 1024 * 1024  # 2 chunks
            mock_stat.content_type = "application/octet-stream"
            mock_stat.etag = "abc123"
            mock_stat.last_modified = None
            mock_minio.stat_object.return_value = mock_stat
            
            mock_body = MagicMock()
            mock_body.iter_chunks.return_value = [b"x" * 100]
            mock_s3.get_object.return_value = {"Body": mock_body}
            
            downloader = ChunkedDownloader(
                chunk_size=100 * 1024 * 1024,
                auto_cleanup=True,
            )
            
            chunk_paths = []
            for chunk in downloader.iter_chunks("test/file.fast5", Path(tmpdir)):
                chunk_paths.append(chunk.local_path)
                
                if chunk.chunk_number == 1:
                    # First chunk should be cleaned up by now
                    assert not chunk_paths[0].exists()
            
            # After iteration, all chunks should be cleaned up
            for path in chunk_paths:
                assert not path.exists()
    
    @patch('worker.api.storage.chunked_download.s3_internal_client')
    @patch('worker.api.storage.chunked_download.minio_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_progress_callback(self, mock_ensure, mock_minio, mock_s3):
        """Should call progress callback for each chunk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup mocks
            mock_stat = MagicMock()
            mock_stat.size = 200 * 1024 * 1024
            mock_stat.content_type = "application/octet-stream"
            mock_stat.etag = "abc123"
            mock_stat.last_modified = None
            mock_minio.stat_object.return_value = mock_stat
            
            mock_body = MagicMock()
            mock_body.iter_chunks.return_value = [b"x" * 100]
            mock_s3.get_object.return_value = {"Body": mock_body}
            
            progress_updates = []
            
            def on_progress(progress: ChunkedDownloadProgress):
                progress_updates.append({
                    "chunks_downloaded": progress.chunks_downloaded,
                    "total_chunks": progress.total_chunks,
                })
            
            downloader = ChunkedDownloader(
                chunk_size=100 * 1024 * 1024,
                auto_cleanup=False,
                progress_callback=on_progress,
            )
            
            list(downloader.iter_chunks("test/file.fast5", Path(tmpdir)))
            
            assert len(progress_updates) == 2
            assert progress_updates[0]["chunks_downloaded"] == 1
            assert progress_updates[1]["chunks_downloaded"] == 2


class TestChunkInfo:
    """Test ChunkInfo dataclass."""
    
    def test_chunk_info_creation(self):
        """Should create ChunkInfo with all fields."""
        chunk = ChunkInfo(
            chunk_number=0,
            start_byte=0,
            end_byte=1023,
            size=1024,
            local_path=Path("/tmp/chunk_0.bin"),
            object_name="test/file.fast5",
            is_last=False,
        )
        
        assert chunk.chunk_number == 0
        assert chunk.size == 1024
        assert chunk.is_last is False


class TestConvenienceFunctions:
    """Test module-level convenience functions."""
    
    @patch('worker.api.storage.chunked_download.minio_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_get_file_chunk_count(self, mock_ensure, mock_minio):
        """Should return correct chunk count."""
        mock_stat = MagicMock()
        mock_stat.size = 500 * 1024 * 1024  # 500MB
        mock_stat.content_type = "application/octet-stream"
        mock_stat.etag = "abc123"
        mock_stat.last_modified = None
        mock_minio.stat_object.return_value = mock_stat
        
        # With 100MB chunks, should be 5 chunks
        count = get_file_chunk_count("test/file.fast5", chunk_size=100 * 1024 * 1024)
        assert count == 5


class TestChunkedSessionDownloader:
    """Test session-level chunked downloads."""
    
    @patch('worker.api.storage.upload_manager.UploadSession.list_session_files')
    @patch('worker.api.storage.chunked_download.minio_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_list_files(self, mock_ensure, mock_minio, mock_list_files):
        """Should list all files with metadata."""
        mock_list_files.return_value = ["uploads/user/file1.fast5", "uploads/user/file2.fastq"]
        
        mock_stat = MagicMock()
        mock_stat.size = 100 * 1024 * 1024
        mock_stat.content_type = "application/octet-stream"
        mock_stat.etag = "abc123"
        mock_stat.last_modified = None
        mock_minio.stat_object.return_value = mock_stat
        
        session_downloader = ChunkedSessionDownloader(
            session_id="test-session",
            user_id="test-user",
            chunk_size=1024 * 1024 * 1024,
        )
        
        files = session_downloader.list_files()
        assert len(files) == 2
    
    def test_needs_chunking(self):
        """Should correctly identify files needing chunked processing."""
        session_downloader = ChunkedSessionDownloader(
            session_id="test-session",
            user_id="test-user",
            chunk_size=1024 * 1024 * 1024,  # 1GB
        )
        
        small_file = ObjectMetadata(
            object_name="small.fast5",
            size=100 * 1024 * 1024,  # 100MB
            content_type="application/octet-stream",
            etag="abc",
        )
        
        large_file = ObjectMetadata(
            object_name="large.fast5",
            size=5 * 1024 * 1024 * 1024,  # 5GB
            content_type="application/octet-stream",
            etag="def",
        )
        
        assert session_downloader.needs_chunking(small_file) is False
        assert session_downloader.needs_chunking(large_file) is True


class TestCustomExceptions:
    """Test custom exception hierarchy."""
    
    def test_exception_hierarchy(self):
        """All custom exceptions should inherit from ChunkedDownloadError."""
        assert issubclass(ObjectNotFoundError, ChunkedDownloadError)
        assert issubclass(RangeDownloadError, ChunkedDownloadError)
    
    @patch('worker.api.storage.chunked_download.minio_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_object_not_found_raises_correct_exception(self, mock_ensure, mock_minio):
        """Should raise ObjectNotFoundError for missing objects."""
        from minio.error import S3Error
        
        mock_minio.stat_object.side_effect = S3Error(
            "GET", "bucket", "NoSuchKey", "The specified key does not exist.",
            {}, None
        )
        
        downloader = ChunkedDownloader()
        
        with pytest.raises(ObjectNotFoundError):
            downloader.get_object_metadata("nonexistent/file.fast5")
    
    @patch('worker.api.storage.chunked_download.minio_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_generic_s3_error_raises_chunked_download_error(self, mock_ensure, mock_minio):
        """Should raise ChunkedDownloadError for generic S3 errors."""
        from minio.error import S3Error
        
        mock_minio.stat_object.side_effect = S3Error(
            "GET", "bucket", "InternalError", "Internal server error",
            {}, None
        )
        
        downloader = ChunkedDownloader()
        
        with pytest.raises(ChunkedDownloadError):
            downloader.get_object_metadata("some/file.fast5")


class TestRetryLogic:
    """Test retry behavior for transient failures."""
    
    @patch('worker.api.storage.chunked_download.time.sleep')  # Speed up test
    @patch('worker.api.storage.chunked_download.s3_internal_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_retry_on_transient_error(self, mock_ensure, mock_s3, mock_sleep):
        """Should retry on transient network errors."""
        from botocore.exceptions import ClientError
        
        # First call fails, second succeeds
        mock_body = MagicMock()
        mock_body.iter_chunks.return_value = [b"test data"]
        
        mock_s3.get_object.side_effect = [
            ClientError({"Error": {"Code": "RequestTimeout"}}, "GetObject"),
            {"Body": mock_body},
        ]
        
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = ChunkedDownloader()
            local_path = Path(tmpdir) / "chunk.bin"
            
            # Should succeed after retry
            bytes_written = downloader._download_range(
                "test/file.fast5", 0, 999, local_path
            )
            
            assert bytes_written == 9  # len("test data")
            assert mock_s3.get_object.call_count == 2
            mock_sleep.assert_called_once()  # Should have slept between retries


class TestDownloadFileChunkedConvenience:
    """Test the download_file_chunked convenience function."""
    
    @patch('worker.api.storage.chunked_download.s3_internal_client')
    @patch('worker.api.storage.chunked_download.minio_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_download_file_chunked_with_processor(self, mock_ensure, mock_minio, mock_s3):
        """Should process each chunk and return results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup mocks
            mock_stat = MagicMock()
            mock_stat.size = 200 * 1024 * 1024  # 2 chunks at 100MB each
            mock_stat.content_type = "application/octet-stream"
            mock_stat.etag = "abc123"
            mock_stat.last_modified = None
            mock_minio.stat_object.return_value = mock_stat
            
            mock_body = MagicMock()
            mock_body.iter_chunks.return_value = [b"chunk_data"]
            mock_s3.get_object.return_value = {"Body": mock_body}
            
            # Track chunks processed
            processed_chunks = []
            
            def process_chunk(chunk):
                processed_chunks.append(chunk.chunk_number)
                return f"result_{chunk.chunk_number}"
            
            results = download_file_chunked(
                "test/file.fast5",
                Path(tmpdir),
                chunk_size=100 * 1024 * 1024,
                processor=process_chunk,
            )
            
            assert len(results) == 2
            assert results == ["result_0", "result_1"]
            assert processed_chunks == [0, 1]


class TestEmptyFileHandling:
    """Test handling of empty files."""
    
    @patch('worker.api.storage.chunked_download.s3_internal_client')
    @patch('worker.api.storage.chunked_download.minio_client')
    @patch('worker.api.storage.chunked_download.ensure_bucket')
    def test_empty_file_iter_chunks(self, mock_ensure, mock_minio, mock_s3):
        """Empty file should yield no chunks."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mock_stat = MagicMock()
            mock_stat.size = 0  # Empty file
            mock_stat.content_type = "application/octet-stream"
            mock_stat.etag = "d41d8cd98f00b204e9800998ecf8427e"
            mock_stat.last_modified = None
            mock_minio.stat_object.return_value = mock_stat
            
            downloader = ChunkedDownloader(chunk_size=100 * 1024 * 1024)
            chunks = list(downloader.iter_chunks("test/empty.fast5", Path(tmpdir)))
            
            assert len(chunks) == 0
            # No download should have been attempted
            mock_s3.get_object.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

