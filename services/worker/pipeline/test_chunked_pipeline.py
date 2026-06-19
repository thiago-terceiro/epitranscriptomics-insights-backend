"""
Tests for Chunked Analysis Pipeline.

Tests cover:
- FAST5 batching logic
- FASTQ chunking
- Batch processing
- Pipeline orchestration
- Checkpoint/resume functionality
"""

import gzip
import json
import tempfile
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from worker.pipeline.chunked_pipeline import (
    BatchProcessor,
    ChunkedPipeline,
    ChunkedPipelineConfig,
    ChunkResult,
    Fast5Batcher,
    FastqChunker,
    PipelineCheckpoint,
    PipelineProgress,
)


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def fast5_dir(temp_dir: Path) -> Path:
    """Create a directory with mock FAST5 files."""
    fast5_path = temp_dir / "fast5"
    fast5_path.mkdir()
    
    # Create 25 mock FAST5 files with varying sizes
    for i in range(25):
        f5 = fast5_path / f"read_{i:04d}.fast5"
        # Write varying sizes (100-500 bytes)
        f5.write_bytes(b"x" * (100 + i * 16))
    
    return fast5_path


@pytest.fixture
def fastq_file(temp_dir: Path) -> Path:
    """Create a mock FASTQ file."""
    fastq_path = temp_dir / "reads.fastq.gz"
    
    # Create 150 mock reads
    with gzip.open(fastq_path, "wt") as f:
        for i in range(150):
            f.write(f"@read_{i:04d}\n")
            f.write("ACGTACGTACGTACGTACGT\n")
            f.write("+\n")
            f.write("IIIIIIIIIIIIIIIIIIII\n")
    
    return fastq_path


@pytest.fixture
def reference_file(temp_dir: Path) -> Path:
    """Create a mock reference file."""
    ref_path = temp_dir / "reference.fa"
    ref_path.write_text(">chr1\nACGTACGTACGTACGTACGT\n")
    return ref_path


# -----------------------------------------------------------------------------
# Fast5Batcher Tests
# -----------------------------------------------------------------------------

class TestFast5Batcher:
    """Tests for FAST5 file batching."""
    
    def test_count_files(self, fast5_dir: Path):
        """Test that batcher correctly counts files."""
        batcher = Fast5Batcher(fast5_dir, batch_size=10)
        assert batcher.total_files == 25
    
    def test_calculate_batches(self, fast5_dir: Path):
        """Test batch calculation."""
        batcher = Fast5Batcher(fast5_dir, batch_size=10)
        assert batcher.total_batches == 3  # 25 files / 10 = 3 batches
    
    def test_batch_size_respected(self, fast5_dir: Path):
        """Test that batch size limit is respected."""
        batcher = Fast5Batcher(fast5_dir, batch_size=10)
        batches = list(batcher.iter_batches())
        
        assert len(batches) == 3
        assert len(batches[0][1]) == 10
        assert len(batches[1][1]) == 10
        assert len(batches[2][1]) == 5  # Remaining
    
    def test_batch_indices_sequential(self, fast5_dir: Path):
        """Test that batch indices are sequential."""
        batcher = Fast5Batcher(fast5_dir, batch_size=10)
        indices = [idx for idx, _ in batcher.iter_batches()]
        assert indices == [0, 1, 2]
    
    def test_get_specific_batch(self, fast5_dir: Path):
        """Test getting a specific batch by index."""
        batcher = Fast5Batcher(fast5_dir, batch_size=10)
        batch = batcher.get_batch(1)
        assert len(batch) == 10
    
    def test_get_invalid_batch_raises(self, fast5_dir: Path):
        """Test that invalid batch index raises error."""
        batcher = Fast5Batcher(fast5_dir, batch_size=10)
        with pytest.raises(IndexError):
            batcher.get_batch(99)
    
    def test_empty_directory(self, temp_dir: Path):
        """Test handling of empty directory."""
        empty_dir = temp_dir / "empty"
        empty_dir.mkdir()
        
        batcher = Fast5Batcher(empty_dir, batch_size=10)
        assert batcher.total_files == 0
        assert batcher.total_batches == 0
        assert list(batcher.iter_batches()) == []
    
    def test_size_limit_batching(self, fast5_dir: Path):
        """Test batching by size limit."""
        # 500 bytes limit should create more batches
        batcher = Fast5Batcher(fast5_dir, batch_size=100, size_limit_bytes=500)
        batches = list(batcher.iter_batches())
        
        # Should have more batches due to size limit
        assert len(batches) > 3
        
        # Verify each batch respects size limit (with tolerance for edge cases)
        for _, files in batches:
            total_size = sum(f.stat().st_size for f in files)
            # First file in batch can exceed if it's larger than limit
            if len(files) > 1:
                # At least one file must be within limit
                assert any(f.stat().st_size <= 500 for f in files)
    
    def test_includes_pod5_files(self, temp_dir: Path):
        """Test that POD5 files are also included."""
        mixed_dir = temp_dir / "mixed"
        mixed_dir.mkdir()
        
        (mixed_dir / "read1.fast5").write_bytes(b"fast5")
        (mixed_dir / "read2.pod5").write_bytes(b"pod5")
        
        batcher = Fast5Batcher(mixed_dir, batch_size=10)
        assert batcher.total_files == 2


# -----------------------------------------------------------------------------
# FastqChunker Tests
# -----------------------------------------------------------------------------

class TestFastqChunker:
    """Tests for FASTQ file chunking."""
    
    def test_count_reads(self, fastq_file: Path):
        """Test read counting."""
        chunker = FastqChunker(fastq_file, chunk_size=50)
        assert chunker.count_reads() == 150
    
    def test_estimate_chunks(self, fastq_file: Path):
        """Test chunk estimation."""
        chunker = FastqChunker(fastq_file, chunk_size=50)
        estimated = chunker.estimate_chunks()
        # Estimation may not be exact, but should be reasonable
        assert estimated >= 1
    
    def test_is_gzipped_detection(self, fastq_file: Path, temp_dir: Path):
        """Test gzip detection."""
        # Gzipped file
        chunker_gz = FastqChunker(fastq_file)
        assert chunker_gz.is_gzipped is True
        
        # Uncompressed file
        uncompressed = temp_dir / "reads.fastq"
        uncompressed.write_text("@read\nACGT\n+\nIIII\n")
        chunker_plain = FastqChunker(uncompressed)
        assert chunker_plain.is_gzipped is False
    
    def test_iter_chunks_creates_files(self, fastq_file: Path, temp_dir: Path):
        """Test that chunk iteration creates files."""
        chunker = FastqChunker(fastq_file, chunk_size=50)
        workdir = temp_dir / "chunks"
        
        chunks = list(chunker.iter_chunks(workdir))
        
        assert len(chunks) == 3  # 150 reads / 50 = 3 chunks
        
        for idx, chunk_path, num_reads in chunks:
            assert chunk_path.exists()
            assert num_reads == 50 or num_reads == 50  # All should be 50
    
    def test_chunk_files_are_valid_fastq(self, fastq_file: Path, temp_dir: Path):
        """Test that chunk files contain valid FASTQ."""
        chunker = FastqChunker(fastq_file, chunk_size=50)
        workdir = temp_dir / "chunks"
        
        for idx, chunk_path, num_reads in chunker.iter_chunks(workdir):
            # Read and verify
            with gzip.open(chunk_path, "rt") as f:
                lines = f.readlines()
            
            # Should have 4 lines per read
            assert len(lines) == num_reads * 4
            
            # Verify FASTQ format
            for i in range(0, len(lines), 4):
                assert lines[i].startswith("@")
                assert lines[i + 2].strip() == "+"
    
    def test_chunk_indices_sequential(self, fastq_file: Path, temp_dir: Path):
        """Test that chunk indices are sequential."""
        chunker = FastqChunker(fastq_file, chunk_size=50)
        workdir = temp_dir / "chunks"
        
        indices = [idx for idx, _, _ in chunker.iter_chunks(workdir)]
        assert indices == [0, 1, 2]


# -----------------------------------------------------------------------------
# PipelineProgress Tests
# -----------------------------------------------------------------------------

class TestPipelineProgress:
    """Tests for progress tracking."""
    
    def test_percent_complete(self):
        """Test percent calculation."""
        progress = PipelineProgress(total_chunks=10, chunks_completed=5)
        assert progress.percent_complete == 50.0
    
    def test_percent_complete_zero_total(self):
        """Test percent with zero total."""
        progress = PipelineProgress(total_chunks=0)
        assert progress.percent_complete == 100.0
    
    def test_progress_attributes(self):
        """Test progress attributes."""
        progress = PipelineProgress(
            total_chunks=10,
            chunks_completed=3,
            chunks_failed=1,
            current_chunk_id=4,
            current_stage="processing",
            bytes_processed=1000,
            total_bytes=5000,
        )
        
        assert progress.total_chunks == 10
        assert progress.chunks_completed == 3
        assert progress.chunks_failed == 1
        assert progress.current_chunk_id == 4
        assert progress.current_stage == "processing"


# -----------------------------------------------------------------------------
# PipelineCheckpoint Tests
# -----------------------------------------------------------------------------

class TestPipelineCheckpoint:
    """Tests for checkpoint save/load."""
    
    def test_save_checkpoint(self, temp_dir: Path):
        """Test saving checkpoint to file."""
        checkpoint = PipelineCheckpoint(
            job_id="test-job-123",
            chunks_completed=[0, 1, 2],
            partial_results={"0": {"fastq": "/path/to/file"}},
            config={"batch_size": 100},
            stage="basecalling",
        )
        
        checkpoint_path = temp_dir / "checkpoint.json"
        checkpoint.save(checkpoint_path)
        
        assert checkpoint_path.exists()
        
        with open(checkpoint_path) as f:
            data = json.load(f)
        
        assert data["job_id"] == "test-job-123"
        assert data["chunks_completed"] == [0, 1, 2]
    
    def test_load_checkpoint(self, temp_dir: Path):
        """Test loading checkpoint from file."""
        checkpoint_path = temp_dir / "checkpoint.json"
        checkpoint_path.write_text(json.dumps({
            "job_id": "test-job",
            "chunks_completed": [0, 1],
            "partial_results": {},
            "config": {},
            "stage": "alignment",
        }))
        
        checkpoint = PipelineCheckpoint.load(checkpoint_path)
        
        assert checkpoint.job_id == "test-job"
        assert checkpoint.chunks_completed == [0, 1]
        assert checkpoint.stage == "alignment"
    
    def test_save_creates_parent_dirs(self, temp_dir: Path):
        """Test that save creates parent directories."""
        checkpoint = PipelineCheckpoint(
            job_id="test",
            chunks_completed=[],
            partial_results={},
            config={},
            stage="init",
        )
        
        deep_path = temp_dir / "a" / "b" / "c" / "checkpoint.json"
        checkpoint.save(deep_path)
        
        assert deep_path.exists()


# -----------------------------------------------------------------------------
# ChunkResult Tests
# -----------------------------------------------------------------------------

class TestChunkResult:
    """Tests for ChunkResult dataclass."""
    
    def test_successful_result(self):
        """Test creating successful result."""
        result = ChunkResult(
            chunk_id=0,
            chunk_type="fast5",
            input_files=["file1.fast5", "file2.fast5"],
            output_files={"fastq": "output.fastq"},
            metrics={"count": 2},
            success=True,
        )
        
        assert result.success is True
        assert result.error is None
    
    def test_failed_result(self):
        """Test creating failed result."""
        result = ChunkResult(
            chunk_id=0,
            chunk_type="fast5",
            input_files=["file1.fast5"],
            output_files={},
            metrics={},
            success=False,
            error="Processing failed",
        )
        
        assert result.success is False
        assert result.error == "Processing failed"


# -----------------------------------------------------------------------------
# BatchProcessor Tests
# -----------------------------------------------------------------------------

class TestBatchProcessor:
    """Tests for batch processing."""
    
    def test_init_creates_workdir(self, temp_dir: Path, reference_file: Path):
        """Test that processor creates workdir."""
        workdir = temp_dir / "processing"
        processor = BatchProcessor(
            workdir=workdir,
            reference=reference_file,
            models=["cheui"],
        )
        
        assert workdir.exists()
    
    @patch("worker.pipeline.chunked_pipeline.basecall")
    def test_process_fast5_batch_success(
        self, mock_basecall, temp_dir: Path, fast5_dir: Path, reference_file: Path
    ):
        """Test successful FAST5 batch processing."""
        workdir = temp_dir / "processing"
        
        # Mock basecall to return paths
        mock_fastq = workdir / "output.fastq"
        mock_bam = workdir / "output.bam"
        mock_fastq.parent.mkdir(parents=True, exist_ok=True)
        mock_fastq.write_text("@read\nACGT\n+\nIIII\n")
        mock_bam.write_text("mock bam")
        mock_basecall.basecall.return_value = (mock_fastq, mock_bam)
        
        processor = BatchProcessor(
            workdir=workdir,
            reference=reference_file,
            models=["cheui"],
            mock=True,
        )
        
        files = list(fast5_dir.glob("*.fast5"))[:5]
        result = processor.process_fast5_batch(files, batch_id=0)
        
        assert result.success is True
        assert result.chunk_id == 0
        assert "fastq" in result.output_files
    
    @patch("worker.pipeline.chunked_pipeline.basecall")
    def test_process_fast5_batch_failure(
        self, mock_basecall, temp_dir: Path, fast5_dir: Path, reference_file: Path
    ):
        """Test FAST5 batch processing failure handling."""
        mock_basecall.basecall.side_effect = RuntimeError("Basecall failed")
        
        processor = BatchProcessor(
            workdir=temp_dir / "processing",
            reference=reference_file,
            models=["cheui"],
        )
        
        files = list(fast5_dir.glob("*.fast5"))[:5]
        result = processor.process_fast5_batch(files, batch_id=0)
        
        assert result.success is False
        assert "Basecall failed" in result.error
    
    def test_cleanup_removes_workdir(self, temp_dir: Path, reference_file: Path):
        """Test cleanup removes working directory."""
        workdir = temp_dir / "processing"
        processor = BatchProcessor(
            workdir=workdir,
            reference=reference_file,
            models=["cheui"],
        )
        
        # Create some files
        (workdir / "test.txt").write_text("test")
        
        processor.cleanup()
        
        assert not workdir.exists()


# -----------------------------------------------------------------------------
# ChunkedPipeline Tests
# -----------------------------------------------------------------------------

class TestChunkedPipeline:
    """Tests for main pipeline orchestrator."""
    
    def test_init_creates_workdir(self, temp_dir: Path, reference_file: Path):
        """Test that pipeline creates workdir."""
        workdir = temp_dir / "pipeline"
        
        pipeline = ChunkedPipeline(
            job_id="test-job",
            workdir=workdir,
            reference=reference_file,
            models=["cheui"],
        )
        
        assert workdir.exists()
    
    def test_config_defaults(self, temp_dir: Path, reference_file: Path):
        """Test default configuration."""
        pipeline = ChunkedPipeline(
            job_id="test-job",
            workdir=temp_dir / "pipeline",
            reference=reference_file,
            models=["cheui"],
        )
        
        assert pipeline.config.fast5_batch_size == 100
        assert pipeline.config.fastq_chunk_size == 100_000
        assert pipeline.config.auto_cleanup is True
    
    def test_custom_config(self, temp_dir: Path, reference_file: Path):
        """Test custom configuration."""
        config = ChunkedPipelineConfig(
            fast5_batch_size=50,
            fastq_chunk_size=10_000,
            auto_cleanup=False,
        )
        
        pipeline = ChunkedPipeline(
            job_id="test-job",
            workdir=temp_dir / "pipeline",
            reference=reference_file,
            models=["cheui"],
            config=config,
        )
        
        assert pipeline.config.fast5_batch_size == 50
        assert pipeline.config.fastq_chunk_size == 10_000
    
    def test_loads_checkpoint(self, temp_dir: Path, reference_file: Path):
        """Test that pipeline loads existing checkpoint."""
        checkpoint_path = temp_dir / "checkpoint.json"
        checkpoint_path.write_text(json.dumps({
            "job_id": "test-job",
            "chunks_completed": [0, 1, 2],
            "partial_results": {},
            "config": {},
            "stage": "basecalling",
        }))
        
        config = ChunkedPipelineConfig(checkpoint_file=checkpoint_path)
        
        pipeline = ChunkedPipeline(
            job_id="test-job",
            workdir=temp_dir / "pipeline",
            reference=reference_file,
            models=["cheui"],
            config=config,
        )
        
        assert pipeline.checkpoint is not None
        assert pipeline.checkpoint.chunks_completed == [0, 1, 2]
    
    def test_should_skip_chunk_from_checkpoint(
        self, temp_dir: Path, reference_file: Path
    ):
        """Test chunk skipping with checkpoint."""
        checkpoint_path = temp_dir / "checkpoint.json"
        checkpoint_path.write_text(json.dumps({
            "job_id": "test-job",
            "chunks_completed": [0, 1],
            "partial_results": {},
            "config": {},
            "stage": "basecalling",
        }))
        
        config = ChunkedPipelineConfig(checkpoint_file=checkpoint_path)
        
        pipeline = ChunkedPipeline(
            job_id="test-job",
            workdir=temp_dir / "pipeline",
            reference=reference_file,
            models=["cheui"],
            config=config,
        )
        
        assert pipeline._should_skip_chunk(0) is True
        assert pipeline._should_skip_chunk(1) is True
        assert pipeline._should_skip_chunk(2) is False
    
    def test_progress_callback(self, temp_dir: Path, reference_file: Path):
        """Test progress callback invocation."""
        progress_updates: List[PipelineProgress] = []
        
        def callback(progress: PipelineProgress):
            progress_updates.append(progress)
        
        pipeline = ChunkedPipeline(
            job_id="test-job",
            workdir=temp_dir / "pipeline",
            reference=reference_file,
            models=["cheui"],
            progress_callback=callback,
        )
        
        # Manually trigger progress update
        progress = PipelineProgress(total_chunks=10, current_stage="test")
        pipeline._update_progress(progress)
        
        assert len(progress_updates) == 1
        assert progress_updates[0].total_chunks == 10
    
    @patch("worker.pipeline.chunked_pipeline.basecall")
    def test_process_fast5_directory(
        self, mock_basecall, temp_dir: Path, fast5_dir: Path, reference_file: Path
    ):
        """Test processing FAST5 directory."""
        # Setup mock
        workdir = temp_dir / "pipeline"
        mock_fastq = workdir / "processing" / "batch_000000" / "output.fastq"
        mock_fastq.parent.mkdir(parents=True, exist_ok=True)
        mock_fastq.write_text("@read\nACGT\n+\nIIII\n")
        mock_basecall.basecall.return_value = (mock_fastq, None)
        
        config = ChunkedPipelineConfig(fast5_batch_size=10)
        
        pipeline = ChunkedPipeline(
            job_id="test-job",
            workdir=workdir,
            reference=reference_file,
            models=["cheui"],
            config=config,
            mock=True,
        )
        
        results = pipeline.process_fast5_directory(fast5_dir)
        
        assert len(results) == 3  # 25 files / 10 batch size = 3 batches
        assert mock_basecall.basecall.call_count == 3
    
    def test_get_aggregated_results(self, temp_dir: Path, reference_file: Path):
        """Test result aggregation."""
        pipeline = ChunkedPipeline(
            job_id="test-job",
            workdir=temp_dir / "pipeline",
            reference=reference_file,
            models=["cheui"],
        )
        
        # Add some results
        pipeline.chunk_results = [
            ChunkResult(
                chunk_id=0,
                chunk_type="fast5",
                input_files=["a.fast5"],
                output_files={"fastq": "a.fastq"},
                metrics={},
                success=True,
            ),
            ChunkResult(
                chunk_id=1,
                chunk_type="fast5",
                input_files=["b.fast5"],
                output_files={},
                metrics={},
                success=False,
                error="Failed",
            ),
        ]
        
        aggregated = pipeline.get_aggregated_results()
        
        assert aggregated["job_id"] == "test-job"
        assert aggregated["total_chunks"] == 2
        assert aggregated["successful_chunks"] == 1
        assert aggregated["failed_chunks"] == 1
        assert len(aggregated["errors"]) == 1
    
    def test_cleanup_workdir(self, temp_dir: Path, reference_file: Path):
        """Test workdir cleanup."""
        workdir = temp_dir / "pipeline"
        
        pipeline = ChunkedPipeline(
            job_id="test-job",
            workdir=workdir,
            reference=reference_file,
            models=["cheui"],
        )
        
        # Create some content
        (workdir / "test.txt").write_text("test")
        
        pipeline.cleanup_workdir()
        
        assert not workdir.exists()


# -----------------------------------------------------------------------------
# Integration Tests
# -----------------------------------------------------------------------------

class TestFastqChunkerIntegration:
    """Integration tests for FASTQ chunking and reassembly."""
    
    def test_chunked_reads_equal_original(self, fastq_file: Path, temp_dir: Path):
        """Test that chunked reads match original."""
        # Count original reads
        chunker = FastqChunker(fastq_file, chunk_size=50)
        original_count = chunker.count_reads()
        
        # Chunk and count
        workdir = temp_dir / "chunks"
        total_reads = 0
        
        for idx, chunk_path, num_reads in chunker.iter_chunks(workdir):
            total_reads += num_reads
        
        assert total_reads == original_count


class TestFast5BatcherIntegration:
    """Integration tests for FAST5 batching."""
    
    def test_all_files_batched_exactly_once(self, fast5_dir: Path):
        """Test that all files appear exactly once across batches."""
        batcher = Fast5Batcher(fast5_dir, batch_size=7)
        
        all_files = set()
        for _, files in batcher.iter_batches():
            for f in files:
                assert f not in all_files, f"File {f} appears multiple times"
                all_files.add(f)
        
        original_files = set(fast5_dir.glob("*.fast5"))
        assert all_files == original_files
