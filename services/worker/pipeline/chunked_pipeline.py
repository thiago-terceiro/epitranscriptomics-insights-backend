"""
Chunked Analysis Pipeline for Large Sequencing Data

Provides a pipeline for processing large sequencing data files in chunks
when working with memory/disk constraints. This module coordinates:

1. Chunked data ingestion (FAST5/FASTQ in batches)
2. Incremental preprocessing (basecall, align, eventalign)
3. Batch model execution
4. Result aggregation across chunks

Key Classes:
- ChunkedPipeline: Main orchestrator for chunked processing
- BatchProcessor: Handles individual chunk processing
- ResultAggregator: Merges results across chunks

Thread Safety:
    Each ChunkedPipeline instance should be used by a single worker.
    Multiple workers can process different jobs concurrently.
"""

import gzip
import json
import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

@dataclass
class ChunkedPipelineConfig:
    """Configuration for chunked processing pipeline."""
    
    # Chunk sizes
    fast5_batch_size: int = 100  # Number of FAST5 files per batch
    fastq_chunk_size: int = 100_000  # Number of reads per FASTQ chunk
    max_disk_usage_gb: float = 5.0  # Maximum disk usage before cleanup
    
    # Processing options
    auto_cleanup: bool = True  # Cleanup intermediate files after each chunk
    parallel_chunks: int = 1  # Number of chunks to process in parallel (1 = sequential)
    checkpoint_interval: int = 1  # Save checkpoint after every N chunks
    
    # Paths
    checkpoint_file: Optional[Path] = None  # Resume from checkpoint


@dataclass
class ChunkResult:
    """Result from processing a single chunk."""
    chunk_id: int
    chunk_type: str  # "fast5", "fastq", etc.
    input_files: List[str]
    output_files: Dict[str, str]
    metrics: Dict[str, Any]
    success: bool
    error: Optional[str] = None


@dataclass
class PipelineProgress:
    """Progress tracking for the chunked pipeline."""
    total_chunks: int
    chunks_completed: int = 0
    chunks_failed: int = 0
    current_chunk_id: Optional[int] = None
    current_stage: str = "idle"
    bytes_processed: int = 0
    total_bytes: int = 0
    
    @property
    def percent_complete(self) -> float:
        if self.total_chunks == 0:
            return 100.0
        return (self.chunks_completed / self.total_chunks) * 100


@dataclass
class PipelineCheckpoint:
    """Checkpoint for resuming interrupted processing."""
    job_id: str
    chunks_completed: List[int]
    partial_results: Dict[str, Any]
    config: Dict[str, Any]
    stage: str
    
    def save(self, path: Path) -> None:
        """Save checkpoint to file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump({
                "job_id": self.job_id,
                "chunks_completed": self.chunks_completed,
                "partial_results": self.partial_results,
                "config": self.config,
                "stage": self.stage,
            }, f, indent=2)
    
    @classmethod
    def load(cls, path: Path) -> "PipelineCheckpoint":
        """Load checkpoint from file."""
        with open(path) as f:
            data = json.load(f)
        return cls(**data)


# -----------------------------------------------------------------------------
# FAST5 File Batcher
# -----------------------------------------------------------------------------

class Fast5Batcher:
    """
    Batches FAST5 files for chunked processing.
    
    FAST5 files are individual signal files that can be processed in batches.
    This class organizes them into processable groups while respecting
    memory constraints.
    """
    
    def __init__(
        self,
        fast5_dir: Path,
        batch_size: int = 100,
        size_limit_bytes: Optional[int] = None,
    ):
        """
        Initialize FAST5 batcher.
        
        Args:
            fast5_dir: Directory containing FAST5 files
            batch_size: Maximum files per batch
            size_limit_bytes: Optional size limit per batch in bytes
        """
        self.fast5_dir = Path(fast5_dir)
        self.batch_size = batch_size
        self.size_limit_bytes = size_limit_bytes
        self._files: Optional[List[Path]] = None
    
    @property
    def files(self) -> List[Path]:
        """Get sorted list of all FAST5 files."""
        if self._files is None:
            self._files = sorted(self.fast5_dir.glob("*.fast5"))
            # Also check for POD5 files (newer format)
            self._files.extend(sorted(self.fast5_dir.glob("*.pod5")))
        return self._files
    
    @property
    def total_files(self) -> int:
        """Total number of FAST5/POD5 files."""
        return len(self.files)
    
    @property
    def total_batches(self) -> int:
        """Number of batches needed."""
        if self.total_files == 0:
            return 0
        return (self.total_files + self.batch_size - 1) // self.batch_size
    
    def iter_batches(self) -> Generator[Tuple[int, List[Path]], None, None]:
        """
        Iterate over batches of FAST5 files.
        
        Yields:
            Tuple of (batch_index, list of file paths)
        """
        if not self.files:
            return
        
        current_batch: List[Path] = []
        current_size = 0
        batch_index = 0
        
        for file_path in self.files:
            file_size = file_path.stat().st_size if file_path.exists() else 0
            
            # Check if adding this file would exceed limits
            would_exceed_count = len(current_batch) >= self.batch_size
            would_exceed_size = (
                self.size_limit_bytes is not None 
                and current_size + file_size > self.size_limit_bytes
                and len(current_batch) > 0
            )
            
            if would_exceed_count or would_exceed_size:
                yield batch_index, current_batch
                batch_index += 1
                current_batch = []
                current_size = 0
            
            current_batch.append(file_path)
            current_size += file_size
        
        # Yield final batch
        if current_batch:
            yield batch_index, current_batch
    
    def get_batch(self, batch_index: int) -> List[Path]:
        """Get a specific batch by index."""
        for idx, batch in self.iter_batches():
            if idx == batch_index:
                return batch
        raise IndexError(f"Batch {batch_index} does not exist")


# -----------------------------------------------------------------------------
# FASTQ Chunker
# -----------------------------------------------------------------------------

class FastqChunker:
    """
    Chunks FASTQ files for incremental processing.
    
    Reads FASTQ in streaming fashion and yields chunks of N reads.
    Supports both compressed (.gz) and uncompressed FASTQ files.
    """
    
    def __init__(
        self,
        fastq_path: Path,
        chunk_size: int = 100_000,  # reads per chunk
    ):
        """
        Initialize FASTQ chunker.
        
        Args:
            fastq_path: Path to FASTQ file (can be .gz)
            chunk_size: Number of reads per chunk
        """
        self.fastq_path = Path(fastq_path)
        self.chunk_size = chunk_size
        self._total_reads: Optional[int] = None
    
    @property
    def is_gzipped(self) -> bool:
        """Check if file is gzipped."""
        return str(self.fastq_path).endswith(".gz")
    
    def count_reads(self) -> int:
        """
        Count total reads in file (may be slow for large files).
        
        Caches result for subsequent calls.
        """
        if self._total_reads is not None:
            return self._total_reads
        
        count = 0
        opener = gzip.open if self.is_gzipped else open
        
        with opener(self.fastq_path, "rt") as f:
            for line in f:
                if line.startswith("@"):
                    count += 1
        
        self._total_reads = count
        return count
    
    def estimate_chunks(self) -> int:
        """Estimate number of chunks (without counting all reads)."""
        # Estimate based on file size
        file_size = self.fastq_path.stat().st_size
        # Average read is ~500 bytes compressed, ~2KB uncompressed
        avg_read_size = 500 if self.is_gzipped else 2000
        estimated_reads = file_size // avg_read_size
        return max(1, (estimated_reads + self.chunk_size - 1) // self.chunk_size)
    
    def iter_chunks(
        self,
        workdir: Path,
    ) -> Generator[Tuple[int, Path, int], None, None]:
        """
        Iterate over chunks of FASTQ data, writing each to a temp file.
        
        Args:
            workdir: Directory to write chunk files
            
        Yields:
            Tuple of (chunk_index, chunk_file_path, num_reads_in_chunk)
        """
        workdir = Path(workdir)
        workdir.mkdir(parents=True, exist_ok=True)
        
        opener = gzip.open if self.is_gzipped else open
        chunk_index = 0
        reads_in_chunk = 0
        chunk_file: Optional[Path] = None
        chunk_writer = None
        
        try:
            with opener(self.fastq_path, "rt") as f:
                lines_buffer: List[str] = []
                
                for line in f:
                    lines_buffer.append(line)
                    
                    # FASTQ has 4 lines per read
                    if len(lines_buffer) == 4:
                        # Start new chunk if needed
                        if chunk_writer is None:
                            chunk_file = workdir / f"chunk_{chunk_index:06d}.fastq.gz"
                            chunk_writer = gzip.open(chunk_file, "wt")
                            reads_in_chunk = 0
                        
                        # Write read to chunk
                        chunk_writer.writelines(lines_buffer)
                        reads_in_chunk += 1
                        lines_buffer = []
                        
                        # Check if chunk is complete
                        if reads_in_chunk >= self.chunk_size:
                            chunk_writer.close()
                            yield chunk_index, chunk_file, reads_in_chunk
                            
                            chunk_index += 1
                            chunk_writer = None
                            chunk_file = None
            
            # Yield final chunk
            if chunk_writer is not None:
                chunk_writer.close()
                yield chunk_index, chunk_file, reads_in_chunk
                
        finally:
            if chunk_writer is not None:
                chunk_writer.close()


# -----------------------------------------------------------------------------
# Batch Processor
# -----------------------------------------------------------------------------

class BatchProcessor:
    """
    Processes a single batch/chunk through the pipeline stages.
    
    Handles:
    - Basecalling (FAST5 -> FASTQ)
    - Alignment (FASTQ -> BAM)
    - Eventalign preparation
    - Model execution
    """
    
    def __init__(
        self,
        workdir: Path,
        reference: Path,
        models: List[str],
        mock: bool = False,
    ):
        """
        Initialize batch processor.
        
        Args:
            workdir: Working directory for this batch
            reference: Reference genome/transcriptome path
            models: List of models to run (cheui, m6anet, xpore)
            mock: Whether to run in mock mode
        """
        self.workdir = Path(workdir)
        self.reference = Path(reference)
        self.models = models
        self.mock = mock
        
        self.workdir.mkdir(parents=True, exist_ok=True)
    
    def process_fast5_batch(
        self,
        fast5_files: List[Path],
        batch_id: int,
    ) -> ChunkResult:
        """
        Process a batch of FAST5 files through basecalling.
        
        Args:
            fast5_files: List of FAST5 file paths
            batch_id: Batch identifier
            
        Returns:
            ChunkResult with output FASTQ path
        """
        batch_dir = self.workdir / f"batch_{batch_id:06d}"
        batch_dir.mkdir(exist_ok=True)
        
        # Create symlinks to FAST5 files in batch directory
        fast5_batch_dir = batch_dir / "fast5"
        fast5_batch_dir.mkdir(exist_ok=True)
        
        for f5_file in fast5_files:
            link_path = fast5_batch_dir / f5_file.name
            if not link_path.exists():
                try:
                    link_path.symlink_to(f5_file)
                except OSError:
                    # Fallback to copy if symlink not supported
                    shutil.copy2(f5_file, link_path)
        
        try:
            # Run basecalling
            from worker.pipeline.preprocess import basecall
            
            fastq, bam = basecall.basecall(
                fast5_dir=fast5_batch_dir,
                workdir=batch_dir,
                reference=self.reference if self.reference.exists() else None,
                mock=self.mock,
            )
            
            return ChunkResult(
                chunk_id=batch_id,
                chunk_type="fast5",
                input_files=[str(f) for f in fast5_files],
                output_files={
                    "fastq": str(fastq),
                    "bam": str(bam) if bam else None,
                },
                metrics={
                    "input_file_count": len(fast5_files),
                    "output_fastq_size": fastq.stat().st_size if fastq.exists() else 0,
                },
                success=True,
            )
            
        except Exception as e:
            logger.error(f"Batch {batch_id} processing failed: {e}")
            return ChunkResult(
                chunk_id=batch_id,
                chunk_type="fast5",
                input_files=[str(f) for f in fast5_files],
                output_files={},
                metrics={},
                success=False,
                error=str(e),
            )
    
    def process_fastq_chunk(
        self,
        fastq_chunk: Path,
        chunk_id: int,
    ) -> ChunkResult:
        """
        Process a FASTQ chunk through alignment.
        
        Args:
            fastq_chunk: Path to FASTQ chunk file
            chunk_id: Chunk identifier
            
        Returns:
            ChunkResult with output BAM path
        """
        chunk_dir = self.workdir / f"chunk_{chunk_id:06d}"
        chunk_dir.mkdir(exist_ok=True)
        
        try:
            from worker.pipeline.preprocess import align
            
            bam = align.align(
                fastq=fastq_chunk,
                reference=self.reference,
                workdir=chunk_dir,
                preset="splice",  # Use splice preset for RNA
                mock=self.mock,
            )
            
            return ChunkResult(
                chunk_id=chunk_id,
                chunk_type="fastq",
                input_files=[str(fastq_chunk)],
                output_files={"bam": str(bam)},
                metrics={
                    "input_size": fastq_chunk.stat().st_size,
                    "output_size": bam.stat().st_size if bam.exists() else 0,
                },
                success=True,
            )
            
        except Exception as e:
            logger.error(f"Chunk {chunk_id} alignment failed: {e}")
            return ChunkResult(
                chunk_id=chunk_id,
                chunk_type="fastq",
                input_files=[str(fastq_chunk)],
                output_files={},
                metrics={},
                success=False,
                error=str(e),
            )
    
    def run_model_on_chunk(
        self,
        model: str,
        fast5_dir: Path,
        fastq: Path,
        bam: Path,
        chunk_id: int,
    ) -> ChunkResult:
        """
        Run a modification detection model on a processed chunk.
        
        Args:
            model: Model name (cheui, m6anet, xpore)
            fast5_dir: Directory with FAST5 files for this chunk
            fastq: FASTQ file for this chunk
            bam: Aligned BAM file
            chunk_id: Chunk identifier
            
        Returns:
            ChunkResult with model outputs
        """
        chunk_dir = self.workdir / f"model_{model}_chunk_{chunk_id:06d}"
        chunk_dir.mkdir(exist_ok=True)
        
        try:
            if model == "cheui":
                from worker.runners import cheui_runner
                result = cheui_runner.run(
                    fast5_dir=fast5_dir,
                    fastq_gz=fastq,
                    ref_fa=str(self.reference),
                    workdir=chunk_dir,
                    mock=self.mock,
                )
            elif model == "m6anet":
                from worker.runners import m6anet_runner
                result = m6anet_runner.run(
                    fast5_dir=fast5_dir,
                    fastq_gz=fastq,
                    ref_fa=str(self.reference),
                    workdir=chunk_dir,
                    mock=self.mock,
                )
            elif model == "xpore":
                from worker.runners import xpore_runner
                result = xpore_runner.run(
                    fast5_dir=fast5_dir,
                    ref_fa=str(self.reference),
                    workdir=chunk_dir,
                    mock=self.mock,
                )
            else:
                raise ValueError(f"Unknown model: {model}")
            
            return ChunkResult(
                chunk_id=chunk_id,
                chunk_type=f"model_{model}",
                input_files=[str(fast5_dir), str(fastq), str(bam)],
                output_files=result,
                metrics={"model": model},
                success=True,
            )
            
        except Exception as e:
            logger.error(f"Model {model} failed on chunk {chunk_id}: {e}")
            return ChunkResult(
                chunk_id=chunk_id,
                chunk_type=f"model_{model}",
                input_files=[str(fast5_dir), str(fastq), str(bam)],
                output_files={},
                metrics={"model": model},
                success=False,
                error=str(e),
            )
    
    def cleanup(self) -> None:
        """Remove all intermediate files in workdir."""
        if self.workdir.exists():
            shutil.rmtree(self.workdir)


# -----------------------------------------------------------------------------
# Chunked Pipeline Orchestrator
# -----------------------------------------------------------------------------

class ChunkedPipeline:
    """
    Orchestrates chunked processing of large sequencing datasets.
    
    This is the main entry point for processing large files with memory
    constraints. It coordinates:
    
    1. Splitting input data into processable chunks
    2. Processing each chunk through the pipeline
    3. Cleaning up intermediate files
    4. Aggregating results across chunks
    5. Checkpointing for resume capability
    
    Usage:
        config = ChunkedPipelineConfig(
            fast5_batch_size=100,
            max_disk_usage_gb=5.0,
        )
        
        pipeline = ChunkedPipeline(
            job_id="job-123",
            workdir=Path("/work/job-123"),
            reference=Path("/refs/hg38.fa"),
            models=["cheui", "m6anet"],
            config=config,
        )
        
        # Process uploaded files
        results = pipeline.process_upload_session(
            session_id="session-abc",
            user_id="anonymous",
        )
    """
    
    def __init__(
        self,
        job_id: str,
        workdir: Path,
        reference: Path,
        models: List[str],
        config: Optional[ChunkedPipelineConfig] = None,
        mock: bool = False,
        progress_callback: Optional[Callable[[PipelineProgress], None]] = None,
    ):
        """
        Initialize chunked pipeline.
        
        Args:
            job_id: Unique job identifier
            workdir: Working directory for processing
            reference: Reference genome/transcriptome
            models: List of models to run
            config: Pipeline configuration
            mock: Whether to run in mock mode
            progress_callback: Optional callback for progress updates
        """
        self.job_id = job_id
        self.workdir = Path(workdir)
        self.reference = Path(reference)
        self.models = models
        self.config = config or ChunkedPipelineConfig()
        self.mock = mock
        self.progress_callback = progress_callback
        
        self.workdir.mkdir(parents=True, exist_ok=True)
        
        # Track chunk results
        self.chunk_results: List[ChunkResult] = []
        self.checkpoint: Optional[PipelineCheckpoint] = None
        
        # Load checkpoint if exists
        if self.config.checkpoint_file and self.config.checkpoint_file.exists():
            self.checkpoint = PipelineCheckpoint.load(self.config.checkpoint_file)
            logger.info(f"Loaded checkpoint: {len(self.checkpoint.chunks_completed)} chunks completed")
    
    def _update_progress(self, progress: PipelineProgress) -> None:
        """Update progress callback if set."""
        if self.progress_callback:
            self.progress_callback(progress)
    
    def _save_checkpoint(self, stage: str) -> None:
        """Save checkpoint for resume capability."""
        if self.config.checkpoint_file:
            checkpoint = PipelineCheckpoint(
                job_id=self.job_id,
                chunks_completed=[r.chunk_id for r in self.chunk_results if r.success],
                partial_results={
                    r.chunk_id: r.output_files 
                    for r in self.chunk_results if r.success
                },
                config={
                    "fast5_batch_size": self.config.fast5_batch_size,
                    "fastq_chunk_size": self.config.fastq_chunk_size,
                },
                stage=stage,
            )
            checkpoint.save(self.config.checkpoint_file)
    
    def _should_skip_chunk(self, chunk_id: int) -> bool:
        """Check if chunk was already processed (from checkpoint)."""
        if self.checkpoint:
            return chunk_id in self.checkpoint.chunks_completed
        return False
    
    def process_fast5_directory(
        self,
        fast5_dir: Path,
    ) -> List[ChunkResult]:
        """
        Process a directory of FAST5 files in batches.
        
        Args:
            fast5_dir: Directory containing FAST5 files
            
        Returns:
            List of ChunkResults from each batch
        """
        batcher = Fast5Batcher(
            fast5_dir=fast5_dir,
            batch_size=self.config.fast5_batch_size,
        )
        
        progress = PipelineProgress(
            total_chunks=batcher.total_batches,
            current_stage="basecalling",
        )
        
        results: List[ChunkResult] = []
        batch_processor = BatchProcessor(
            workdir=self.workdir / "processing",
            reference=self.reference,
            models=self.models,
            mock=self.mock,
        )
        
        for batch_id, files in batcher.iter_batches():
            if self._should_skip_chunk(batch_id):
                logger.info(f"Skipping batch {batch_id} (already processed)")
                progress.chunks_completed += 1
                continue
            
            progress.current_chunk_id = batch_id
            self._update_progress(progress)
            
            logger.info(f"Processing batch {batch_id}/{batcher.total_batches} ({len(files)} files)")
            
            result = batch_processor.process_fast5_batch(files, batch_id)
            results.append(result)
            self.chunk_results.append(result)
            
            if result.success:
                progress.chunks_completed += 1
            else:
                progress.chunks_failed += 1
            
            # Checkpoint periodically
            if (batch_id + 1) % self.config.checkpoint_interval == 0:
                self._save_checkpoint("basecalling")
            
            # Cleanup if auto_cleanup enabled
            if self.config.auto_cleanup and result.success:
                # Keep only the output files, remove intermediate
                pass  # Cleanup handled per-batch
            
            self._update_progress(progress)
        
        return results
    
    def process_fastq_chunked(
        self,
        fastq_path: Path,
    ) -> List[ChunkResult]:
        """
        Process a large FASTQ file in chunks.
        
        Args:
            fastq_path: Path to FASTQ file
            
        Returns:
            List of ChunkResults from each chunk
        """
        chunker = FastqChunker(
            fastq_path=fastq_path,
            chunk_size=self.config.fastq_chunk_size,
        )
        
        estimated_chunks = chunker.estimate_chunks()
        progress = PipelineProgress(
            total_chunks=estimated_chunks,
            current_stage="alignment",
            total_bytes=fastq_path.stat().st_size,
        )
        
        results: List[ChunkResult] = []
        batch_processor = BatchProcessor(
            workdir=self.workdir / "alignment",
            reference=self.reference,
            models=self.models,
            mock=self.mock,
        )
        
        chunks_workdir = self.workdir / "fastq_chunks"
        
        for chunk_id, chunk_file, num_reads in chunker.iter_chunks(chunks_workdir):
            if self._should_skip_chunk(chunk_id):
                logger.info(f"Skipping chunk {chunk_id} (already processed)")
                progress.chunks_completed += 1
                continue
            
            progress.current_chunk_id = chunk_id
            self._update_progress(progress)
            
            logger.info(f"Processing FASTQ chunk {chunk_id} ({num_reads} reads)")
            
            result = batch_processor.process_fastq_chunk(chunk_file, chunk_id)
            results.append(result)
            self.chunk_results.append(result)
            
            if result.success:
                progress.chunks_completed += 1
                progress.bytes_processed += chunk_file.stat().st_size
            else:
                progress.chunks_failed += 1
            
            # Cleanup chunk file after processing
            if self.config.auto_cleanup:
                chunk_file.unlink(missing_ok=True)
            
            # Checkpoint
            if (chunk_id + 1) % self.config.checkpoint_interval == 0:
                self._save_checkpoint("alignment")
            
            self._update_progress(progress)
        
        return results
    
    def cleanup_workdir(self) -> None:
        """Remove all working files."""
        if self.workdir.exists():
            shutil.rmtree(self.workdir)
            logger.info(f"Cleaned up workdir: {self.workdir}")
    
    def get_aggregated_results(self) -> Dict[str, Any]:
        """
        Get aggregated results from all processed chunks.
        
        Returns:
            Dictionary with aggregated metrics and outputs
        """
        successful = [r for r in self.chunk_results if r.success]
        failed = [r for r in self.chunk_results if not r.success]
        
        return {
            "job_id": self.job_id,
            "total_chunks": len(self.chunk_results),
            "successful_chunks": len(successful),
            "failed_chunks": len(failed),
            "chunk_results": [
                {
                    "chunk_id": r.chunk_id,
                    "type": r.chunk_type,
                    "success": r.success,
                    "outputs": r.output_files,
                    "error": r.error,
                }
                for r in self.chunk_results
            ],
            "errors": [r.error for r in failed if r.error],
        }
