"""
Unit tests for dorado_runner.py.

Covers:
- build_dorado_command: flag composition
- run_dorado_basecall: success, no-BAM-error, subprocess failure, mock mode
- run_uploaded_pod5: happy path, POD5 deleted on failure, cleanup on success
"""

import subprocess
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Patch at module level before importing the runner so env vars apply cleanly
_RUNNER_PATH = "worker.runners.dorado_runner"


@pytest.fixture()
def tmp(tmp_path: Path) -> Path:
    return tmp_path


# ---------------------------------------------------------------------------
# build_dorado_command
# ---------------------------------------------------------------------------

class TestBuildDoradoCommand:
    def test_basic_flags_present(self, tmp: Path) -> None:
        from worker.runners.dorado_runner import build_dorado_command, DORADO_BIN, DORADO_MODEL, DORADO_DEVICE

        pod5 = tmp / "reads.pod5"
        out_dir = tmp / "out"
        cmd = build_dorado_command(pod5, out_dir)

        assert cmd[0] == DORADO_BIN
        assert "basecaller" in cmd
        assert "--device" in cmd
        assert DORADO_DEVICE in cmd
        assert "--emit-moves" in cmd
        assert DORADO_MODEL in cmd
        assert "--modified-bases" in cmd
        assert "m6A_DRACH" in cmd
        assert "--output-dir" in cmd
        assert str(out_dir) in cmd

    def test_reference_flag_added_when_provided(self, tmp: Path) -> None:
        from worker.runners.dorado_runner import build_dorado_command

        pod5 = tmp / "reads.pod5"
        out_dir = tmp / "out"
        ref = tmp / "hg38.fa"
        cmd = build_dorado_command(pod5, out_dir, reference_path=ref)

        assert "--reference" in cmd
        assert str(ref) in cmd

    def test_no_reference_flag_by_default(self, tmp: Path) -> None:
        from worker.runners.dorado_runner import build_dorado_command

        cmd = build_dorado_command(tmp / "reads.pod5", tmp / "out")
        assert "--reference" not in cmd

    def test_no_modified_bases_when_disabled(self, tmp: Path) -> None:
        from worker.runners.dorado_runner import build_dorado_command

        cmd = build_dorado_command(tmp / "reads.pod5", tmp / "out", modified_bases=False)
        assert "--modified-bases" not in cmd


# ---------------------------------------------------------------------------
# run_dorado_basecall
# ---------------------------------------------------------------------------

class TestRunDoradoBasecall:
    def test_returns_bam_on_success(self, tmp: Path) -> None:
        from worker.runners.dorado_runner import run_dorado_basecall

        pod5 = tmp / "reads.pod5"
        pod5.write_bytes(b"fake-pod5")

        def _fake_run(cmd: Any, **kwargs: Any) -> MagicMock:
            # Simulate Dorado writing a BAM
            bam_dir = Path(cmd[cmd.index("--output-dir") + 1])
            bam_dir.mkdir(parents=True, exist_ok=True)
            (bam_dir / "output.bam").write_bytes(b"BAM\x01" + b"\x00" * 30)
            result = MagicMock()
            result.stdout = ""
            result.stderr = ""
            return result

        with patch(f"{_RUNNER_PATH}.subprocess.run", side_effect=_fake_run):
            bam = run_dorado_basecall(pod5, tmp)

        assert bam.suffix == ".bam"
        assert bam.exists()

    def test_raises_when_no_bam_produced(self, tmp: Path) -> None:
        from worker.runners.dorado_runner import run_dorado_basecall

        pod5 = tmp / "reads.pod5"
        pod5.write_bytes(b"fake-pod5")

        def _fake_run(cmd: Any, **kwargs: Any) -> MagicMock:
            # Dorado exits 0 but produces nothing
            bam_dir = Path(cmd[cmd.index("--output-dir") + 1])
            bam_dir.mkdir(parents=True, exist_ok=True)
            result = MagicMock()
            result.stdout = ""
            result.stderr = ""
            return result

        with patch(f"{_RUNNER_PATH}.subprocess.run", side_effect=_fake_run):
            with pytest.raises(FileNotFoundError, match="no BAM file"):
                run_dorado_basecall(pod5, tmp)

    def test_propagates_subprocess_error(self, tmp: Path) -> None:
        from worker.runners.dorado_runner import run_dorado_basecall

        pod5 = tmp / "reads.pod5"
        pod5.write_bytes(b"fake-pod5")

        with patch(f"{_RUNNER_PATH}.subprocess.run",
                   side_effect=subprocess.CalledProcessError(1, "dorado")):
            with pytest.raises(subprocess.CalledProcessError):
                run_dorado_basecall(pod5, tmp)

    def test_mock_mode_returns_synthetic_bam(self, tmp: Path) -> None:
        from worker.runners import dorado_runner

        pod5 = tmp / "reads.pod5"
        pod5.write_bytes(b"fake-pod5")

        with patch.object(dorado_runner, "MOCK_MODE", True):
            bam = dorado_runner.run_dorado_basecall(pod5, tmp)

        assert bam.name == "mock_output.bam"
        assert bam.read_bytes()[:4] == b"BAM\x01"

    def test_picks_largest_bam_if_multiple(self, tmp: Path) -> None:
        """When Dorado emits multiple BAMs, the largest wins."""
        from worker.runners.dorado_runner import run_dorado_basecall

        pod5 = tmp / "reads.pod5"
        pod5.write_bytes(b"fake-pod5")

        def _fake_run(cmd: Any, **kwargs: Any) -> MagicMock:
            bam_dir = Path(cmd[cmd.index("--output-dir") + 1])
            bam_dir.mkdir(parents=True, exist_ok=True)
            (bam_dir / "small.bam").write_bytes(b"x" * 10)
            (bam_dir / "large.bam").write_bytes(b"x" * 100)
            result = MagicMock()
            result.stdout = result.stderr = ""
            return result

        with patch(f"{_RUNNER_PATH}.subprocess.run", side_effect=_fake_run):
            bam = run_dorado_basecall(pod5, tmp)

        assert bam.name == "large.bam"


# ---------------------------------------------------------------------------
# run_uploaded_pod5
# ---------------------------------------------------------------------------

class TestRunUploadedPod5:
    def _mock_s3(self) -> MagicMock:
        return MagicMock()

    def _fake_basecall(self, pod5_path: Path, workdir: Path, **kwargs: Any) -> Path:
        """Simulate Dorado: write a BAM next to the POD5."""
        bam = workdir / "bam_out" / "output.bam"
        bam.parent.mkdir(parents=True, exist_ok=True)
        bam.write_bytes(b"BAM\x01" + b"\x00" * 30)
        return bam

    def test_happy_path_returns_bam_info(self) -> None:
        from worker.runners import dorado_runner

        s3 = self._mock_s3()

        with patch.object(dorado_runner, "_s3", s3), \
             patch.object(dorado_runner, "run_dorado_basecall", self._fake_basecall):
            result = dorado_runner.run_uploaded_pod5(
                job_id="job-123",
                pod5_object_name="uploads/anonymous/sess/reads.pod5",
                user_id="anonymous",
            )

        assert result["bam_object_name"] == "results/anonymous/job-123/output.bam"
        assert result["pod5_deleted"] is True
        assert result["bam_size_bytes"] > 0

        # Download POD5 called, upload BAM called, delete POD5 called
        s3.download_file.assert_called_once()
        s3.upload_file.assert_called_once()
        s3.delete_object.assert_called_once_with(
            Bucket=dorado_runner.MINIO_BUCKET,
            Key="uploads/anonymous/sess/reads.pod5",
        )

    def test_pod5_deleted_even_on_basecall_failure(self) -> None:
        from worker.runners import dorado_runner

        s3 = self._mock_s3()

        with patch.object(dorado_runner, "_s3", s3), \
             patch.object(dorado_runner, "run_dorado_basecall",
                          side_effect=RuntimeError("GPU OOM")):
            with pytest.raises(RuntimeError, match="GPU OOM"):
                dorado_runner.run_uploaded_pod5(
                    job_id="job-fail",
                    pod5_object_name="uploads/anonymous/sess/reads.pod5",
                    user_id="anonymous",
                )

        # POD5 must still be deleted on failure
        s3.delete_object.assert_called_once_with(
            Bucket=dorado_runner.MINIO_BUCKET,
            Key="uploads/anonymous/sess/reads.pod5",
        )

    def test_tempdir_cleaned_up_on_success(self) -> None:
        from worker.runners import dorado_runner

        created_dirs: list[str] = []
        real_mkdtemp = tempfile.mkdtemp

        def _track_mkdtemp(**kwargs: Any) -> str:
            d = real_mkdtemp(**kwargs)
            created_dirs.append(d)
            return d

        s3 = self._mock_s3()

        with patch.object(dorado_runner, "_s3", s3), \
             patch.object(dorado_runner, "run_dorado_basecall", self._fake_basecall), \
             patch(f"{_RUNNER_PATH}.tempfile.mkdtemp", side_effect=_track_mkdtemp):
            dorado_runner.run_uploaded_pod5(
                job_id="job-cleanup",
                pod5_object_name="uploads/anonymous/sess/reads.pod5",
                user_id="anonymous",
            )

        # All temp dirs must be gone
        for d in created_dirs:
            assert not Path(d).exists(), f"Temp dir not cleaned up: {d}"

    def test_reference_download_triggered_when_provided(self) -> None:
        from worker.runners import dorado_runner

        s3 = self._mock_s3()

        with patch.object(dorado_runner, "_s3", s3), \
             patch.object(dorado_runner, "run_dorado_basecall", self._fake_basecall):
            dorado_runner.run_uploaded_pod5(
                job_id="job-ref",
                pod5_object_name="uploads/anonymous/sess/reads.pod5",
                user_id="anonymous",
                reference_object_name="refs/hg38.fa",
            )

        # Two downloads: POD5 + reference
        assert s3.download_file.call_count == 2
