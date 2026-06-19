import numpy as np
import h5py
from datetime import datetime
import uuid

# ----------------------- Config -----------------------
read_id = str(uuid.uuid4())
num_samples = 5000  # short read for testing
sampling_rate = 4000  # Hz, typical for MinION
digitisation = 8192.0
range_pA = 1000.0
offset = 0.0
channel_number = "1"

# Fake raw signal (realistic squiggle)
np.random.seed(42)
signal = np.random.normal(80, 12, num_samples).astype(np.float32)

# Digitise exactly like ONT does
raw_int16 = np.digitize(
    signal,
    np.linspace(signal.min() - 1, signal.max() + 1, int(digitisation))
).astype(np.int16)

# ----------------------- Write FAST5 -----------------------
with h5py.File("test_fast5_realistic.fast5", "w") as f:
    # Root groups
    raw = f.create_group(f"Raw/Reads/Read_{read_id}")
    unique = f.create_group("UniqueGlobalKey")
    
    # Raw signal
    raw.create_dataset("Signal", data=raw_int16, compression="gzip")
    
    # channel_id
    chan = unique.create_group("channel_id")
    chan.attrs["channel_number"] = channel_number
    chan.attrs["digitisation"] = digitisation
    chan.attrs["offset"] = offset
    chan.attrs["range"] = range_pA
    chan.attrs["sampling_rate"] = sampling_rate
    chan.attrs["start_time"] = 0
    
    # tracking_id (required)
    track = unique.create_group("tracking_id")
    track.attrs["exp_start_time"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    track.attrs["run_id"] = str(uuid.uuid4())
    track.attrs["flow_cell_id"] = "TEST123456"
    track.attrs["device_id"] = "MN12345"
    
    # context_tags (optional but nice)
    ctx = unique.create_group("context_tags")
    ctx.attrs["sequencing_kit"] = "sqk-lsk109"
    ctx.attrs["flowcell"] = "flo-min106"

print("✅ test_fast5_realistic.fast5 created (no extra ONT libs needed)")