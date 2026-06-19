"""
Preprocessing Pipeline for Nanopore RNA Modification Detection

This package contains the three mandatory preprocessing steps required
before modification calling with CHEUI, m6Anet, or similar tools:

1. basecall.py    - Dorado/Guppy basecalling (FAST5 → FASTQ/BAM)
2. align.py       - minimap2 alignment (FASTQ → aligned BAM)
3. eventalign.py  - f5c/nanopolish eventalign (re-attach signal to positions)

These steps are REQUIRED because modification callers need:
- Basecalled sequences to know which read is which
- Aligned coordinates to know which genomic position each signal belongs to
- Eventaligned signal to compare raw currents against expected values
"""
