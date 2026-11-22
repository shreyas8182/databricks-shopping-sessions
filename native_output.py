"""Publish only completed Parquet files, including typed empty outputs."""
import json
import os
from pathlib import Path
import subprocess


def run_native(executable, data, output, scratch):
    vectorized = executable.with_name('native_sweep_avx2')
    if vectorized.is_file() and Path('/proc/cpuinfo').is_file():
        flags = next((line.split(':', 1)[1].split() for line in Path('/proc/cpuinfo').read_text().splitlines()
                      if line.startswith('flags') and ':' in line), ())
        if {'avx2', 'bmi1', 'bmi2', 'popcnt'}.issubset(flags):
            executable = vectorized
    manifest = json.loads((data / 'manifest.json').read_text())
    subprocess.run([str(executable), str(data), str(output), manifest['window_start'],
                    manifest['window_end'], os.environ.get('NCPU', '8')], check=True)
