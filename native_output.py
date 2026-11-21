"""Publish only completed Parquet files, including typed empty outputs."""
import json
import os
from pathlib import Path
import subprocess
import tempfile


def run_native(executable, data, output, scratch):
    manifest = json.loads((data / 'manifest.json').read_text())
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output) as staging:
        staging = Path(staging)
        for name in ('user_daily', 'campaign_daily'):
            (staging / name).mkdir()
        subprocess.run([str(executable), str(data), str(staging), manifest['window_start'],
                        manifest['window_end'], os.environ.get('NCPU', '8')], check=True)
        for name in ('user_daily', 'campaign_daily'):
            dest = output / name
            dest.mkdir(exist_ok=True)
            files = list((staging / name).glob('*.parquet'))
            generated = {p.name for p in files}
            if any(p.name not in generated for p in dest.glob('*.parquet')):
                raise ValueError(f'use a clean output directory: {dest}')
        for name in ('user_daily', 'campaign_daily'):
            for path in (staging / name).glob('*.parquet'):
                path.replace(output / name / path.name)
