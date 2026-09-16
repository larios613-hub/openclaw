#!/usr/bin/env python3
"""Throwaway storage probes; each destination uses a private temporary directory."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


def timed(*command):
    begin = time.monotonic_ns()
    subprocess.run(command, check=True, timeout=120)
    return (time.monotonic_ns() - begin) / 1e9


package = Path('node_modules/.pnpm/zod@4.5.4')
if not package.is_dir():
    raise SystemExit(f'Fixed small-copy source missing: {package}')
files = [p for p in package.rglob('*') if p.is_file() and not p.is_symlink()]
print('BENCH_IO_SOURCE ' + json.dumps({'path': str(package), 'files': len(files),
                                      'bytes': sum(p.stat().st_size for p in files)}), flush=True)
for name, parent in [('tmp', '/tmp'), ('workspace', os.environ['GITHUB_WORKSPACE'])]:
    with tempfile.TemporaryDirectory(prefix='provider-bench-io-', dir=parent) as directory:
        target = Path(directory)
        dsync = timed('dd', 'if=/dev/zero', f'of={target / "dsync.bin"}',
                      'bs=4k', 'count=2000', 'oflag=dsync', 'status=none')
        seq = timed('dd', 'if=/dev/zero', f'of={target / "fsync.bin"}',
                    'bs=1M', 'count=512', 'conv=fsync', 'status=none')
        copy = timed('cp', '-r', str(package), str(target / 'package'))
        # dd uses MiB blocks; expose decimal MB/s and binary MiB/s explicitly.
        print(f'BENCH io={name} dsync_4k_ms={dsync * 1000 / 2000:.6f} '
              f'dsync_batch_s={dsync:.6f} fsync_1m_mbps={512 * 1024**2 / seq / 1e6:.3f} '
              f'fsync_1m_mibps={512 / seq:.3f} smallcopy_ms={copy * 1000:.3f}', flush=True)
