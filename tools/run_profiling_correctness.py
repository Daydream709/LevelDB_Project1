#!/usr/bin/env python3
"""Build and run correctness/regression/sanitizer checks; never select perf_* cases."""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

from validate_profiling import validate_case

ROOT = Path(__file__).resolve().parents[1]
CASES = (
    'ttl_contract', 'ttl_views', 'metadata_contract', 'metadata_reopen',
    'combined_contract', 'combined_overwrites', 'combined_views',
    'combined_reopen', 'combined_gc', 'combined_concurrent',
)


def run(command, log, env):
    with log.open('w') as stream:
        stream.write('COMMAND ' + json.dumps([str(x) for x in command]) + '\n')
        stream.flush()
        subprocess.run([str(x) for x in command], stdout=stream,
                       stderr=subprocess.STDOUT, env=env, check=True, timeout=1200)


def suite(output, sanitizer):
    output.mkdir()
    env = os.environ.copy()
    env['TEST_TMPDIR'] = str(output / 'tmp')
    Path(env['TEST_TMPDIR']).mkdir()
    if sanitizer:
        env['ASAN_OPTIONS'] = ('detect_leaks=1' if platform.system() == 'Linux' else 'detect_leaks=0') + ':halt_on_error=1'
        env['UBSAN_OPTIONS'] = 'halt_on_error=1:print_stacktrace=1'
    flags = ['-DCMAKE_BUILD_TYPE=Debug', '-DLEVELDB_BUILD_BENCHMARKS=OFF',
             '-DLEVELDB_INSTALL=OFF', f'-DHW_SOURCE={ROOT}', '-DHW_KIND=combined',
             f'-DHW_REGRESSION_SOURCE={ROOT / "benchmark/regression-source"}']
    if sanitizer:
        flags += ['-DCMAKE_C_FLAGS=-fsanitize=address,undefined -fno-omit-frame-pointer',
                  '-DCMAKE_CXX_FLAGS=-fsanitize=address,undefined -fno-omit-frame-pointer',
                  '-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=address,undefined']
    build = output / 'build'
    run(['cmake', '-S', ROOT / 'benchmark', '-B', build, *flags], output / 'configure.log', env)
    targets = ('leveldb_tests', 'homework_bench', 'homework_regression',
               'homework_c_regression', 'homework_env_regression')
    run(['cmake', '--build', build, '--target', *targets, '-j', args.jobs], output / 'build.log', env)
    run([build / 'student/leveldb_tests'], output / 'unit.log', env)
    for target in targets[2:]:
        run([build / target], output / f'{target}.log', env)
    available = subprocess.check_output([build / 'homework_bench', '--list'], text=True, env=env).splitlines()
    assert all(name in available and not name.startswith('perf_') for name in CASES)
    for enabled in (False, True):
        for case in CASES:
            directory = output / (case + ('-profiled' if enabled else '-disabled'))
            directory.mkdir()
            command = [build / 'homework_bench', '--case', case, '--db', directory / 'db',
                       '--keys', '128', '--threads', '4', '--profiling', str(int(enabled)),
                       '--profiling-output', directory / 'profiling']
            run(command, directory / 'run.log', env)
            validate_case(directory / 'profiling', case, 128, enabled)
    return dict(status='pass', sanitizer=sanitizer, cases=len(CASES) * 2)


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True, help='new output directory')
parser.add_argument('--jobs', type=int, default=4)
parser.add_argument('--skip-sanitizers', action='store_true')
parser.add_argument('--allow-non-linux', action='store_true', help='local checks only; never labels results Linux')
args = parser.parse_args()
if platform.system() != 'Linux' and not args.allow_non_linux:
    parser.error('Linux required; --allow-non-linux is only for explicitly labeled local validation')
if args.output.exists():
    parser.error('output directory must not already exist')
args.output = args.output.resolve()
args.output.mkdir(parents=True)
results = dict(platform=platform.platform(), machine=platform.machine(), linux_verified=platform.system() == 'Linux',
               benchmarks_run=False, compiler=subprocess.check_output([os.environ.get('CXX', 'c++'), '--version'], text=True),
               suites=[], status='running')
try:
    results['suites'].append(suite(args.output / 'debug', False))
    if not args.skip_sanitizers:
        results['suites'].append(suite(args.output / 'sanitizers', True))
    results['status'] = 'pass'
except Exception as exc:
    results.update(status='fail', error=str(exc))
    raise
finally:
    (args.output / 'validation.json').write_text(json.dumps(results, indent=2, ensure_ascii=False) + '\n')
