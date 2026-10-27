#!/usr/bin/env python3
"""Small infrastructure checks: timeout descendants, failed comparisons, and exit status."""
import json
from pathlib import Path
import sys
import subprocess
import uuid
import tempfile
import unittest
import DatabaseDesign.leveldb_project1.benchmark.runner as runner
from DatabaseDesign.leveldb_project1.benchmark.runner import run
from DatabaseDesign.leveldb_project1.benchmark.compare import compare

class RunnerTests(unittest.TestCase):
    def test_timeout_terminates_descendant(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);pidfile=root/'child.pid'
            code=('import subprocess,time,pathlib,sys; '
                  'p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(60)"]); '
                  f'pathlib.Path({str(pidfile)!r}).write_text(str(p.pid)); time.sleep(60)')
            result=run([sys.executable,'-c',code],root/'log',1)
            self.assertEqual(result['status'],'timeout')
            pid=int(pidfile.read_text());stat=Path(f'/proc/{pid}/stat')
            self.assertTrue(not stat.exists() or stat.read_text().split()[2]=='Z')
    def test_rejects_recursive_source_copy(self):
        root=Path(__file__).resolve().parents[1]
        source=root
        output=source/('forbidden-output-'+uuid.uuid4().hex)
        result=subprocess.run([sys.executable,str(root/'benchmark/runner.py'),'--kind','baseline',
                               '--source',str(source),'--output',str(output)],capture_output=True,text=True)
        self.assertEqual(result.returncode,2)
        self.assertIn('outside the submitted source tree',result.stderr)
        self.assertFalse(output.exists())
    def test_nonzero_is_not_pass(self):
        with tempfile.TemporaryDirectory() as td:
            result=run([sys.executable,'-c','raise SystemExit(7)'],Path(td)/'log',5)
            self.assertEqual(result['status'],'fail');self.assertEqual(result['returncode'],7)
    def test_comparison_rejects_failed_candidate(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);a=root/'a';b=root/'b';a.mkdir();b.mkdir()
            (a/'summary.json').write_text(json.dumps({'status':'pass'}))
            (b/'summary.json').write_text(json.dumps({'status':'fail'}))
            with self.assertRaises(ValueError):compare(a,b)
    def test_comparison_rejects_mismatched_inputs(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);a=root/'a';b=root/'b';a.mkdir();b.mkdir()
            for p,size in [(a,256),(b,128)]:
                (p/'summary.json').write_text(json.dumps({'status':'pass','kind':'baseline','value_bytes':size,'seed':1,'sanitize':False}))
            with self.assertRaises(ValueError):compare(a,b)

class CaseSelectionTests(unittest.TestCase):
    def setUp(self):
        self.available = [
            'ttl_boundary', 'ttl_crash_writer', 'metadata_basic',
            'combined_lifecycle', 'combined_crash_writer', 'perf_write',
        ]

    def test_default_excludes_child_process_helpers(self):
        self.assertEqual(
            runner.select_cases(self.available, None),
            ['ttl_boundary', 'metadata_basic', 'combined_lifecycle', 'perf_write'])

    def test_explicit_selection_preserves_requested_order(self):
        self.assertEqual(
            runner.select_cases(self.available, 'combined_lifecycle,ttl_boundary'),
            ['combined_lifecycle', 'ttl_boundary'])

    def test_rejects_unknown_helpers_duplicates_and_empty_selections(self):
        invalid = [
            'unknown_case', 'ttl_crash_writer', 'combined_crash_writer',
            'ttl_boundary,ttl_boundary', 'ttl_boundary,unknown_case',
            'ttl_boundary,combined_crash_writer', '', ',ttl_boundary',
            'ttl_boundary,', 'ttl_boundary,,metadata_basic',
        ]
        for requested in invalid:
            with self.subTest(requested=requested):
                with self.assertRaises(ValueError):
                    runner.select_cases(self.available, requested)

    def test_default_cannot_succeed_with_no_executable_cases(self):
        for available in ([], ['combined_crash_writer']):
            with self.subTest(available=available):
                with self.assertRaises(ValueError):
                    runner.select_cases(available, None)


class MatrixTests(unittest.TestCase):
    def setUp(self):
        self.config = dict(keys=10000, threads=4, rounds=4, repeat=1)

    def test_without_matrix_preserves_explicit_parameters(self):
        for name in ('combined_lifecycle', 'perf_write',
                     'perf_combined_stats_under_write', 'perf_combined_gc'):
            with self.subTest(case=name):
                self.assertEqual(
                    runner.case_variants(name, self.config, False, 25),
                    [(10000, 4, 25)])

    def test_matrix_does_not_expand_correctness_cases(self):
        self.assertEqual(
            runner.case_variants('combined_lifecycle', self.config, True, 25),
            [(10000, 4, 25)])

    def test_ordinary_performance_varies_size_only(self):
        self.assertEqual(
            runner.case_variants('perf_write', self.config, True, 25),
            [(1000, 1, 25), (10000, 1, 25)])

    def test_concurrent_workloads_vary_size_and_threads(self):
        expected = [(1000, 1, 25), (1000, 4, 25),
                    (10000, 1, 25), (10000, 4, 25)]
        for name in ('perf_concurrent', 'perf_stats_under_write'):
            with self.subTest(case=name):
                self.assertEqual(
                    runner.case_variants(name, self.config, True, 25), expected)

    def test_expiration_matrix_includes_none_half_and_all(self):
        expected = [(1000, 1, 0), (1000, 1, 50), (1000, 1, 100),
                    (10000, 1, 0), (10000, 1, 50), (10000, 1, 100)]
        for name in ('perf_ttl_mixed', 'perf_combined_gc'):
            with self.subTest(case=name):
                self.assertEqual(
                    runner.case_variants(name, self.config, True, 25), expected)

    def test_combined_writes_cover_all_independent_dimensions(self):
        variants = runner.case_variants(
            'perf_combined_stats_under_write', self.config, True, 25)
        self.assertEqual(len(variants), 12)
        self.assertEqual(len(set(variants)), 12)
        self.assertEqual({item[0] for item in variants}, {1000, 10000})
        self.assertEqual({item[1] for item in variants}, {1, 4})
        self.assertEqual({item[2] for item in variants}, {0, 50, 100})
        self.assertIn((1000, 4, 100), variants)
        self.assertIn((10000, 1, 0), variants)

    def test_small_inputs_never_create_zero_or_oversized_variants(self):
        config = dict(self.config, keys=9, threads=1)
        self.assertEqual(
            runner.case_variants('perf_concurrent', config, True, 50),
            [(1, 1, 50), (9, 1, 50)])
        config['keys'] = 1
        self.assertEqual(
            runner.case_variants('perf_combined_stats_under_write', config, True, 50),
            [(1, 1, 0), (1, 1, 50), (1, 1, 100)])


class CombinedCLITests(unittest.TestCase):
    def test_combined_and_existing_kinds_reach_source_validation(self):
        script = Path(__file__).resolve().parent / 'runner.py'
        with tempfile.TemporaryDirectory() as td:
            source = Path(td) / 'empty-source'
            source.mkdir()
            for kind in ('combined', 'ttl', 'metadata', 'baseline'):
                with self.subTest(kind=kind):
                    result = subprocess.run(
                        [sys.executable, str(script), '--kind', kind,
                         '--source', str(source), '--output', str(Path(td) / kind)],
                        capture_output=True, text=True)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn('source must be a LevelDB engine directory', result.stderr)
                    self.assertFalse((Path(td) / kind).exists())


class CombinedComparisonTests(unittest.TestCase):
    def write_run(self, path, kind, metrics):
        path.mkdir()
        (path / 'summary.json').write_text(json.dumps(dict(
            status='pass', kind=kind, value_bytes=256, seed=7, sanitize=False)))
        (path / 'provenance.json').write_text(json.dumps(dict(
            platform='test-machine', compiler='test-compiler', submodules='bundled')))
        (path / 'aggregate.json').write_text(json.dumps(metrics))

    def metric(self, case, throughput=100, **overrides):
        result = dict(case=case, phase='measure', keys=1000, threads=1,
                      expired_percent=50, repeats=3,
                      median_ops_per_second=throughput, median_p99_us=10)
        result.update(overrides)
        return result

    def test_combined_compares_only_shared_performance_workloads(self):
        with tempfile.TemporaryDirectory() as td:
            base, candidate = Path(td) / 'baseline', Path(td) / 'combined'
            self.write_run(base, 'baseline', [
                self.metric('perf_write', 200), self.metric('perf_read'),
                self.metric('correctness_with_metrics'),
            ])
            self.write_run(candidate, 'combined', [
                self.metric('perf_write', 100), self.metric('perf_combined_gc'),
                self.metric('perf_combined_stats_under_write'),
                self.metric('correctness_with_metrics'),
            ])
            result = compare(base, candidate)
            self.assertEqual(len(result['comparisons']), 1)
            row = result['comparisons'][0]
            self.assertEqual(row['case'], 'perf_write')
            self.assertEqual(row['candidate_over_baseline_throughput'], 0.5)
            self.assertIsNone(result['grade'])

    def test_combined_feature_metrics_alone_are_not_a_baseline_comparison(self):
        with tempfile.TemporaryDirectory() as td:
            base, candidate = Path(td) / 'baseline', Path(td) / 'combined'
            self.write_run(base, 'baseline', [self.metric('perf_write')])
            self.write_run(candidate, 'combined', [self.metric('perf_combined_gc')])
            with self.assertRaises(ValueError):
                compare(base, candidate)

    def test_shared_correctness_metrics_are_not_performance_matches(self):
        with tempfile.TemporaryDirectory() as td:
            base, candidate = Path(td) / 'baseline', Path(td) / 'combined'
            self.write_run(base, 'baseline', [self.metric('correctness_with_metrics')])
            self.write_run(candidate, 'combined', [self.metric('correctness_with_metrics')])
            with self.assertRaises(ValueError):
                compare(base, candidate)

    def test_combined_comparison_requires_matching_workload_parameters(self):
        for changed in ({'keys': 2000}, {'threads': 4},
                        {'expired_percent': 100}, {'phase': 'other'}):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as td:
                base, candidate = Path(td) / 'baseline', Path(td) / 'combined'
                self.write_run(base, 'baseline', [self.metric('perf_write')])
                self.write_run(candidate, 'combined', [self.metric('perf_write', **changed)])
                with self.assertRaises(ValueError):
                    compare(base, candidate)

    def test_combined_comparison_requires_equal_repeat_counts(self):
        with tempfile.TemporaryDirectory() as td:
            base, candidate = Path(td) / 'baseline', Path(td) / 'combined'
            self.write_run(base, 'baseline', [self.metric('perf_write')])
            self.write_run(candidate, 'combined', [self.metric('perf_write', repeats=1)])
            with self.assertRaises(ValueError):
                compare(base, candidate)

if __name__=='__main__':unittest.main()
