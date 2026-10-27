#!/usr/bin/env python3
"""Validate exported profiling JSON/JSONL without running a performance workload."""
import argparse
import json
from pathlib import Path


def validate(directory):
    directory = Path(directory)
    summary = json.loads((directory / 'summary.json').read_text())
    assert summary['schema_version'] == 1
    assert summary['time_unit'] == 'ns' and summary['timings'] == 'inclusive'
    assert isinstance(summary['enabled'], bool)
    assert summary['foreground_sample_interval'] >= 1
    assert summary['dropped_task_events'] >= 0
    for data in [summary, *summary['databases'].values()]:
        for name, metric in data['metrics'].items():
            assert metric['calls'] == metric['completed'] + metric['in_flight'], name
            assert metric['completed'] == metric['ok'] + metric['not_found'] + metric['errors'], name
            assert metric['timing_samples'] <= metric['completed'], name
            assert len(metric['histogram_log2_ns']) == 65, name
            assert sum(metric['histogram_log2_ns']) == metric['timing_samples'], name
            for field, value in metric.items():
                if isinstance(value, list):
                    assert all(isinstance(n, int) and n >= 0 for n in value), field
                else:
                    assert isinstance(value, int) and value >= 0, field
        assert all(isinstance(n, int) and n >= 0 for n in data['counters'].values())
    events = [json.loads(line) for line in (directory / 'tasks.jsonl').read_text().splitlines()]
    assert len(events) == summary['retained_task_events']
    assert len(events) <= summary['max_task_events']
    assert len({e['event_id'] for e in events}) == len(events)
    for event in events:
        assert event['schema_version'] == 1
        assert event['session_id'] == summary['session_id']
        assert event['export_id'] == summary['export_id']
        assert str(event['db_instance_id']) in summary['databases']
        assert event['elapsed_ns'] >= 0
        assert isinstance(event['failure_stage'], str)
        if event['status'] not in ('ok', 'not_found'):
            assert event['failure_stage']
        assert event['status'] in ('ok', 'not_found', 'io_error', 'corruption', 'invalid_argument', 'not_supported')
        assert all(isinstance(n, int) and n >= 0 for n in event['counters'].values())
        for stage in event['stages'].values():
            assert stage['calls'] >= 1 and stage['total_ns'] >= 0
    if not summary['enabled'] and not summary['metrics']:
        assert summary['profiling_clock_reads'] == 0
    return len(summary['metrics']), len(events)


def validate_case(directory, case, keys, enabled):
    """Check exact work counters against known correctness workloads."""
    validate(directory)
    summary = json.loads((Path(directory) / 'summary.json').read_text())
    assert all(metric['in_flight'] == 0 for metric in summary['metrics'].values())
    if not enabled:
        assert not summary['metrics'] and not summary['counters']
        assert summary['profiling_clock_reads'] == 0
        return
    counters = summary['counters']
    if case == 'combined_concurrent':
        assert counters['write.committed_records'] == 3 * keys
        assert counters['gc.installed_removed_records'] == keys
    elif case == 'combined_gc':
        count = max(8, min(keys, 256))
        assert counters['write.committed_records'] == count + 1
        assert counters['gc.installed_removed_records'] == count // 2
    elif case == 'combined_overwrites':
        assert counters['write.committed_records'] == 9
        assert counters['gc.installed_removed_records'] == 8


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', nargs='+', type=Path)
    args = parser.parse_args()
    for directory in args.directories:
        metrics, events = validate(directory)
        print(json.dumps(dict(directory=str(directory), metrics=metrics, events=events, status='pass')))
