"""Compare every Spark output user against the independent set/Counter reference."""
import json
import sys
from pathlib import Path
from audit_dataset import audit

report, expected = audit(sys.argv[1])
output = Path(sys.argv[2])
assert (output / 'users/_SUCCESS').exists()
assert (output / 'metadata/_SUCCESS').exists()
actual = {}
for part in (output / 'users').glob('part-*'):
    for line in part.read_text().splitlines():
        row = json.loads(line)
        assert row['userId'] not in actual, 'Duplicate output user'
        actual[row['userId']] = row
assert actual == expected, 'Spark and independent reference differ'
metadata = json.loads(next((output / 'metadata').glob('part-*')).read_text())
for key in ['totalLines', 'validRows', 'blankRows', 'invalidRows', 'uniqueEdges', 'duplicateRows', 'selfEdges', 'users', 'totalFollowers', 'totalFollowees', 'requiredUsers']:
    assert metadata[key] == report[key], key
print(f"PASS: all {len(actual):,} users match the independent reference; degree sums = {report['uniqueEdges']:,}.")
print(json.dumps(metadata['requiredUsers'], indent=2))
