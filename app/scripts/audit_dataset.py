"""Independent single-machine reference, NOT the submitted cloud pipeline."""
import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path


def audit(path):
    path = Path(path)
    edges = set()
    valid = blank = invalid = lines = 0
    examples = []
    with path.open() as f:
        for lines, line in enumerate(f, 1):
            parts = line.split()
            if not parts:
                blank += 1
            elif len(parts) != 2 or not all(p.isascii() and p.isdigit() for p in parts):
                invalid += 1
                if len(examples) < 5:
                    examples.append({"line": lines, "value": line.strip()[:100]})
            else:
                valid += 1
                edges.add(tuple(parts))
    followers, followees = Counter(), Counter()
    for source, target in edges:
        followers[target] += 1
        followees[source] += 1
    users = set(followers) | set(followees)
    with path.open('rb') as f:
        digest = hashlib.file_digest(f, 'sha256').hexdigest()
    report = {
        "file": path.name, "bytes": path.stat().st_size, "sha256": digest,
        "totalLines": lines, "validRows": valid, "blankRows": blank,
        "invalidRows": invalid, "invalidExamples": examples,
        "uniqueEdges": len(edges), "duplicateRows": valid - len(edges),
        "selfEdges": sum(a == b for a, b in edges), "selfEdgePolicy": "retain",
        "users": len(users), "totalFollowers": sum(followers.values()),
        "totalFollowees": sum(followees.values()),
        "requiredUsers": {u: {"followers": followers[u], "followees": followees[u]} for u in ["214328887", "107830991"] if u in users},
    }
    counts = {u: {"userId": u, "followers": followers[u], "followees": followees[u]} for u in users}
    return report, counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('--report', required=True)
    args = parser.parse_args()
    report, _ = audit(args.input)
    dest = Path(args.report); dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
