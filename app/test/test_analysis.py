import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'analysis'))
from twitter_counts import parse_line, contributions, add_counts


class DirectedEdgeTests(unittest.TestCase):
    def test_direction_and_zero_degree(self):
        self.assertEqual(contributions(('A', 'B')), [('A', (0, 1)), ('B', (1, 0))])

    def test_self_edge_counts_both_directions(self):
        values = contributions(('A', 'A'))
        self.assertEqual(add_counts(values[0][1], values[1][1]), (1, 1))

    def test_identifiers_preserve_precision(self):
        self.assertEqual(parse_line('99999999999999999999 1'), ('valid', ('99999999999999999999', '1')))

    def test_malformed_rows_are_not_silently_accepted(self):
        for line in ['1 2 3', '1 -2', '1 ２', 'x y']:
            self.assertEqual(parse_line(line)[0], 'invalid')
        self.assertEqual(parse_line(' \t ')[0], 'blank')
