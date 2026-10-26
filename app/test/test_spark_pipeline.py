"""Exercise the real distributed pipeline, including duplicates across splits."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))
from pyspark.sql import SparkSession
from twitter_counts import analyze


class SparkPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("Lab2-pipeline-regression")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sparkContext.addPyFile(str(
            Path(__file__).resolve().parents[1] / "analysis/twitter_counts.py"))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_cross_partition_duplicates_reverse_edges_and_zero_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "input"
            source.mkdir()
            (source / "a.txt").write_text("1 2\n3 2\n4 4\n\n")
            (source / "b.txt").write_text("1 2\n2 1\n4 4\n5 6\n")
            output = root / "output"
            metadata = analyze(self.spark, str(source), str(output), 4)
            rows = {}
            for part in (output / "users").glob("part-*"):
                for line in part.read_text().splitlines():
                    row = json.loads(line)
                    self.assertNotIn(row["userId"], rows)
                    rows[row["userId"]] = (row["followers"], row["followees"])
            self.assertEqual(rows, {
                "1": (1, 1), "2": (2, 1), "3": (0, 1),
                "4": (1, 1), "5": (0, 1), "6": (1, 0),
            })
            self.assertEqual(metadata["duplicateRows"], 2)
            self.assertEqual(metadata["blankRows"], 1)
            self.assertEqual(metadata["uniqueEdges"], 5)
            self.assertEqual(metadata["selfEdges"], 1)
            self.assertEqual(metadata["totalFollowers"], 5)
            self.assertEqual(metadata["totalFollowees"], 5)
            self.assertEqual(len(list((output / "users").glob("part-*"))), 4)
            for folder in ["users", "metadata"]:
                self.assertTrue((output / folder / "_SUCCESS").exists())

    def test_malformed_input_fails_without_publishing_results(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "input.txt"
            source.write_text("1 2\n1 invalid\n")
            output = root / "output"
            with self.assertRaisesRegex(ValueError, "1 malformed rows"):
                analyze(self.spark, str(source), str(output), 2)
            self.assertFalse((output / "metadata" / "_SUCCESS").exists())

    def test_blank_only_input_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "input.txt"
            source.write_text("\n \t\n")
            with self.assertRaisesRegex(ValueError, "no valid directed edges"):
                analyze(self.spark, str(source), str(root / "output"), 2)
