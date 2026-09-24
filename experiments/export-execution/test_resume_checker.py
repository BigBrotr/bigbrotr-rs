"""Trace-oracle negative controls; these do not qualify native product I/O."""
from pathlib import Path
import sys
import unittest
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
from export_destination_blocked import pending_checker


class PendingCheckerTests(unittest.TestCase):
    destination = '/out/signed_normal/export-destination'
    line = '102 1790000000.125 openat(9</out/signed_normal/export-destination>, "destination.json", O_RDONLY|O_NOFOLLOW|O_NONBLOCK|O_CLOEXEC <unfinished ...>'

    def test_actual_destination_and_worker(self):
        self.assertEqual(pending_checker(self.line, 101, self.destination), 102)

    def test_other_destination_and_default_are_excluded(self):
        self.assertIsNone(pending_checker(self.line, 101))
        self.assertIsNone(pending_checker(self.line, 101, self.destination + '-other'))

    def test_main_thread_is_excluded(self):
        self.assertIsNone(pending_checker(self.line, 102, self.destination))

    def test_returned_and_resumed_are_excluded(self):
        for trace in (self.line.replace(' <unfinished ...>', ') = 10'),
                      self.line + '\n102 1790000000.126 <... openat resumed>) = 10'):
            self.assertIsNone(pending_checker(trace, 101, self.destination))

    def test_wrong_marker_and_flags_are_excluded(self):
        for trace in (self.line.replace('destination.json', 'other.json'),
                      self.line.replace('O_NOFOLLOW|', ''), self.line.replace('O_NONBLOCK|', '')):
            self.assertIsNone(pending_checker(trace, 101, self.destination))

    def test_destination_is_literal_not_regex(self):
        destination = '/out/a[1].x'
        self.assertEqual(pending_checker(self.line.replace(self.destination, destination), 101, destination), 102)
        self.assertIsNone(pending_checker(self.line.replace(self.destination, '/out/a1-x'), 101, destination))


if __name__ == '__main__': unittest.main()
