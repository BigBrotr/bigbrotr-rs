"""Failure after real child creation must still terminate and reap that child."""
import importlib.util
import os
from pathlib import Path
import signal
import subprocess
import sys
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('held_owner',Path(__file__).with_name('held_old_owner.py'))
owner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(owner)


class SupervisionTest(unittest.TestCase):
    def test_selector_failure_after_spawn_reaps_real_child(self):
        real_popen = subprocess.Popen
        children = []
        def spawn(*args, **kwargs):
            child = real_popen([sys.executable,'-c','import time; time.sleep(30)'],**kwargs)
            children.append(child)
            return child
        try:
            with patch.object(owner.subprocess,'Popen',side_effect=spawn), patch.object(
                    owner.selectors,'DefaultSelector') as selector:
                selector.return_value.register.side_effect = OSError('injected descriptor exhaustion')
                with self.assertRaisesRegex(OSError,'injected descriptor exhaustion'):
                    owner.main(['unused','database','secret','home'])
            self.assertEqual(len(children),1)
            self.assertEqual(children[0].poll(),-signal.SIGKILL)
            with self.assertRaises(ChildProcessError): os.waitpid(children[0].pid,os.WNOHANG)
        finally:
            for child in children:
                if child.poll() is None:
                    os.killpg(child.pid,signal.SIGKILL)
                    child.wait(timeout=5)
                child.stdout.close(); child.stderr.close()


if __name__ == '__main__':
    unittest.main()
