"""Own one installed activated workflow process; publish custody, never product receipts."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def main():
    assert len(sys.argv) in (3, 4)
    config, prefix = map(Path, sys.argv[1:3])
    assert config.is_absolute() and prefix.is_absolute()
    action = sys.argv[3] if len(sys.argv) == 4 else '--abandon-activated-acquisition'
    assert action in ('--abandon-activated-acquisition', '--retire-activated-initial')
    command = ['/build/bigbrotr', 'ingest', 'recover', '--file', str(config), action]

    def publish(suffix, value):
        target = Path(str(prefix) + suffix)
        partial = Path(str(target) + '.partial')
        assert not target.exists()
        with partial.open('x') as stream:
            stream.write(json.dumps(value) + '\n')
        os.replace(partial, target)

    started = time.monotonic()
    with Path(str(prefix) + '.stdout').open('xb') as out, Path(str(prefix) + '.stderr').open('xb') as err:
        child = subprocess.Popen(command, stdout=out, stderr=err, env={})
        forced = False
        identity = dict(pid=child.pid, config=str(config), prefix=str(prefix), command=command)
        try:
            identity['start_ticks'] = int(Path(f'/proc/{child.pid}/stat').read_text().split()[21])
            publish('.pid.json', identity)
            child.wait(timeout=45)
        except BaseException:
            forced = True
            child.kill()
            child.wait(timeout=5)
            raise
        finally:
            publish('.terminal.json', dict(identity, exit_code=child.returncode,
                elapsed_seconds=time.monotonic() - started, forced=forced,
                reaped=child.returncode is not None))


if __name__ == '__main__':
    main()
