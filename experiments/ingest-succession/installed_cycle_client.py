"""Finite process custody for the installed cycle; no HTTP or job submission."""
import json
from pathlib import Path
import subprocess
import sys
import time

FLAGS = {'complete-initial': '--complete-initial-preparation', 'recover': '--successor', 'source': '--close-successor-source',
         'abandon': '--abandon-successor-acquisition', 'retire': '--retire-successor',
         'abandon-initial': '--abandon-acquisition', 'retire-initial': '--retire-initial',
         'source-activated': '--close-activated-source', 'retire-activated': '--retire-activated-initial'}


def argv(operation, config):
    assert operation in (*FLAGS, 'live', 'live-initial', 'recover-initial')
    assert config.is_absolute() and config.parent == Path('/lab') and config.suffix == '.toml'
    if operation in ('live','live-initial'):
        return ['/build/bigbrotr', 'serve', '--file', str(config), '--activate-initial-successor' if operation=='live-initial' else '--activate-successor',
                '--roles', 'control,ingest']
    if operation == 'recover-initial':
        return ['/build/bigbrotr', 'ingest', 'recover', '--file', str(config)]
    return ['/build/bigbrotr', 'ingest', 'recover', '--file', str(config), FLAGS[operation]]


def publish(path, value):
    temporary = Path(str(path) + '.tmp')
    with temporary.open('x') as stream:
        json.dump(value, stream)
    temporary.rename(path)


def run(operation, config, prefix):
    command = argv(operation, config)
    assert prefix.is_absolute() and prefix.parent == Path('/lab')
    started, forced, error = time.monotonic(), False, None
    with Path(str(prefix) + '.stdout').open('xb') as out, Path(str(prefix) + '.stderr').open('xb') as err:
        child = subprocess.Popen(command, stdout=out, stderr=err, env={})
        identity = dict(pid=child.pid, config=str(config), prefix=str(prefix), command=command)
        try:
            identity['start_ticks'] = int(Path(f'/proc/{child.pid}/stat').read_text().split()[21])
            publish(Path(str(prefix) + '.pid.json'), identity)
            child.wait(timeout=55)
        except BaseException as caught:
            error, forced = repr(caught), True
            child.kill()
            child.wait(timeout=5)
            raise
        finally:
            publish(Path(str(prefix) + '.terminal.json'), dict(identity, exit_code=child.returncode,
                reaped=child.returncode is not None, forced=forced, error=error,
                elapsed_seconds=time.monotonic() - started))


if __name__ == '__main__':
    assert len(sys.argv) == 4
    run(sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3]))
