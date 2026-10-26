#!/usr/bin/env python3
"""Select only a proven documentation-only diff; unknown input runs full CI."""
import argparse
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
DOCUMENTATION_ROOTS = ('docs/planning/', 'docs/implementation/', 'docs/research/',
                       'docs/reviews/', 'docs/engineering/')
DOCUMENTATION_FILES = {'README.md', 'AGENTS.md', 'PLAN.md'}


def documentation_only(paths):
    # Deliberately narrow: generated references, shipped tutorials, contracts,
    # fixtures, workflows and unknown files retain the full native gate.
    return bool(paths) and all(
        path in DOCUMENTATION_FILES or
        (path.startswith(DOCUMENTATION_ROOTS) and path.endswith(('.md', '.json')))
        for path in paths)


def select(base, head, root=ROOT):
    result = {'format': 'bigbrotr-ci-selection/1', 'base': base, 'head': head,
              'foundation': True, 'reason': 'unknown or unavailable comparison'}
    if not all(re.fullmatch(r'[0-9a-f]{40}', value or '') and value != '0'*40 for value in (base, head)):
        return result
    try:
        paths = subprocess.check_output(
            ['git', 'diff', '--name-only', '--no-renames', '-z', base, head, '--'], cwd=root,
            stderr=subprocess.DEVNULL).decode().split('\0')
    except (OSError, subprocess.CalledProcessError, UnicodeError):
        return result
    paths = [path for path in paths if path]
    result.update(paths=paths, foundation=not documentation_only(paths),
                  reason='documentation-only allowlist' if documentation_only(paths) else 'product, tooling or unknown change')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', default='')
    parser.add_argument('--head', default='')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--github-output', type=Path)
    args = parser.parse_args()
    result = select(args.base, args.head)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    if args.github_output:
        with args.github_output.open('a') as stream:
            stream.write('foundation=' + str(result['foundation']).lower() + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
