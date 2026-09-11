"""Mechanical checks for planning artifacts; does not certify architecture."""
import ast
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
files = [ROOT/'README.md', ROOT/'AGENTS.md', ROOT/'PLAN.md']
files += sorted((ROOT/'docs').rglob('*.md'))
files += sorted((ROOT/'experiments').rglob('*.md'))
errors, json_examples = [], 0
for p in files:
    text = p.read_text()
    if not text.endswith('\n'): errors.append(f'{p}: missing final newline')
    if any(line.rstrip()!=line for line in text.splitlines()): errors.append(f'{p}: trailing whitespace')
    if sum(line.startswith('```') for line in text.splitlines()) % 2: errors.append(f'{p}: unbalanced fence')
    for target in re.findall(r'\[[^\]\n]+\]\(([^)\s]+)\)',text):
        if target.startswith(('http:','https:','#','mailto:')): continue
        path = target.split('#')[0]
        if path and not (p.parent/path).exists(): errors.append(f'{p}: missing local link {target}')
    for sample in re.findall(r'```json\s*\n(.*?)\n```',text,re.S):
        try: json.loads(sample); json_examples+=1
        except ValueError as exc: errors.append(f'{p}: invalid JSON example: {exc}')
json_files = sorted((ROOT/'docs').rglob('*.json'))
for p in json_files:
    try: json.loads(p.read_text())
    except ValueError as exc: errors.append(f'{p}: invalid JSON: {exc}')
for p in (ROOT/'experiments').rglob('*.py'):
    ast.parse(p.read_text(),filename=str(p))
manifest_path=ROOT/'docs/research/evidence/prior-components/manifest.json'
manifest=json.loads(manifest_path.read_text())
for name,digest in manifest['files'].items():
    if hashlib.sha256((manifest_path.parent/name).read_bytes()).hexdigest()!=digest:
        errors.append(f'prior evidence digest differs: {name}')
report={'scope':'local links, fences, JSON examples/evidence, Python syntax and selected evidence hashes; no semantic or product verification',
        'markdown_files':len(files),'json_files':len(json_files),'json_examples':json_examples,'errors':errors}
print(json.dumps(report,indent=2))
raise SystemExit(bool(errors))
