"""Check planning links/task IDs/counts; no product or semantic qualification.

First run: python3 experiments/verify_handoff.py --prepare
Then: python3 experiments/verify_handoff.py --output PATH

--prepare downloads only checksum-pinned github-slugger 2.0.0 into ignored
.research/checkouts/handoff-validation/. Node executes that canonical slugger;
there is no product dependency or npm install/script execution. Dependency cycles
are review candidates: prose can distinguish foundation slices and optional gates.
"""
import argparse
import hashlib
import html
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile
from urllib.parse import unquote
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
SLUG_ROOT = ROOT / ".research/checkouts/handoff-validation/github-slugger"
TARBALL = "https://registry.npmjs.org/github-slugger/-/github-slugger-2.0.0.tgz"
SHA256 = "ddde66e96b7d722ff2c3d478e7595aeccae7e75dde798d405510d862f1367d0f"


def prepare():
    with urlopen(TARBALL, timeout=30) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise SystemExit("github-slugger archive checksum mismatch")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        for name in ("package/package.json", "package/index.js", "package/regex.js"):
            target = SLUG_ROOT / Path(name).name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.extractfile(name).read())


def prose(text):
    lines, fence = [], None
    for line in text.splitlines():
        marker = re.match(r"^ {0,3}(`{3,}|~{3,})", line)
        if marker:
            token = marker.group(1)
            if fence is None:
                fence = token
            elif token[0] == fence[0] and len(token) >= len(fence):
                fence = None
            lines.append("")
        else:
            lines.append("" if fence else line)
    return "\n".join(lines)


def headings(text):
    values = []
    lines = text.splitlines()
    for index, line in enumerate(lines):
        atx = re.match(r"^ {0,3}#{1,6} +(.*?)(?: +#+)?\s*$", line)
        if atx:
            value = atx.group(1)
        elif index and re.fullmatch(r" {0,3}(?:=+|-+)\s*", line) and lines[index - 1].strip():
            value = lines[index - 1].strip()
        else:
            continue
        value = re.sub(r"!?\[([^]]+)\]\([^)]*\)", r"\1", value)
        value = re.sub(r"<[^>]+>", "", value)
        values.append(html.unescape(value))
    return values


def references(text, local_phase=None):
    """Expand explicit IDs and adjacent /Tnn or –Tnn, plus local Tnn dependencies."""
    token = re.compile(r"\bP(\d{2})-T(\d{2})\b|\bT(\d{2})\b")
    phase, previous, end = local_phase, None, None
    found = []
    for match in token.finditer(text):
        if match.group(1):
            new_phase, number = match.group(1), int(match.group(2))
        elif phase and (local_phase or (end is not None and re.fullmatch(r"[/–—-]", text[end:match.start()]))):
            new_phase, number = phase, int(match.group(3))
        else:
            continue
        gap = text[end:match.start()] if end is not None else ""
        if phase == new_phase and previous is not None and re.fullmatch(r"[–—-]", gap):
            found.extend(f"P{phase}-T{n:02}" for n in range(previous + 1, number + 1))
        else:
            found.append(f"P{new_phase}-T{number:02}")
        phase, previous, end = new_phase, number, match.end()
    return found


def cycles(graph):
    """Return strongly connected groups without claiming every prose edge is hard."""
    index, stack, indices, low, active, groups = 0, [], {}, {}, set(), []

    def visit(node):
        nonlocal index
        indices[node] = low[node] = index
        index += 1
        stack.append(node)
        active.add(node)
        for child in graph.get(node, []):
            if child not in indices:
                visit(child)
                low[node] = min(low[node], low[child])
            elif child in active:
                low[node] = min(low[node], indices[child])
        if low[node] == indices[node]:
            group = []
            while True:
                child = stack.pop()
                active.remove(child)
                group.append(child)
                if child == node:
                    break
            if len(group) > 1 or node in graph.get(node, []):
                groups.append(sorted(group))

    for node in graph:
        if node not in indices:
            visit(node)
    return groups


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.prepare:
        prepare()
    slugger = SLUG_ROOT / "index.js"
    if not slugger.exists():
        raise SystemExit("Missing canonical slugger; run with --prepare (requires network and Node).")
    paths = [ROOT / name for name in ("README.md", "AGENTS.md", "PLAN.md")]
    paths += sorted((ROOT / "docs").rglob("*.md"))
    paths += sorted((ROOT / "experiments").rglob("*.md"))
    texts = {path: prose(path.read_text()) for path in paths}
    payload = {str(path): headings(text) for path, text in texts.items()}
    js = """
import {pathToFileURL} from 'node:url';
const {default: Slugger} = await import(pathToFileURL(process.argv[1]));
let input = ''; for await (const chunk of process.stdin) input += chunk;
const result = {};
for (const [file, headings] of Object.entries(JSON.parse(input))) {
  const slugger = new Slugger(); result[file] = headings.map(h => slugger.slug(h));
}
process.stdout.write(JSON.stringify(result));
"""
    result = subprocess.run(["node", "--input-type=module", "-e", js, str(slugger)],
                            input=json.dumps(payload), text=True, capture_output=True, check=True)
    anchors = {Path(path): set(values) for path, values in json.loads(result.stdout).items()}
    for path, text in texts.items():
        anchors[path].update(re.findall(r'<a\s+(?:[^>]*\s)?(?:id|name)=["\']([^"\']+)', text))
    errors, fragment_count, reference_count, tasks, phases, dependencies, dependency_text = [], 0, 0, {}, [], {}, {}
    for path in sorted((ROOT / "docs/implementation").glob("[0-9][0-9]-*.md")):
        text = texts[path]
        matches = list(re.finditer(r"^## (P\d{2}-T\d{2})\b", text, re.M))
        expected = [f"P{path.name[:2]}-T{number:02}" for number in range(1, len(matches) + 1)]
        if [match.group(1) for match in matches] != expected:
            errors.append({"kind": "phase_task_sequence", "path": str(path.relative_to(ROOT))})
        step_count = 0
        for number, match in enumerate(matches):
            task = match.group(1)
            body = text[match.end():matches[number + 1].start() if number + 1 < len(matches) else len(text)]
            steps = [int(value) for value in re.findall(r"^- \[[ xX]\] \*\*(?:Step )?(\d+)[.:]", body, re.M)]
            if task in tasks:
                errors.append({"kind": "duplicate_task", "task": task})
            if steps != list(range(1, len(steps) + 1)):
                errors.append({"kind": "step_sequence", "task": task, "steps": steps})
            tasks[task] = {"path": str(path.relative_to(ROOT)), "steps": len(steps)}
            step_count += len(steps)
            dep = re.search(r"^(?:\*\*)?(?:Dependencies|Depends on|Dependency/output):(?:\*\*)?\s*(.*?)(?=\n\n|\Z)", body, re.M | re.S)
            if dep:
                dependency_text[task] = dep.group(1)
                dependencies[task] = references(dep.group(1), task[1:3])
            else:
                dependencies[task] = []
        phases.append({"file": str(path.relative_to(ROOT)), "tasks": len(matches), "steps": step_count})
    for path, text in texts.items():
        for ref in references(text):
            reference_count += 1
            if ref not in tasks:
                errors.append({"kind": "unknown_task", "path": str(path.relative_to(ROOT)), "task": ref})
        for match in re.finditer(r"\]\(([^)\s]+)\)", text):
            target = match.group(1)
            if "#" not in target or target.startswith(("http:", "https:", "mailto:")):
                continue
            filename, fragment = target.split("#", 1)
            dest = (path.parent / unquote(filename)).resolve() if filename else path
            fragment_count += 1
            if unquote(fragment) not in anchors.get(dest, set()):
                errors.append({"kind": "missing_fragment", "path": str(path.relative_to(ROOT)),
                               "line": text[:match.start()].count("\n") + 1, "target": target})
    for task, refs in dependencies.items():
        for ref in refs:
            if ref not in tasks:
                errors.append({"kind": "unknown_dependency", "task": task, "dependency": ref})
    report = {
        "scope": "Canonical GitHub fragments, literal/range task references, declared dependency IDs and task/step counts; prose dependency cycles need human/agent review, not automatic DAG claims",
        "slugger": {"name": "github-slugger", "version": "2.0.0", "tarball": TARBALL, "sha256": SHA256},
        "markdown_files": len(paths), "checked_local_fragments": fragment_count,
        "checked_task_references": reference_count, "declared_dependency_sections": len(dependency_text),
        "tasks": len(tasks), "steps": sum(row["steps"] for row in phases), "phases": phases,
        "errors": errors, "dependency_cycle_candidates": cycles(dependencies),
        "dependencies": dependencies, "dependency_text": dependency_text,
        "source_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},
        "limitations": ["Does not parse arbitrary CommonMark or check external links.",
                        "Phase-level/prose-only prerequisites and conditional task slices require semantic review.",
                        "No Rust product, native adapter or engine compilation/execution."],
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("markdown_files", "checked_local_fragments", "tasks", "steps", "errors", "dependency_cycle_candidates")}, indent=2))
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
