"""The explicit Hyper client lock delta allowed over the retained compiler cache."""
import copy
import hashlib
import tomllib


def verify_delta(previous, current):
    old = tomllib.loads(previous.read_text())
    new = tomllib.loads(current.read_text())
    additions = [
        dict(name='try-lock',version='0.2.5',checksum='e421abadd41a4225275504ea4d6566923418b7f05506fbc9c0fe86ba7396114b'),
        dict(name='want',version='0.3.1',checksum='bfa7760aed19e106de2c7c0b581b509f2f25d3dacaf737cb82ac61bc6d760b0e',dependencies=['try-lock']),
    ]
    expected = copy.deepcopy(old)
    hyper = [p for p in expected['package'] if p['name'] == 'hyper']
    assert len(hyper) == 1 and hyper[0]['version'] == '1.11.1'
    assert 'want' not in hyper[0]['dependencies']
    hyper[0]['dependencies'].append('want')
    for package in additions:
        expected['package'].append({**package,'source':'registry+https://github.com/rust-lang/crates.io-index'})
    for document in (expected,new):
        document['package'].sort(key=lambda p:(p['name'],p['version'],p.get('source','')))
    assert new == expected, 'Unexpected dependency change relative to compiler input'
    return dict(previous_sha256=hashlib.sha256(previous.read_bytes()).hexdigest(),
        current_sha256=hashlib.sha256(current.read_bytes()).hexdigest(),
        additions=additions,changed='Hyper1.11.1 adds want; every other package unchanged')


def verify_archives(cargo_home, delta):
    result = {}
    for package in delta['additions']:
        name = package['name']+'-'+package['version']+'.crate'
        files = list((cargo_home/'registry/cache').glob('*/'+name))
        assert len(files) == 1 and not files[0].is_symlink()
        digest = hashlib.sha256(files[0].read_bytes()).hexdigest()
        assert digest == package['checksum']
        result[name] = dict(sha256=digest,bytes=files[0].stat().st_size,
            path=str(files[0].relative_to(cargo_home)))
    return result
