"""Comparable container observations; never hide lifecycle or mount changes."""
import copy


def projection(item):
    state = copy.deepcopy(item['State'])
    if 'Health' in state:
        # Completed periodic probes rotate this diagnostic log without changing
        # configuration/lifecycle. Keep status and failing streak in comparison.
        state['Health'].pop('Log', None)
    return dict(Id=item['Id'], Name=item['Name'], Image=item['Image'], State=state,
                Mounts=sorted(item['Mounts'], key=lambda mount: mount['Destination']))
