import copy
import unittest
from inventory import projection


class InventoryComparison(unittest.TestCase):
    def setUp(self):
        self.original = dict(Id='one', Name='fixture', Image='sha256:original',
            State=dict(Running=True, Pid=123, StartedAt='original', OOMKilled=False,
                       Health=dict(Status='healthy', FailingStreak=0, Log=['old probe'])),
            Mounts=[dict(Destination='/b', Source='/original/b', RW=False),
                    dict(Destination='/a', Source='/original/a', RW=True)])

    def test_order_and_probe_log_do_not_change_the_observed_resources(self):
        after = copy.deepcopy(self.original)
        after['Mounts'].reverse()
        after['State']['Health']['Log'] = ['new probe']
        self.assertEqual(projection(self.original), projection(after))
        self.assertEqual(self.original['State']['Health']['Log'], ['old probe'])

    def test_material_changes_are_still_detected(self):
        changes = [
            lambda x: x.update(Image='sha256:different'),
            lambda x: x['State'].update(Running=False),
            lambda x: x['State'].update(Pid=456),
            lambda x: x['State'].update(StartedAt='new'),
            lambda x: x['State'].update(OOMKilled=True),
            lambda x: x['State']['Health'].update(Status='unhealthy'),
            lambda x: x['State']['Health'].update(FailingStreak=1),
            lambda x: x['Mounts'][0].update(Source='/changed'),
            lambda x: x['Mounts'][0].update(RW=True),
            lambda x: x['Mounts'].pop(),
        ]
        for change in changes:
            after = copy.deepcopy(self.original)
            change(after)
            self.assertNotEqual(projection(self.original), projection(after))
