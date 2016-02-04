import mock
from celery import chord
from celeryone import QueueOne

from celery.tests.case import AppCase
from fakeredis import FakeRedis


@mock.patch('celeryone.QueueOne.redis', FakeRedis())
class test_chord(AppCase):

    def setup_method(self, test_method):
        super(test_chord, self).setUp()
        @self.app.task(name="example_group", base=QueueOne, one_options={'fail': False, 'keys': ['a']}, shared=False)
        def example_group(a):
            print(a)
            return a
        self.example_group = example_group

        @self.app.task(shared=False)
        def print_res(a):
            print(a)
        self.print_res = print_res

    def teardown_method(self, test_method):
        pass

    def test_run(self):
        r1 = chord((self.example_group.s(1), self.example_group.s(2)))
        r2 = chord((self.example_group.s(1), self.example_group.s(3)))
        body = self.print_res.s()
        result1 = r1(body)
        result2 = r2(body)
        i = self.app.control.inspect()
        print(i.registered())
        print(result1)
        print(result2)

