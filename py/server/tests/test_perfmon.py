#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import empty_table
from deephaven.perfmon import process_info_log, process_metrics_log, server_state_log, \
    query_operation_performance_log, query_performance_log, update_performance_log, metrics_get_counters, \
    metrics_reset_counters, query_performance_tree_table, query_operation_performance_tree_table, \
    update_performance_ancestors_log, ancestor_dot, ancestor_svg
from deephaven.perfmon import query_update_performance, query_performance, query_operation_performance, server_state

import os
import tempfile
from tests.testbase import BaseTestCase


def create_some_counters():
    t = empty_table(10).update(formulas=["X=i"])
    t2 = empty_table(10).update(formulas=["X=i"])
    return t.join(t2, on=["X"])


class PerfmonTestCase(BaseTestCase):
    # global performance log tables are supposed to be persistent, and in the production environment they are created
    # in the system default liveness scope during server initialization. The current test environment skips that as it
    # only partially initialize the server. So here we need to disable calling the super setUP to skip opening a user
    # liveness scope which would result in the destroying of these tables.
    def setUp(self) -> None:
        ...

    def tearDown(self) -> None:
        ...

    def test_metrics_get_counters(self):
        metrics_reset_counters()
        t = create_some_counters()
        # counters should accumulate some values after performing some operations
        counters_str = metrics_get_counters()
        t = create_some_counters()
        # counters now should have different values after performing the same operation one more time
        counters_str2 = metrics_get_counters()
        self.assertNotEqual(counters_str, counters_str2)

        # After reset and performing the same operation again, the counters' values should reset.
        # We can't ensure they are exactly the same as before, because the engine state may be
        # captured in some non-exactly-zero-counters due to other pre-existing table updates.
        metrics_reset_counters()
        t = create_some_counters()
        counters_str3 = metrics_get_counters()
        self.assertNotEqual(counters_str2, counters_str3)

    def test_process_logs(self):
        log_table = process_info_log()
        self.assertTrue(log_table.to_string())
        log_table = server_state_log()
        self.assertTrue(log_table.to_string())
        log_table = process_metrics_log()
        self.assertTrue(log_table.to_string())

    def test_query_logs(self):
        log_table = query_operation_performance_log()
        self.assertTrue(log_table.to_string())
        log_table = query_performance_log()
        self.assertTrue(log_table.to_string())
        log_table = update_performance_log()
        self.assertTrue(log_table.to_string())
        log_table = update_performance_ancestors_log()
        self.assertTrue(log_table.to_string())
        log_table = query_performance_tree_table()
        self.assertIsNotNone(log_table)

    def test_graphviz(self):
        upl = update_performance_log()
        ua = update_performance_ancestors_log()
        self.assertIsNotNone(ancestor_dot(0, upl, ua))
        self.assertIsNotNone(ancestor_svg(0, upl, ua, None))

        fp = tempfile.NamedTemporaryFile(suffix=".svg")
        fname = fp.name
        fp.close()

        svg_data = ancestor_svg(0, upl, ua, fname)
        self.assertIsNotNone(svg_data)

        if not os.path.exists(fname):
            self.fail(f"SVG output '{fname}' not found.")
        else:
            with open(fname, 'r') as f:
                text = f.read()
            self.assertEqual(text, svg_data)

        os.remove(fname)

    def test_performance_queries(self):
        q = query_performance(1)
        self.assertTrue(q.to_string())
        q = query_operation_performance(1)
        self.assertTrue(q.to_string())
        q = server_state()
        self.assertTrue(q.to_string())
        q = query_update_performance(1)
        self.assertTrue(q.to_string())
        q = query_operation_performance_tree_table()
        self.assertIsNotNone(q)


if __name__ == '__main__':
    unittest.main()
