from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).with_name("monitor-urls.py")
spec = importlib.util.spec_from_file_location("stress_monitor_urls", SCRIPT)
if spec is None or spec.loader is None:
    raise RuntimeError("cannot load monitor-urls.py")
monitor_urls = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor_urls)


class MonitorUrlTests(unittest.TestCase):
    def test_cluster_two_monitor_urls_follow_the_first_run_server_not_intel1_default(self) -> None:
        connz, varz, jsz = monitor_urls.derive_monitor_urls(
            "tls://ryzen7.tail62219a.ts.net:4333,tls://ryzen11.tail62219a.ts.net:4333,"
            "tls://ryzen3.tail62219a.ts.net:4333"
        )
        self.assertEqual(connz, "http://ryzen7.tail62219a.ts.net:8222/connz")
        self.assertEqual(varz, "http://ryzen7.tail62219a.ts.net:8222/varz")
        self.assertEqual(jsz, "http://ryzen7.tail62219a.ts.net:8222/jsz?streams=1")
        self.assertNotIn("intel1", connz + varz + jsz)

    def test_production_server_list_remains_a_valid_counterexample(self) -> None:
        connz, varz, _jsz = monitor_urls.derive_monitor_urls("tls://intel1.tail62219a.ts.net:4222")
        self.assertEqual(connz, "http://intel1.tail62219a.ts.net:8222/connz")
        self.assertEqual(varz, "http://intel1.tail62219a.ts.net:8222/varz")

    def test_rejects_non_tls_or_credential_bearing_server_urls(self) -> None:
        for value in (
            "nats://ryzen7.tail62219a.ts.net:4333",
            "tls://user:secret@ryzen7.tail62219a.ts.net:4333",
            "tls://127.0.0.1:4333",
            "tls://ryzen7.tail62219a.ts.net:4333/path",
            "tls://bad..name:4333",
            "tls://ryzen7.tail62219a.ts.net:4333,",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                monitor_urls.derive_monitor_urls(value)


if __name__ == "__main__":
    unittest.main()
