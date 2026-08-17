import unittest

from diagnostics import command_for


class DiagnosticsTests(unittest.TestCase):
    def test_list_command_is_namespace_confined(self):
        self.assertEqual(
            ["kubectl", "get", "pods", "--namespace", "workloads", "--output", "json"],
            command_for("workloads", {"kind": "pods"}),
        )

    def test_named_command_uses_argv_not_shell(self):
        self.assertEqual(
            ["kubectl", "get", "deployments", "api", "--namespace", "workloads", "--output", "json"],
            command_for("workloads", {"kind": "deployments", "name": "api"}),
        )

    def test_invalid_values_are_rejected(self):
        with self.assertRaises(ValueError):
            command_for("workloads", {"kind": "pods; rm -rf /"})
        with self.assertRaises(ValueError):
            command_for("workloads", {"kind": "pods", "name": "../../secret"})


if __name__ == "__main__":
    unittest.main()
