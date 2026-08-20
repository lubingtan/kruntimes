import json
import unittest

from kruntimes.sandbox import (
    APIError,
    Command,
    CreateOptions,
    HTTPResponse,
    SandboxClient,
    SandboxStateError,
)
from kruntimes.kubernetes import PortForwardGatewayTransport


class FakeRuns:
    def __init__(self, run):
        self.run = run
        self.created = None

    def create(self, namespace, run):
        self.created = (namespace, run)
        metadata = run["metadata"]
        self.run["metadata"]["name"] = metadata.get("name", "sandbox")
        self.run["metadata"]["namespace"] = metadata["namespace"]
        return self.run

    def get(self, namespace, name):
        return self.run

    def replace(self, namespace, name, run):
        self.run = run
        return run


class FakeGateway:
    def __init__(self):
        self.requests = []
        self.response = HTTPResponse(200, b"{}")

    def request(self, method, url, body, headers):
        self.requests.append((method, url, body, headers))
        return self.response


class FakeLogs:
    def read(self, namespace, pod, container):
        return '{"run_uid":"run-uid","stream":"audit","message":"command"}\n{"run_uid":"other","stream":"audit","message":"ignored"}'


def ready_run():
    return {
        "metadata": {"name": "sandbox", "namespace": "default", "uid": "run-uid"},
        "spec": {"mode": {"session": {}}},
        "status": {
            "phase": "Ready",
            "assignedPod": "runtime-pod",
            "endpoint": {"url": "https://gateway/v1/namespaces/default/runtimes/python/sessions/run-uid"},
        },
    }


class SandboxTests(unittest.TestCase):
    def test_create_sorts_environment_and_executes(self):
        runs = FakeRuns(ready_run())
        gateway = FakeGateway()
        client = SandboxClient(runs, gateway, bearer_token="token")
        sandbox = client.create(CreateOptions(namespace="default", name="sandbox", runtime="python", env={"B": "2", "A": "1"}))
        gateway.response = HTTPResponse(200, json.dumps({"command": {"exitCode": 0, "stdout": "b2s="}}).encode())

        result = sandbox.execute(Command(argv=["python", "-V"]))

        self.assertEqual(0, result.exit_code)
        self.assertEqual(b"ok", result.stdout)
        self.assertEqual([{"name": "A", "value": "1"}, {"name": "B", "value": "2"}], runs.created[1]["spec"]["env"])
        self.assertEqual("Bearer token", gateway.requests[0][3]["Authorization"])

    def test_read_file_preserves_path_for_runtime_boundary_validation(self):
        gateway = FakeGateway()
        gateway.response = HTTPResponse(200, b'{"contents":"b2s=","truncated":false}')
        sandbox = SandboxClient(FakeRuns(ready_run()), gateway).open("default", "sandbox")

        contents, truncated = sandbox.read_file("../outside.txt")

        self.assertEqual(b"ok", contents)
        self.assertFalse(truncated)
        self.assertTrue(gateway.requests[0][1].endswith("/files/../outside.txt"))

    def test_logs_filter_run_uid(self):
        sandbox = SandboxClient(FakeRuns(ready_run()), FakeGateway(), logs=FakeLogs()).open("default", "sandbox")
        lines = sandbox.logs()
        self.assertEqual(1, len(lines))
        self.assertEqual("command", lines[0].message)

    def test_open_rejects_non_session_run(self):
        run = ready_run()
        run["spec"] = {"mode": {"task": {}}}
        with self.assertRaises(SandboxStateError):
            SandboxClient(FakeRuns(run), FakeGateway()).open("default", "sandbox")

    def test_gateway_error_is_typed(self):
        gateway = FakeGateway()
        gateway.response = HTTPResponse(403, b'{"error":"forbidden"}')
        sandbox = SandboxClient(FakeRuns(ready_run()), gateway).open("default", "sandbox")
        with self.assertRaises(APIError) as error:
            sandbox.execute(Command(shell="true"))
        self.assertEqual(403, error.exception.status_code)

    def test_close_requests_drain(self):
        run = ready_run()
        run["status"]["phase"] = "Succeeded"
        runs = FakeRuns(run)
        sandbox = SandboxClient(runs, FakeGateway()).open("default", "sandbox")

        sandbox.close()

        self.assertEqual("Drain", runs.run["spec"]["termination"]["mode"])

    def test_cancel_requests_immediate_and_escalates_drain(self):
        run = ready_run()
        run["status"]["phase"] = "Cancelled"
        run["spec"]["termination"] = {"mode": "Drain"}
        runs = FakeRuns(run)
        sandbox = SandboxClient(runs, FakeGateway()).open("default", "sandbox")

        sandbox.cancel()

        self.assertEqual("Immediate", runs.run["spec"]["termination"]["mode"])

    def test_close_does_not_downgrade_immediate_termination(self):
        run = ready_run()
        run["status"]["phase"] = "Succeeded"
        run["spec"]["termination"] = {"mode": "Immediate"}
        runs = FakeRuns(run)
        sandbox = SandboxClient(runs, FakeGateway()).open("default", "sandbox")

        sandbox.close()

        self.assertEqual("Immediate", runs.run["spec"]["termination"]["mode"])

    def test_termination_reports_unexpected_terminal_phase(self):
        failed = ready_run()
        failed["status"]["phase"] = "Failed"
        with self.assertRaises(SandboxStateError):
            SandboxClient(FakeRuns(failed), FakeGateway()).open("default", "sandbox").close()

        succeeded = ready_run()
        succeeded["status"]["phase"] = "Succeeded"
        with self.assertRaises(SandboxStateError):
            SandboxClient(FakeRuns(succeeded), FakeGateway()).open("default", "sandbox").cancel()

    def test_port_forward_preserves_endpoint_path(self):
        gateway = FakeGateway()
        transport = PortForwardGatewayTransport(gateway, "http://127.0.0.1:19090")

        transport.request("GET", "https://gateway/v1/namespaces/default/runtimes/python/sessions/run-uid/files?maxBytes=10", None, {})

        self.assertEqual(
            "http://127.0.0.1:19090/v1/namespaces/default/runtimes/python/sessions/run-uid/files?maxBytes=10",
            gateway.requests[0][1],
        )


if __name__ == "__main__":
    unittest.main()
