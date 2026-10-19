import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import socket
import subprocess
import time
import urllib.request


class Validation:
    def __init__(self, kubeconfig, root):
        self.root = root
        self.base = ["kubectl", "--kubeconfig", str(kubeconfig), "--context", "kind-ew-recovery-lab", "-n", "ew-validation"]
        self.forward = None
        self.forward_log = None
        self.endpoint = None
        self.results = []

    def command(self, *args, body=None, missing_ok=False):
        result = subprocess.run(self.base + list(args), input=body, text=True, capture_output=True, timeout=60)
        if result.returncode:
            if missing_ok and "NotFound" in result.stderr:
                return None
            raise RuntimeError(result.stderr[-2000:])
        return result.stdout

    def get(self, kind, name):
        raw = self.command("get", kind, name, "-o", "json", missing_ok=True)
        return json.loads(raw) if raw else None

    def wait(self, label, predicate, timeout=120):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if predicate():
                return
            time.sleep(0.5)
        raise TimeoutError(label)

    def mark(self, name, evidence):
        self.results.append({"name": name, "status": "passed", "evidence": evidence})
        print("PASS", name, flush=True)
        self.write_report()

    def write_report(self):
        passed = bool(self.results) and self.results[-1]["name"] == "steady_final_state"
        failed = any(item["status"] == "failed" for item in self.results)
        (self.root / "artifacts/cluster-validation.json").write_text(json.dumps({
            "context": "kind-ew-recovery-lab", "namespace": "ew-validation",
            "status": "failed" if failed else "passed" if passed else "running",
            "updated_at": datetime.now(timezone.utc).isoformat(), "results": self.results,
        }, ensure_ascii=False, indent=2))

    def scale(self, name, replicas):
        self.command("scale", "deployment", name, f"--replicas={replicas}")
        self.wait(f"scale {name} to {replicas}", lambda: self.deployment_available(name, replicas))
        if replicas == 0:
            self.wait(f"terminate {name} pods", lambda: not json.loads(self.command(
                "get", "pods", "-l", f"app={name}", "-o", "json"))["items"])

    def deployment_available(self, name, replicas):
        dep = self.get("deployment", name)
        if not dep:
            return False
        status = dep.get("status", {})
        return status.get("observedGeneration", 0) >= dep["metadata"]["generation"] and all(
            status.get(key, 0) == replicas for key in ("replicas", "updatedReplicas", "availableReplicas", "readyReplicas")
        )

    def stop_forward(self):
        if self.forward:
            self.forward.terminate()
            try:
                self.forward.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.forward.kill()
                self.forward.wait(timeout=5)
            self.forward = None
        if self.forward_log:
            self.forward_log.close()
            self.forward_log = None

    def start_forward(self):
        self.stop_forward()
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        self.endpoint = f"http://127.0.0.1:{port}"
        self.forward_log = (self.root / "artifacts/mock-forward.log").open("a")
        self.forward = subprocess.Popen(self.base + ["port-forward", "svc/mockrouter", f"{port}:8080", "--address=127.0.0.1"],
                                        stdout=self.forward_log, stderr=subprocess.STDOUT)
        def available():
            if self.forward.poll() is not None:
                raise RuntimeError("mock port-forward exited")
            try:
                with urllib.request.urlopen(self.endpoint + "/ttft_p95", timeout=2):
                    return True
            except OSError:
                return False
        self.wait("mock port-forward", available, timeout=30)

    def control(self, query):
        with urllib.request.urlopen(self.endpoint + "/control?" + query, timeout=5) as response:
            response.read()

    def resource(self, name, zero=False, broken=False):
        return {
            "apiVersion": "aiinfra.example.com/v1alpha1", "kind": "ElasticWorkload",
            "metadata": {"name": name, "namespace": "ew-validation"},
            "spec": {
                "training": {"replicas": 0 if zero else 4, "minReplicas": 0 if zero else 1,
                             "maxReplicas": 0 if zero else 6, "gpuPerWorker": 0,
                             "image": "busybox:1.36", "command": ["sleep", "3600"]},
                "inference": {"replicas": 0 if zero else 2, "minReplicas": 0 if zero else 1,
                              "maxReplicas": 0 if zero else 6, "gpuPerInstance": 0,
                              "image": "busybox:1.36", "command": ["/missing-program"] if broken else ["sleep", "3600"]},
                "scalingPolicy": {"targetMetric": "ttft_p95", "slo": 300, "minHoldDuration": 1, "maxScaleStep": 1},
            },
        }

    def apply(self, resource):
        self.command("apply", "-f", "-", body=json.dumps(resource))

    def target(self, name):
        ew = self.get("elasticworkloads", name)
        if not ew:
            return None
        target = ew.get("status", {}).get("desiredReplicas", {})
        return target.get("training"), target.get("inference")

    def condition(self, name, kind, status, reason=None):
        ew = self.get("elasticworkloads", name)
        if not ew:
            return False
        return any(item["type"] == kind and item["status"] == status and (reason is None or item["reason"] == reason)
                   for item in ew.get("status", {}).get("conditions", []))

    def assert_stable(self, name, target, duration=6):
        history = len(self.get("elasticworkloads", name)["status"].get("scalingHistory", []))
        end = time.monotonic() + duration
        while time.monotonic() < end:
            current = self.get("elasticworkloads", name)
            if self.target(name) != target or len(current["status"].get("scalingHistory", [])) != history:
                raise AssertionError(f"{name} changed while decisions should be frozen")
            time.sleep(0.5)

    def remove(self, name):
        self.command("delete", "elasticworkloads", name, "--ignore-not-found", "--wait=false")
        self.wait(f"delete {name}", lambda: self.get("elasticworkloads", name) is None)
        for suffix in ("training", "inference"):
            self.wait(f"garbage collection {name}-{suffix}", lambda suffix=suffix: self.get("deployment", name + "-" + suffix) is None)

    def run(self):
        self.scale("ew-controller", 1)
        self.scale("mockrouter", 1)
        self.start_forward()
        self.control("ttft=210&samples=100&freeze=0")
        for name in ("acceptance", "bootstrap", "zero", "broken"):
            self.remove(name)
        self.apply(self.resource("acceptance"))
        self.wait("initial workload ready", lambda: self.condition("acceptance", "Ready", "True"))
        if self.target("acceptance") != (4, 2):
            raise AssertionError("initial target mismatch")
        self.mark("baseline", self.get("elasticworkloads", "acceptance")["status"])

        self.control("ttft=600")
        self.wait("overload target", lambda: self.target("acceptance") == (3, 3))
        self.control("ttft=210")
        self.wait("overload convergence", lambda: self.condition("acceptance", "Ready", "True"))
        self.mark("overload_yield", self.get("elasticworkloads", "acceptance")["status"])
        self.control("ttft=120")
        self.wait("return target", lambda: self.target("acceptance") == (4, 2))
        self.control("ttft=210")
        self.wait("return convergence", lambda: self.condition("acceptance", "Ready", "True"))
        self.mark("recovery_return", self.get("elasticworkloads", "acceptance")["status"])

        self.control("ttft=600&samples=0")
        self.wait("no sample protection", lambda: self.condition("acceptance", "MetricsReady", "False", "InsufficientSamples"))
        self.assert_stable("acceptance", (4, 2))
        self.mark("zero_samples_freeze", self.get("elasticworkloads", "acceptance")["status"])
        self.control("ttft=210&samples=100&freeze=1")
        self.wait("stale protection", lambda: self.condition("acceptance", "MetricsReady", "False", "MetricStale"), timeout=40)
        self.control("ttft=600")
        self.assert_stable("acceptance", (4, 2))
        self.mark("stale_freeze", self.get("elasticworkloads", "acceptance")["status"])
        self.control("ttft=210&freeze=0")
        self.wait("metric recovery", lambda: self.condition("acceptance", "MetricsReady", "True"))
        self.mark("metric_recovery", self.get("elasticworkloads", "acceptance")["status"])

        self.scale("ew-controller", 0)
        self.stop_forward()
        self.scale("mockrouter", 0)
        patch = {"status": {
            "desiredReplicas": {"training": 3, "inference": 3},
            "scalingHistory": [{"time": (datetime.now(timezone.utc)-timedelta(hours=1)).isoformat(),
                                "action": "inference+1/training-1", "reason": "overload", "metric": "ttft_p95=600ms"}],
        }}
        self.command("patch", "elasticworkloads", "acceptance", "--subresource=status", "--type=merge", "-p", json.dumps(patch))
        self.command("scale", "deployment", "acceptance-training", "--replicas=3")
        if self.get("deployment", "acceptance-inference")["spec"]["replicas"] != 2:
            raise AssertionError("partial failure fixture is not 3/2")
        self.scale("ew-controller", 1)
        self.wait("partial recovery while metrics offline", lambda: all(
            self.deployment_available("acceptance-" + role, 3) for role in ("training", "inference")))
        self.wait("metrics unavailable condition", lambda: self.condition("acceptance", "MetricsReady", "False", "MetricsUnavailable"))
        self.assert_stable("acceptance", (3, 3))
        self.mark("partial_apply_metrics_down_restart", self.get("elasticworkloads", "acceptance")["status"])
        self.scale("mockrouter", 1)
        self.start_forward()
        self.control("ttft=210&samples=100&freeze=0")
        self.wait("restored metric service", lambda: self.condition("acceptance", "MetricsReady", "True"))

        self.scale("ew-controller", 0)
        bootstrap = self.resource("bootstrap")
        bootstrap["metadata"]["finalizers"] = ["aiinfra.example.com/finalizer"]
        self.apply(bootstrap)
        if self.get("elasticworkloads", "bootstrap").get("status", {}).get("desiredReplicas"):
            raise AssertionError("bootstrap fixture unexpectedly initialized")
        self.scale("ew-controller", 1)
        self.wait("bootstrap initialization recovery", lambda: self.condition("bootstrap", "Ready", "True"))
        if self.target("bootstrap") != (4, 2) or not self.condition("bootstrap", "Initialized", "True"):
            raise AssertionError("bootstrap allocation not initialized")
        self.mark("finalizer_only_initialization", self.get("elasticworkloads", "bootstrap")["status"])
        self.remove("bootstrap")
        self.mark("finalizer_and_owner_gc", {"resource": "bootstrap", "children_removed": True})

        self.apply(self.resource("zero", zero=True))
        self.wait("zero replica initialization", lambda: self.condition("zero", "Ready", "True"))
        self.assert_stable("zero", (0, 0))
        self.mark("valid_zero_allocation", self.get("elasticworkloads", "zero")["status"])
        self.remove("zero")

        self.apply(self.resource("broken", broken=True))
        self.wait("workload not ready", lambda: self.condition("broken", "MetricsReady", "Unknown", "WaitingForWorkloads"))
        self.assert_stable("broken", (4, 2))
        if self.condition("broken", "Ready", "True"):
            raise AssertionError("broken workload incorrectly ready")
        self.mark("not_ready_blocks_decisions", self.get("elasticworkloads", "broken")["status"])
        self.remove("broken")
        self.mark("steady_final_state", self.get("elasticworkloads", "acceptance")["status"])

    def collect(self):
        for filename, arguments in {
            "controller.log": ("logs", "deployment/ew-controller", "--tail=500"),
            "cluster-events.json": ("get", "events", "-o", "json"),
            "cluster-pods.json": ("get", "pods", "-o", "json"),
            "cluster-workloads.json": ("get", "elasticworkloads", "-o", "json"),
        }.items():
            try:
                (self.root / "artifacts" / filename).write_text(self.command(*arguments))
            except Exception as exc:
                print("Evidence collection failed:", filename, str(exc), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubeconfig", required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    validation = Validation(args.kubeconfig.resolve(), root)
    cluster = json.loads(validation.command("config", "view", "--minify", "-o", "json"))
    if cluster["contexts"][0]["context"]["cluster"] != "kind-ew-recovery-lab":
        raise RuntimeError("Validation requires the dedicated kind-ew-recovery-lab cluster")
    try:
        validation.run()
    except Exception as exc:
        validation.results.append({"name": "validation_error", "status": "failed", "error": str(exc)})
        validation.write_report()
        raise
    finally:
        validation.collect()
        validation.stop_forward()
    print(f"Completed {len(validation.results)} cluster checks", flush=True)


if __name__ == "__main__":
    main()
