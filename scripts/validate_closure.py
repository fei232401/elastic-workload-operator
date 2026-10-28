import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import socket
import subprocess
import time
import urllib.request

import validate_handoff


NAME = "closure-probe"
NAMESPACE = "ew-validation"
GATE = "closure.example.com/Ready"
SERVER = """import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'ok')
    def do_POST(self):
        request_id = int(parse_qs(urlparse(self.path).query)['id'][0])
        self.send_response(202)
        self.end_headers()
        self.wfile.write(json.dumps({'checkpoint_id': request_id if Path('/ckpt/allow').exists() else 0, 'step': 42}).encode())
    def log_message(self, *args):
        pass
HTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
"""


def main(kubeconfig, image):
    root = Path(__file__).resolve().parents[1]
    validate_handoff.NAME = NAME
    run = validate_handoff.Run(kubeconfig, root, image, "ew-trainer:v1")
    evidence = root / "docs/evidence/closure-20261020/kind-faults.json"
    report = {"status": "running", "started_at": datetime.now(timezone.utc).isoformat(), "checks": [],
              "conditions": {"cluster": "Kind single node", "namespace": NAMESPACE, "trainer": "HTTP checkpoint protocol stub, not real training", "inference": "busybox with injected readiness gate"}}
    forward = None
    holder_created = False
    created = False

    def record(name, **facts):
        report["checks"].append({"name": name, **facts})
        print("PASS", name, flush=True)

    def patch(kind, name, data, patch_type="merge", subresource=None):
        arguments = ["patch", kind, name, "--type", patch_type, "-p", json.dumps(data)]
        if subresource:
            arguments.extend(["--subresource", subresource])
        run.kubectl(*arguments)

    def condition(name):
        return next((entry for entry in run.status().get("conditions", []) if entry["type"] == name), {})

    def phase(expected):
        return run.status().get("handoff", {}).get("phase") == expected

    def guard(expected):
        handoff = run.status()["handoff"]
        patch("elasticworkload", NAME, {"spec": {"scalingPolicy": {"slo": 301}}})
        run.wait("spec change frozen", lambda: condition("Ready").get("reason") == "HandoffSpecChanged", 30)
        assert phase(expected), "spec edit advanced the handoff"
        assert run.status()["handoff"]["checkpointID"] == handoff["checkpointID"]
        patch("elasticworkload", NAME, [{"op": "test", "path": "/status/handoff/checkpointID", "value": handoff["checkpointID"]},
                                       {"op": "replace", "path": "/spec", "value": handoff["specSnapshot"]}], "json")
        run.wait("original spec resumes", lambda: condition("Ready").get("reason") != "HandoffSpecChanged", 30)
        record("spec_guard_" + expected)

    try:
        controller = run.json("get", "deployment", "ew-controller")
        assert controller["spec"]["template"]["spec"]["containers"][0]["image"] == image, "deploy the expected controller image first"
        existing = run.json("get", "elasticworkload", NAME)
        if existing or run.json("get", "pvc", NAME + "-ckpt"):
            raise RuntimeError("closure-probe or its PVC already exists; inspect it before rerunning")
        run.kubectl("apply", "-f", "-", body=json.dumps({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                    "metadata": {"name": NAME + "-ckpt", "namespace": NAMESPACE},
                    "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "64Mi"}}}}))
        created = True
        for kind in ["Service", "Deployment"]:
            metadata = {"name": NAME + "-metrics", "namespace": NAMESPACE}
            labels = {"app": NAME + "-metrics"}
            spec = {"selector": labels, "ports": [{"port": 8080, "targetPort": 8080}]} if kind == "Service" else {
                "replicas": 1, "selector": {"matchLabels": labels}, "template": {"metadata": {"labels": labels},
                "spec": {"containers": [{"name": "metrics", "image": "ew-mockrouter:recovery-v1", "imagePullPolicy": "IfNotPresent"}]}}}
            run.kubectl("apply", "-f", "-", body=json.dumps({"apiVersion": "v1" if kind == "Service" else "apps/v1", "kind": kind, "metadata": metadata, "spec": spec}))
        run.kubectl("rollout", "status", "deployment/" + NAME + "-metrics", "--timeout=90s")
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        forward = subprocess.Popen(run.base + ["-n", NAMESPACE, "port-forward", "deployment/" + NAME + "-metrics", f"{port}:8080"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        endpoint = f"http://127.0.0.1:{port}"

        def metric(value):
            try:
                with urllib.request.urlopen(f"{endpoint}/control?ttft={value}&samples=100&freeze=0", timeout=2) as response:
                    return response.status == 200
            except OSError:
                return False

        run.wait("metrics ready", lambda: metric(210), 30)
        resource = {"apiVersion": "aiinfra.example.com/v1alpha1", "kind": "ElasticWorkload", "metadata": {"name": NAME, "namespace": NAMESPACE},
                    "spec": {"training": {"replicas": 1, "minReplicas": 0, "maxReplicas": 1, "gpuPerWorker": 0, "image": "ew-trainer:v1",
                              "command": ["python", "-u", "-c", SERVER], "checkpoint": {"claimName": NAME + "-ckpt", "timeoutSeconds": 90}},
                             "inference": {"replicas": 1, "minReplicas": 1, "maxReplicas": 2, "gpuPerInstance": 0, "image": "busybox:1.36", "command": ["sleep", "36000"]},
                             "scalingPolicy": {"targetMetric": "ttft_p95", "slo": 300, "minHoldDuration": 1, "maxScaleStep": 1, "handoffTimeoutSeconds": 5,
                                               "metrics": {"routerURL": f"http://{NAME}-metrics:8080"}}}}
        run.kubectl("apply", "-f", "-", body=json.dumps(resource))
        run.wait("initial available", lambda: condition("Ready").get("status") == "True", 120)
        trainer = NAME + "-training-0"
        holder = NAME + "-held-training"
        run.kubectl("apply", "-f", "-", body=json.dumps({"apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": holder, "namespace": NAMESPACE,
                                 "labels": {"elasticworkload.aiinfra.example.com/name": NAME, "elasticworkload.aiinfra.example.com/workload": "training"}},
                    "spec": {"containers": [{"name": "held-resource", "image": "busybox:1.36", "command": ["sleep", "3600"]}]}}))
        holder_created = True
        run.wait("held resource running", lambda: run.json("get", "pod", holder)["status"].get("phase") == "Running", 60)
        assert metric(600)
        run.wait("checkpoint requested", lambda: phase("CheckpointRequested"), 30)
        guard("CheckpointRequested")
        patch("deployment", NAME + "-inference", {"spec": {"template": {"spec": {"readinessGates": [{"conditionType": GATE}]}}}})
        run.kubectl("exec", trainer, "--", "touch", "/ckpt/allow")
        run.wait("releasing", lambda: phase("Releasing"), 30)
        guard("Releasing")
        run.wait("releasing timeout", lambda: condition("HandoffStalled").get("status") == "True", 30)
        before = run.status()["handoff"]["phaseStartedAt"]
        record("releasing_timeout", phaseStartedAt=before, ready=condition("Ready"), progressing=condition("Progressing"))
        run.kubectl("rollout", "restart", "deployment/ew-controller")
        run.kubectl("rollout", "status", "deployment/ew-controller", "--timeout=120s")
        time.sleep(2)
        assert phase("Releasing") and run.status()["handoff"]["phaseStartedAt"] == before
        assert condition("HandoffStalled").get("status") == "True"
        record("restart_keeps_deadline")
        run.kubectl("delete", "pod", holder, "--grace-period=1", "--wait=true")
        holder_created = False
        run.wait("acquiring", lambda: phase("Acquiring"), 90)
        guard("Acquiring")
        run.wait("acquiring timeout", lambda: condition("HandoffStalled").get("status") == "True", 30)
        assert run.status()["handoff"]["phaseStartedAt"] != before
        record("acquiring_timeout", handoff=run.status()["handoff"], ready=condition("Ready"))
        assert metric(210)

        def release_readiness():
            pods = run.json("get", "pods", "-l", f"elasticworkload.aiinfra.example.com/name={NAME},elasticworkload.aiinfra.example.com/workload=inference")["items"]
            for pod in pods:
                if pod["metadata"].get("deletionTimestamp") or not pod["spec"].get("readinessGates"):
                    continue
                patch("pod", pod["metadata"]["name"], {"status": {"conditions": [{"type": GATE, "status": "True", "reason": "FaultRemoved", "message": "controlled test gate released", "lastTransitionTime": datetime.now(timezone.utc).isoformat()}]}}, "strategic", "status")
            return not run.status().get("handoff") and condition("Ready").get("status") == "True"

        run.wait("recovered after fault removal", release_readiness, 150)
        assert condition("HandoffStalled").get("status") == "False"
        record("fault_removal_recovers", desired=run.status()["desiredReplicas"])
        warnings = run.json("get", "events", "--field-selector", f"involvedObject.name={NAME},reason=HandoffPhaseTimeout")["items"]
        record("timeout_events", events=[{"reason": event["reason"], "count": event.get("count", 1), "message": event["message"]} for event in warnings])
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        if holder_created:
            run.kubectl("delete", "pod", NAME + "-held-training", "--grace-period=1", "--wait=true", check=False)
        if created:
            run.kubectl("delete", "elasticworkload", NAME, "--ignore-not-found", "--wait=true", check=False)
            run.kubectl("delete", "deployment,service", NAME + "-metrics", "--ignore-not-found", check=False)
            if report["status"] == "passed":
                run.wait("test pods removed", lambda: not run.json("get", "pods", "-l", f"elasticworkload.aiinfra.example.com/name={NAME}")["items"], 90)
                run.kubectl("delete", "pvc", NAME + "-ckpt", "--ignore-not-found", "--wait=true")
                report["owned_resources_cleaned"] = True
        if forward:
            forward.terminate()
            forward.wait(timeout=10)
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        evidence.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--image", default="ew-operator:closure-20261020-v2")
    arguments = parser.parse_args()
    main(arguments.kubeconfig, arguments.image)
