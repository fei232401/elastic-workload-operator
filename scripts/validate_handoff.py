import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import socket
import subprocess
import time
import urllib.request

NAMESPACE = "ew-validation"
NAME = "handoff"


class Run:
    def __init__(self, kubeconfig: Path, root: Path, image: str, trainer: str):
        self.base = ["kubectl", "--kubeconfig", str(kubeconfig), "--context", "kind-ew-recovery-lab"]
        self.root, self.image, self.trainer = root, image, trainer
        self.forward = None
        self.endpoint = None
        self.report = {"started_at": datetime.now(timezone.utc).isoformat(), "steps": [], "status": "running"}

    def kubectl(self, *args, body=None, check=True, namespaced=True, timeout=120):
        command = self.base + (["-n", NAMESPACE] if namespaced else []) + list(args)
        result = subprocess.run(command, input=body, text=True, capture_output=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(f"{' '.join(args)}: {result.stderr[-1500:]}")
        return result.stdout

    def json(self, *args):
        raw = self.kubectl(*args, "-o", "json", check=False)
        return json.loads(raw) if raw.strip() else None

    def wait(self, label, predicate, timeout=300):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            value = predicate()
            if value:
                return value
            time.sleep(1)
        raise TimeoutError(label)

    def record(self, name, **evidence):
        self.report["steps"].append({"name": name, "at": datetime.now(timezone.utc).isoformat(), **evidence})
        print("PASS", name, json.dumps(evidence, ensure_ascii=False)[:400], flush=True)
        self.save()

    def save(self):
        output = self.root / "artifacts/handoff-validation.json"
        output.parent.mkdir(exist_ok=True)
        output.write_text(json.dumps(self.report, ensure_ascii=False, indent=2) + "\n")

    def start_forward(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        self.forward = subprocess.Popen(self.base + ["-n", NAMESPACE, "port-forward", "deployment/mockrouter", f"{port}:8080"],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.endpoint = f"http://127.0.0.1:{port}"

        def ready():
            try:
                with urllib.request.urlopen(self.endpoint + "/ttft_p95", timeout=2):
                    return True
            except OSError:
                return False
        self.wait("mockrouter port-forward", ready, 30)

    def ttft(self, value):
        with urllib.request.urlopen(f"{self.endpoint}/control?ttft={value}&samples=100&freeze=0", timeout=5) as response:
            response.read()

    def workload(self):
        return self.json("get", "elasticworkloads", NAME) or {}

    def status(self):
        return self.workload().get("status", {})

    def events(self):
        raw = self.kubectl("exec", f"{NAME}-training-0", "--", "cat", "/ckpt/events.jsonl", check=False)
        return [json.loads(line) for line in raw.splitlines() if line.strip()]

    def resource(self):
        resources = {"requests": {"cpu": "300m", "memory": "600Mi"}, "limits": {"cpu": "1", "memory": "900Mi"}}
        return {
            "apiVersion": "aiinfra.example.com/v1alpha1", "kind": "ElasticWorkload",
            "metadata": {"name": NAME, "namespace": NAMESPACE},
            "spec": {
                "training": {"replicas": 2, "minReplicas": 1, "maxReplicas": 2, "gpuPerWorker": 0, "image": self.trainer,
                             "resources": resources,
                             "checkpoint": {"claimName": "trainer-ckpt", "timeoutSeconds": 90}},
                "inference": {"replicas": 1, "minReplicas": 1, "maxReplicas": 2, "gpuPerInstance": 0, "image": "busybox:1.36",
                              "command": ["sleep", "36000"],
                              "resources": {"requests": resources["requests"], "limits": {"cpu": "300m", "memory": "600Mi"}}},
                "scalingPolicy": {"targetMetric": "ttft_p95", "slo": 300, "minHoldDuration": 1, "maxScaleStep": 1},
            },
        }

    def prepare(self):
        for name in ("acceptance", "bootstrap", "zero", "broken", NAME):
            self.kubectl("delete", "elasticworkloads", name, "--ignore-not-found", "--wait=true", timeout=300)
        self.kubectl("delete", "pvc", "trainer-ckpt", "--ignore-not-found", "--wait=true", timeout=120)
        self.wait("old pods gone", lambda: not [p for p in self.json("get", "pods")["items"]
                                                if p["metadata"]["name"].startswith(("acceptance-", NAME + "-"))], 300)
        self.kubectl("apply", "--server-side", "--force-conflicts", "-f", str(self.root / "config/crd/bases"), namespaced=False)
        self.kubectl("apply", "-f", str(self.root / "config/rbac/role.yaml"), namespaced=False)
        self.kubectl("set", "image", "deployment/ew-controller", f"manager={self.image}")
        self.kubectl("rollout", "status", "deployment/ew-controller", "--timeout=180s", timeout=200)
        self.kubectl("apply", "-f", "-", body=json.dumps({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": "trainer-ckpt", "namespace": NAMESPACE},
            "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "256Mi"}}},
        }))
        self.record("prepared", controller=self.image)

    def wait_ready(self, training, inference, label, timeout=600):
        def ready():
            status = self.status()
            if status.get("handoff"):
                return False
            conditions = {c["type"]: c for c in status.get("conditions", [])}
            target = status.get("desiredReplicas", {})
            return conditions.get("Ready", {}).get("status") == "True" and target == {"training": training, "inference": inference}
        self.wait(label, ready, timeout)

    def wait_resume(self, world, after, label, timeout=300):
        def resumed():
            for event in self.events():
                if event["event"] == "resume" and event["world_size"] == world and event["time"] > after:
                    return event
            return None
        return self.wait(label, resumed, timeout)

    def handoff(self, ttft, training, inference, label):
        started = time.time()
        phases = []
        self.ttft(ttft)

        def track():
            handoff = self.status().get("handoff")
            if handoff and (not phases or phases[-1]["phase"] != handoff["phase"]):
                phases.append({"phase": handoff["phase"], "seen_after_s": round(time.time() - started, 1)})
            return handoff is None and phases and self.status().get("desiredReplicas") == {"training": training, "inference": inference}
        self.wait(f"{label} handoff", track, 600)
        self.ttft(210)
        self.wait_ready(training, inference, f"{label} convergence")
        resume = self.wait_resume(training, started, f"{label} trainer resume")
        checkpoints = [e for e in self.events() if e["event"] == "checkpoint" and e["time"] > started]
        requested = next((e for e in checkpoints if e["reason"] == "requested"), None)
        handoff_event = self.kubectl("get", "events", "--field-selector", f"involvedObject.name={NAME},reason=HandoffCompleted",
                                     "-o", "jsonpath={.items[-1].message}", check=False)
        last_saved = max((e for e in checkpoints if e["time"] <= resume["time"]), key=lambda e: e["time"], default=None)
        self.record(label, phases=phases, handoff_event=handoff_event,
                    requested_checkpoint=requested, last_checkpoint_before_resume=last_saved, resume=resume,
                    resumed_from_saved_step=bool(last_saved) and resume["step"] == last_saved["step"],
                    checkpoint_request_to_resume_s=round(resume["time"] - (requested or last_saved or resume)["time"], 2),
                    training_paused_s=round(resume["time"] - (last_saved or resume)["time"], 2),
                    total_s=round(time.time() - started, 1))
        if not last_saved or resume["step"] != last_saved["step"]:
            raise AssertionError(f"{label}: trainers did not resume from the last checkpoint")

    def crash(self):
        progress = [e for e in self.events() if e["event"] == "progress"]
        before = progress[-1]["step"] if progress else None
        killed_at = time.time()
        self.kubectl("delete", "pod", f"{NAME}-training-1", "--grace-period=0", "--force", check=False)
        resume = self.wait_resume(2, killed_at, "forced delete recovery", 600)
        saved = [e for e in self.events() if e["event"] == "checkpoint" and e["time"] <= killed_at]
        self.record("forced_delete_sigterm_recovery", note="kubectl delete --force still sends SIGTERM, so this measures graceful restart; scripts/crash_trainer.sh is the SIGKILL crash test", last_progress_before_kill=before,
                    last_checkpoint_before_kill=saved[-1] if saved else None, resume=resume,
                    steps_lost_at_least=(before - resume["step"]) if before is not None else None,
                    recovery_s=round(resume["time"] - killed_at, 2))

    def run(self):
        try:
            self.prepare()
            self.start_forward()
            self.ttft(210)
            started = time.time()
            self.kubectl("apply", "-f", "-", body=json.dumps(self.resource()))
            self.wait_ready(2, 1, "initial ready")
            first = self.wait_resume(2, started - 1, "initial trainer rendezvous")
            self.wait("training progress", lambda: any(e["event"] == "progress" for e in self.events()), 300)
            self.record("initial", resume=first, statefulset_ready=self.json("get", "statefulset", f"{NAME}-training")["status"].get("readyReplicas"))
            self.handoff(600, 1, 2, "overload_handoff")
            self.handoff(120, 2, 1, "recovery_handoff")
            self.wait("progress after recovery", lambda: sum(e["event"] == "progress" for e in self.events()) >= 3, 300)
            self.crash()
            losses = [e.get("loss") for e in self.events() if e["event"] == "progress"]
            self.record("loss_trace", first=losses[:3], last=losses[-3:], samples=len(losses))
            self.report["status"] = "passed"
        except Exception as exc:
            self.report["status"] = "failed"
            self.report["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            self.report["finished_at"] = datetime.now(timezone.utc).isoformat()
            self.save()
            if self.forward:
                self.forward.terminate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--image", default="ew-operator:handoff-v1")
    parser.add_argument("--trainer", default="ew-trainer:v1")
    args = parser.parse_args()
    Run(args.kubeconfig, Path(__file__).resolve().parents[1], args.image, args.trainer).run()
