import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time


class Demo:
    def __init__(self, kubeconfig: Path, root: Path):
        self.base = ["kubectl", "--kubeconfig", str(kubeconfig), "--context", "kind-ew-recovery-lab"]
        self.root = root
        self.report = {"started_at": datetime.now(timezone.utc).isoformat(), "timeline": [], "status": "running"}
        self.t0 = time.time()

    def kubectl(self, *args, body=None, check=True, timeout=180):
        result = subprocess.run(self.base + list(args), input=body, text=True, capture_output=True, timeout=timeout)
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

    def mark(self, name, **evidence):
        entry = {"name": name, "t": round(time.time() - self.t0, 1), **evidence}
        self.report["timeline"].append(entry)
        print("STEP", json.dumps(entry, ensure_ascii=False)[:500], flush=True)
        self.save()

    def save(self):
        path = self.root / "artifacts/kueue-demo.json"
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps(self.report, ensure_ascii=False, indent=2) + "\n")

    def workload(self, namespace, job):
        items = (self.json("get", "workloads.kueue.x-k8s.io", "-n", namespace) or {}).get("items", [])
        for item in items:
            if any(ref.get("name") == job for ref in item["metadata"].get("ownerReferences", [])):
                return item
        return None

    def conditions(self, namespace, job):
        workload = self.workload(namespace, job)
        if not workload:
            return {}
        return {c["type"]: c for c in workload.get("status", {}).get("conditions", [])}

    def admitted(self, namespace, job):
        return self.conditions(namespace, job).get("Admitted", {}).get("status") == "True"

    def pod(self, namespace, job, phase=None):
        items = (self.json("get", "pods", "-n", namespace, "-l", f"job-name={job}") or {}).get("items", [])
        for item in items:
            if item["metadata"].get("deletionTimestamp"):
                continue
            if phase is None or item["status"].get("phase") == phase:
                return item["metadata"]["name"]
        return None

    def events(self):
        name = self.pod("team-b", "trainer-b", "Running")
        if not name:
            return []
        raw = self.kubectl("exec", "-n", "team-b", name, "--", "cat", "/ckpt/events.jsonl", check=False)
        return [json.loads(line) for line in raw.splitlines() if line.strip()]

    def usage(self):
        result = {}
        for queue in ("team-a", "team-b"):
            status = (self.json("get", "clusterqueues.kueue.x-k8s.io", queue) or {}).get("status", {})
            result[queue] = {"admitted": status.get("admittedWorkloads"), "pending": status.get("pendingWorkloads"),
                             "usage": status.get("flavorsUsage") or status.get("flavorsReservation")}
        return result

    def run(self):
        try:
            for namespace, job in (("team-a", "urgent-a"), ("team-b", "trainer-b")):
                self.kubectl("delete", "job", job, "-n", namespace, "--ignore-not-found", "--wait=true", check=False)
            self.kubectl("delete", "pvc", "trainer-b-ckpt", "-n", "team-b", "--ignore-not-found", "--wait=true", check=False)
            demo = self.root / "config/kueue-demo"
            self.kubectl("apply", "-f", str(demo / "queues.yaml"))
            self.kubectl("apply", "-f", str(demo / "trainer-b.yaml"))
            self.wait("trainer-b admitted", lambda: self.admitted("team-b", "trainer-b"))
            self.mark("trainer_b_admitted_by_borrowing", usage=self.usage())
            self.wait("trainer-b running", lambda: self.pod("team-b", "trainer-b", "Running"), 300)
            self.wait("trainer-b progress", lambda: any(e["event"] == "progress" and e["step"] >= 100 for e in self.events()), 600)
            before = [e for e in self.events() if e["event"] == "progress"][-1]
            self.mark("trainer_b_training", last_progress=before)

            submitted = time.time()
            self.kubectl("apply", "-f", str(demo / "urgent-a.yaml"))
            self.wait("trainer-b preempted", lambda: not self.admitted("team-b", "trainer-b"), 300)
            evicted = self.conditions("team-b", "trainer-b")
            self.mark("trainer_b_preempted", seconds_after_submit=round(time.time() - submitted, 1),
                      evicted=evicted.get("Evicted"), preempted=evicted.get("Preempted"))
            self.wait("urgent-a admitted", lambda: self.admitted("team-a", "urgent-a"), 300)
            self.mark("urgent_a_admitted", seconds_after_submit=round(time.time() - submitted, 1), usage=self.usage())

            self.wait("trainer-b readmitted", lambda: self.admitted("team-b", "trainer-b"), 600)
            self.mark("trainer_b_readmitted", seconds_after_submit=round(time.time() - submitted, 1))

            def resumed():
                rows = self.events()
                resumes = [e for e in rows if e["event"] == "resume" and e["time"] > submitted]
                return (rows, resumes[0]) if resumes else None
            rows, resume = self.wait("trainer-b resumed", resumed, 600)
            saved = [e for e in rows if e["event"] == "checkpoint" and submitted <= e["time"] <= resume["time"]]
            self.mark("trainer_b_resumed", checkpoint_on_preemption=saved[-1] if saved else None, resume=resume,
                      resumed_from_saved_step=bool(saved) and saved[-1]["step"] == resume["step"],
                      urgent_status=(self.json("get", "job", "urgent-a", "-n", "team-a") or {}).get("status", {}).get("succeeded"))
            if not saved or saved[-1]["step"] != resume["step"]:
                raise AssertionError("preempted trainer did not resume from the checkpoint saved on eviction")
            self.report["status"] = "passed"
        except Exception as exc:
            self.report["status"] = "failed"
            self.report["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            self.report["finished_at"] = datetime.now(timezone.utc).isoformat()
            self.save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubeconfig", type=Path, required=True)
    args = parser.parse_args()
    Demo(args.kubeconfig, Path(__file__).resolve().parents[1]).run()
