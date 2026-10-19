#!/usr/bin/env bash
K="kubectl --kubeconfig ${1:?usage: crash_trainer.sh path/to/kubeconfig} --context kind-ew-recovery-lab -n ew-validation"
events() { $K exec handoff-training-0 -- cat /ckpt/events.jsonl; }
events > ${TMPDIR:-/tmp}/ev-before.jsonl
kill_time=$($K exec handoff-training-1 -- python -c "
import os, signal, time
for pid in os.listdir('/proc'):
    if pid.isdigit():
        try:
            cmd = open(f'/proc/{pid}/cmdline','rb').read().split(b'\0')
        except OSError:
            continue
        if any(part.endswith(b'train.py') for part in cmd) and b'torchrun' not in cmd[0] and not any(b'torchrun' in part for part in cmd[:2]):
            os.kill(int(pid), signal.SIGKILL)
            print(time.time())
            break
")
echo "kill_time=$kill_time"
[ -n "$kill_time" ] || { echo "没找到训练 worker 进程"; exit 1; }
for i in $(seq 1 120); do
  sleep 3
  events > ${TMPDIR:-/tmp}/ev-after.jsonl 2>/dev/null || continue
  python3 - "$kill_time" <<'P' && break
import json, sys
kill = float(sys.argv[1])
rows = [json.loads(l) for l in open("${TMPDIR:-/tmp}/ev-after.jsonl") if l.strip()]
resume = [r for r in rows if r["event"] == "resume" and r["time"] > kill]
if not resume:
    sys.exit(1)
resume = resume[0]
progress = [r for r in rows if r["event"] == "progress" and r["time"] <= kill]
p2, p1 = progress[-1], progress[-2]
rate = (p2["time"] - p1["time"]) / (p2["step"] - p1["step"])
at_kill = p2["step"] + (kill - p2["time"]) / rate
saved = [r for r in rows if r["event"] == "checkpoint" and r["time"] <= kill][-1]
after = [r for r in rows if r["event"] == "checkpoint" and r["time"] > kill]
print(json.dumps({"kill_time": kill, "estimated_step_at_kill": round(at_kill), "seconds_per_step": round(rate, 3),
  "last_checkpoint_before_kill": saved, "checkpoints_between_kill_and_resume": after[:3], "resume": resume,
  "steps_lost": round(at_kill) - resume["step"], "work_lost_s": round((at_kill - resume["step"]) * rate, 1),
  "recovery_s": round(resume["time"] - kill, 1)}, ensure_ascii=False, indent=1))
P
done
$K get pods -l elasticworkload.aiinfra.example.com/workload=training
