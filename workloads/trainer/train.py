from __future__ import annotations

import json
import os
import signal
import threading
import time
from datetime import timedelta
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import torch
import torch.distributed as dist
from torch import nn
from torch.nn.parallel import DistributedDataParallel


@dataclass(frozen=True)
class Config:
    checkpoint_dir: Path
    checkpoint_every: int
    total_steps: int
    global_batch: int
    step_sleep: float
    control_port: int
    seed: int

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            checkpoint_dir=Path(os.environ.get("CHECKPOINT_DIR", "/tmp/ckpt")),
            checkpoint_every=int(os.environ.get("CHECKPOINT_EVERY", "200")),
            total_steps=int(os.environ.get("TOTAL_STEPS", "1000000")),
            global_batch=int(os.environ.get("GLOBAL_BATCH", "840")),
            step_sleep=float(os.environ.get("STEP_SLEEP", "0.05")),
            control_port=int(os.environ.get("CONTROL_PORT", "8080")),
            seed=int(os.environ.get("SEED", "20261019")),
        )


class Control:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.requested = 0
        self.stopping = False
        self.lock = threading.Lock()

    def request(self, request_id: int) -> None:
        with self.lock:
            self.requested = max(self.requested, request_id)

    def status(self) -> dict:
        path = self.config.checkpoint_dir / "status.json"
        try:
            return json.loads(path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return {"checkpoint_id": 0, "step": 0}


def serve(control: Control) -> None:
    class Handler(BaseHTTPRequestHandler):
        def _reply(self, code: int, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:
            if self.path == "/healthz":
                self._reply(200, {"status": "ok"})
            elif self.path == "/checkpoint":
                self._reply(200, control.status())
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self) -> None:
            if not self.path.startswith("/checkpoint?id="):
                self._reply(404, {"error": "not found"})
                return
            try:
                request_id = int(self.path.split("=", 1)[1])
            except ValueError:
                self._reply(400, {"error": "id must be an integer"})
                return
            if request_id <= 0:
                self._reply(400, {"error": "id must be positive"})
                return
            control.request(request_id)
            self._reply(202, {"accepted": request_id, **control.status()})

        def log_message(self, *_args) -> None:
            pass

    server = ThreadingHTTPServer(("0.0.0.0", control.config.control_port), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()


def batch(config: Config, step: int, rank: int, world: int) -> tuple[torch.Tensor, torch.Tensor]:
    generator = torch.Generator().manual_seed(config.seed + step)
    inputs = torch.randn(config.global_batch, 32, generator=generator)
    targets = (inputs[:, :8].sum(dim=1, keepdim=True) > 0).float()
    shard = config.global_batch // world
    return inputs[rank * shard:(rank + 1) * shard], targets[rank * shard:(rank + 1) * shard]


def atomic_write(path: Path, writer) -> None:
    temporary = path.with_name(path.name + ".tmp")
    writer(temporary)
    os.replace(temporary, path)


def append_event(config: Config, event: dict) -> None:
    with open(config.checkpoint_dir / "events.jsonl", "a") as handle:
        handle.write(json.dumps({"time": time.time(), **event}) + "\n")


def save(config: Config, step: int, model: nn.Module, optimizer: torch.optim.Optimizer, world: int, checkpoint_id: int, reason: str) -> None:
    state = {"step": step, "model": model.state_dict(), "optimizer": optimizer.state_dict(), "world_size": world}
    atomic_write(config.checkpoint_dir / "checkpoint.pt", lambda path: torch.save(state, path))
    status = {"checkpoint_id": checkpoint_id, "step": step, "world_size": world, "saved_at": time.time(), "reason": reason}
    atomic_write(config.checkpoint_dir / "status.json", lambda path: path.write_text(json.dumps(status)))
    append_event(config, {"event": "checkpoint", "step": step, "checkpoint_id": checkpoint_id, "reason": reason, "world_size": world})


def main() -> None:
    config = Config.from_env()
    config.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    control = Control(config)
    serve(control)
    signal.signal(signal.SIGTERM, lambda *_: setattr(control, "stopping", True))

    dist.init_process_group("gloo", timeout=timedelta(seconds=int(os.environ.get("COLLECTIVE_TIMEOUT", "60"))))
    rank, world = dist.get_rank(), dist.get_world_size()
    if config.global_batch % world:
        raise SystemExit(f"global batch {config.global_batch} is not divisible by world size {world}")
    torch.manual_seed(config.seed)
    model = nn.Sequential(nn.Linear(32, 128), nn.ReLU(), nn.Linear(128, 1))
    optimizer = torch.optim.SGD(model.parameters(), lr=0.05, momentum=0.9)
    step = 0
    restored = config.checkpoint_dir / "checkpoint.pt"
    if restored.exists():
        state = torch.load(restored, weights_only=False)
        model.load_state_dict(state["model"])
        optimizer.load_state_dict(state["optimizer"])
        step = state["step"]
    ddp = DistributedDataParallel(model)
    loss_fn = nn.BCEWithLogitsLoss()
    agreed = torch.tensor([control.status().get("checkpoint_id", 0)], dtype=torch.int64)
    dist.broadcast(agreed, src=0)
    checkpoint_id = int(agreed[0])
    if rank == 0:
        append_event(config, {"event": "resume", "step": step, "world_size": world})

    while step < config.total_steps:
        inputs, targets = batch(config, step, rank, world)
        optimizer.zero_grad()
        loss = loss_fn(ddp(inputs), targets)
        loss.backward()
        optimizer.step()
        step += 1

        with control.lock:
            requested = control.requested
        leaving = control.stopping
        flags = torch.tensor([requested, int(leaving)], dtype=torch.int64)
        dist.all_reduce(flags, op=dist.ReduceOp.MAX)
        requested, peer_leaving = int(flags[0]), bool(flags[1])
        periodic = step % config.checkpoint_every == 0
        if requested > checkpoint_id or peer_leaving or periodic:
            reason = "requested" if requested > checkpoint_id else "member_leaving" if peer_leaving else "periodic"
            checkpoint_id = max(checkpoint_id, requested)
            if rank == 0:
                save(config, step, model, optimizer, world, checkpoint_id, reason)
            dist.barrier()
        if rank == 0 and step % 50 == 0:
            append_event(config, {"event": "progress", "step": step, "loss": round(loss.item(), 5), "world_size": world})
        if leaving:
            break
        time.sleep(config.step_sleep)

    dist.destroy_process_group()


if __name__ == "__main__":
    main()
