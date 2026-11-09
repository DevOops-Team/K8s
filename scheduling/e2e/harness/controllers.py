"""컨트롤러를 케이스가 정한 설정(어떤 컨트롤러, 어떤 환경 값)대로 띄운다.

설정이 직전 케이스와 같으면 그대로 두고, 다르면 내렸다 다시 띄운다. 로그는 설정마다 파일 하나.
"""
import logging
import os
import signal
import subprocess
import time
from datetime import datetime

from .env import CONTROLLERS_DIR, MGR_CONTEXT, WORKER_CONTEXTS
from .verdict import Invalid

log = logging.getLogger("e2e")
ALL = ("dispatcher", "relocator")
PY = CONTROLLERS_DIR / ".venv" / "bin" / "python"


class Controllers:
    def __init__(self, logdir):
        self.logdir = logdir
        self.procs = {}
        self.key = None
        self.logfile = None
        self.started_at = None
        self.seq = 0

    def use(self, names=ALL, **env):
        """이 설정으로 돌게 한다. 같은 설정으로 이미 살아 있으면 그대로."""
        names = tuple(sorted(names))
        env = {k: str(v) for k, v in env.items()}
        key = (names, tuple(sorted(env.items())))
        if key == self.key and not self.dead():
            return self
        self.stop()
        if not PY.exists():
            raise Invalid(f"{PY} 없음: controllers/run-local.sh를 한 번 돌려 venv를 만들 것")
        self.seq += 1
        self.logfile = self.logdir / f"controllers-{self.seq:02d}-{'+'.join(names) or 'none'}.log"
        full = {**os.environ, **env, "KUBE_CONTEXT": MGR_CONTEXT, "PYTHONUNBUFFERED": "1",
                "WORKER_CONTEXTS": ",".join(f"{w}={c}" for w, c in WORKER_CONTEXTS.items())}
        out = open(self.logfile, "a", buffering=1)
        out.write(f"# {datetime.now():%H:%M:%S} 설정: {names} {env}\n")
        for n in names:
            self.procs[n] = subprocess.Popen([str(PY), f"{n}.py"], cwd=CONTROLLERS_DIR, env=full,
                                             stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        self.key, self.started_at = key, time.time()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.dead():
                raise Invalid(f"컨트롤러가 바로 죽음: {self.dead()} (로그 {self.logfile})")
            started = self.logfile.read_text().count("INFO 시작:")
            if started >= len(names):
                log.info("  컨트롤러 %s 시작 %s (로그 %s)", names or "없음", env, self.logfile.name)
                return self
            time.sleep(1)
        raise Invalid(f"컨트롤러가 30s 안에 시작 안 함 (로그 {self.logfile})")

    def none(self):
        """컨트롤러 없이 (Kueue 설정만으로) 도는지 볼 때."""
        return self.use(())

    @property
    def names(self):
        return self.key[0] if self.key else ()

    def dead(self):
        return [n for n, p in self.procs.items() if p.poll() is not None]

    def stop(self):
        for p in self.procs.values():
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGTERM)
        for p in self.procs.values():
            try:
                p.wait(10)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)
        self.procs, self.key = {}, None
