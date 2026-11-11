"""접속 정보, 이름, 시간 값. 이름은 컨트롤러와 같은 common/keys.py를 쓴다 (한 곳에서만 바꾸게)."""
import os
import pathlib
import sys

from kubernetes import client, config

ROOT = pathlib.Path(__file__).resolve().parents[2]          # scheduling/
CONTROLLERS_DIR = ROOT
sys.path.insert(0, str(ROOT / "common"))
import keys as K  # noqa: E402

MGR_CONTEXT = os.getenv("MGR_CONTEXT", "kind-manager")
WORKERS = os.getenv("WORKERS", "worker1,worker2,worker3").split(",")
WORKER_CONTEXTS = {w: f"kind-{w}" for w in WORKERS}
NS = K.NAMESPACE                       # training
STORE_NS = "scheduling-local-store"    # kind에서 S3·DynamoDB 대신 쓰는 ConfigMap 자리
GPU_SELECTOR = K.GPU_NODE_SELECTOR

TRAINING_GPUS_PER_WORKER = 4           # KWOK 학습 노드 4 (+ 예비 1은 학습 잡이 안 씀)
TRAINING_GPUS = TRAINING_GPUS_PER_WORKER * len(WORKERS)
HEAVY_MIN_GPUS = int(os.getenv("HEAVY_MIN_GPUS", "3"))   # 잡 투입기 규칙: 한 클러스터 학습 노드의 절반 초과 -> Heavy
MAX_JOB_GPUS = int(os.getenv("MAX_JOB_GPUS", "4"))       # 한 클러스터 학습 노드 수를 넘으면 거부

SLACK = 30          # 컨트롤러 루프 + Kueue 반영 지연 여유
REQUEST_TIMEOUT = 10


class Kube:
    """관리 클러스터와 워커 클러스터 API 클라이언트."""

    def __init__(self):
        self.mgr = config.new_client_from_config(context=MGR_CONTEXT)
        self.workers = {w: config.new_client_from_config(context=c) for w, c in WORKER_CONTEXTS.items()}
        self.co = client.CustomObjectsApi(self.mgr)
        self.core = client.CoreV1Api(self.mgr)

    def wcore(self, worker):
        return client.CoreV1Api(self.workers[worker])
