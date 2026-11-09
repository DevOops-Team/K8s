"""디스패처의 판단 로직. 쿠버네티스 API를 부르지 않는 순수 함수만 둔다 (단위 테스트 대상).

노드당 GPU 1장, Pod당 GPU 1장 구성(학습 노드 4 + 예비 1, 잡 최대 4장)이라 노드 단위 배치는 따로 계산하지 않는다.
"들어가는 노드가 있나" = "스케줄 가능한 노드의 남은 GPU 수가 잡 GPU 수 이상인가".
"""
from dataclasses import dataclass


@dataclass
class Cluster:
    name: str
    active: bool = True          # MultiKueueCluster Active=True
    fenced: bool = False         # 펜싱 라벨
    quota_free: int = 0          # 워커 학습 CQ(빌려주는 쿼터): nominalQuota - 예약량
    recovery_free: int = 0       # 워커 복구 CQ(lendingLimit 0): 복구 잡만 쓸 수 있는 남은 쿼터
    node_free: int = 0           # 스케줄 가능한(cordon 아님, Ready) 일반 GPU 노드의 남은 GPU
    spare_free: int = 0          # 자리 표시 Pod가 잡고 있는 예비 GPU (복구 잡만 쓸 수 있음)

    def nodes_for(self, recovery: bool) -> int:
        return self.node_free + (self.spare_free if recovery else 0)

    def quota_for(self, recovery: bool) -> int:
        # 학습 몫은 복구 잡에 빌려주지만, 복구 몫은 학습 잡에 안 빌려줌
        return self.quota_free + (self.recovery_free if recovery else 0)

    def place(self, gpus: int, recovery: bool):
        """잡 하나가 이 클러스터에 들어갔다고 기록 (쿼터 + 노드). 같은 루프 안 이중 배정 방지.
        복구 잡은 복구 몫·예비 노드부터 쓰고 모자라면 학습 몫·일반 노드에서. 학습 잡은 예비를 건드리지 않는다."""
        if recovery:
            own = min(gpus, max(self.recovery_free, 0))
            self.recovery_free -= own
            self.quota_free -= gpus - own
            spare = min(gpus, max(self.spare_free, 0))
            self.spare_free -= spare
            self.node_free -= gpus - spare
        else:
            self.quota_free -= gpus
            self.node_free -= gpus


def eligible(c: Cluster, target=None, exclude=frozenset()) -> bool:
    if not c.active or c.fenced or c.name in exclude:
        return False
    return target is None or c.name == target


def choose(gpus: int, clusters: list, *, recovery=False, target=None, exclude=frozenset()):
    """빈 패킹: 들어가는 클러스터 중 넣고 나서 남는 GPU가 가장 적은 곳 (같으면 이름순). 없으면 None."""
    best = None
    for c in clusters:
        if not eligible(c, target, exclude):
            continue
        quota = c.quota_for(recovery)
        if quota < gpus or c.nodes_for(recovery) < gpus:
            continue
        key = (quota - gpus, c.name)
        if best is None or key < best[0]:
            best = (key, c)
    return best[1].name if best else None
