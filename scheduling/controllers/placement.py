"""디스패처의 판단 로직. 쿠버네티스 API를 부르지 않는 순수 함수만 둔다 (단위 테스트 대상).

노드당 GPU 1장, Pod당 GPU 1장 구성(학습 노드 4 + 예비 1, 잡 최대 4장)이라 노드 단위 배치는 따로 계산하지 않는다.
"들어가는 노드가 있나" = "스케줄 가능한 노드의 남은 GPU 수가 잡 GPU 수 이상인가".

찜(reserve): 쿼터는 받았는데 들어갈 워커가 없는 잡에게 워커 하나를 잡아 준다. 그 워커에는 뒤 순서 학습 잡을 안 보낸다.
관리 Kueue는 워커 3개 합계로 쿼터를 주므로, 4장 잡이 쿼터를 받아도 4장이 한 워커에 비어 있다는 보장이 없다.
찜이 없으면 작은 잡이 빈 칸을 계속 메워서 큰 잡이 쿼터만 쥔 채 끝없이 기다린다 (11/10 compare: 쿼터 뒤 142s).
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
    quota_total: int | None = None      # 워커 학습 CQ nominalQuota 합 (None = 모름 -> 들어갈 수 있다고 봄)
    recovery_total: int | None = None   # 워커 복구 CQ nominalQuota 합
    node_total: int | None = None       # 스케줄 가능한 일반 GPU 노드의 GPU 합 (다 비었을 때 학습 잡이 쓸 수 있는 양)

    def nodes_for(self, recovery: bool) -> int:
        return self.node_free + (self.spare_free if recovery else 0)

    def quota_for(self, recovery: bool) -> int:
        # 학습 몫은 복구 잡에 빌려주지만, 복구 몫은 학습 잡에 안 빌려줌
        return self.quota_free + (self.recovery_free if recovery else 0)

    def free_for(self, recovery: bool) -> int:
        return min(self.quota_for(recovery), self.nodes_for(recovery))

    def can_ever_fit(self, gpus: int, recovery: bool) -> bool:
        """다른 잡이 다 빠지면 들어갈 수 있나. cordon으로 노드가 줄어 영영 못 들어가는 워커를 찜하지 않게"""
        if self.quota_total is not None:
            if self.quota_total + ((self.recovery_total or 0) if recovery else 0) < gpus:
                return False
        if self.node_total is not None:
            if self.node_total + (self.spare_free if recovery else 0) < gpus:
                return False
        return True

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


def reserve(gpus: int, clusters: list, *, recovery=False, target=None, exclude=frozenset(), pinned=None):
    """들어갈 워커가 없는 잡에게 찜해 줄 워커. 없으면 None.
    pinned(지난 루프에 찜한 곳)가 아직 쓸 수 있으면 그대로 둔다: 루프마다 옮기면 옛 워커의 빈 칸에 작은 잡이 다시 들어간다.
    새로 고를 땐 지금 빈 GPU가 가장 많은 곳 (같으면 이름순). 잡이 언제 끝날지는 모르므로 가장 빨리 비는 곳이라는 보장은 없다.
    exclude에는 앞 순서 잡이 이미 찜한 워커도 넣어서 부른다 (워커 하나에 찜 하나)."""
    ok = [c for c in clusters if eligible(c, target, exclude) and c.can_ever_fit(gpus, recovery)]
    if pinned and any(c.name == pinned for c in ok):
        return pinned
    if not ok:
        return None
    return min(ok, key=lambda c: (-c.free_for(recovery), c.name)).name
