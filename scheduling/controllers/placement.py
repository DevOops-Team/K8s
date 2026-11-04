"""디스패처의 판단 로직. 쿠버네티스 API를 부르지 않는 순수 함수만 둔다 (단위 테스트 대상)."""
from dataclasses import dataclass, field


@dataclass
class Cluster:
    name: str
    active: bool = True          # MultiKueueCluster Active=True
    fenced: bool = False         # 펜싱 라벨
    quota_free: int = 0          # 워커 학습 CQ(빌려주는 쿼터): nominalQuota - 예약량
    recovery_free: int = 0       # 워커 복구 CQ(lendingLimit 0): 복구 잡만 쓸 수 있는 남은 쿼터
    node_free: list = field(default_factory=list)        # 스케줄 가능한 노드별 남은 GPU
    spare_free: list = field(default_factory=list)       # 자리 표시 Pod가 잡고 있는 GPU (복구 잡만 쓸 수 있음)

    def nodes_for(self, recovery: bool) -> list:
        return self.node_free + (self.spare_free if recovery else [])

    def quota_for(self, recovery: bool) -> int:
        # 학습 몫은 복구 잡에 빌려주지만, 복구 몫은 학습 잡에 안 빌려줌
        return self.quota_free + (self.recovery_free if recovery else 0)

    def use(self, gpus: int, recovery: bool):
        """자리를 차지했다고 기록. 복구 잡은 복구 몫부터 쓰고 모자라면 학습 몫에서 빌림"""
        if recovery:
            own = min(gpus, max(self.recovery_free, 0))
            self.recovery_free -= own
            gpus -= own
        self.quota_free -= gpus


def fits(pods: list, nodes: list) -> bool:
    """Pod별 GPU 요청(pods)이 노드별 남은 GPU(nodes)에 전부 들어가는지. 큰 것부터 first-fit."""
    free = sorted(nodes, reverse=True)
    for need in sorted(pods, reverse=True):
        for i, f in enumerate(free):
            if f >= need:
                free[i] -= need
                break
        else:
            return False
    return True


def take(pods: list, nodes: list) -> list:
    """fits와 같은 방식으로 자리를 차지한 뒤 남은 노드 목록을 돌려준다 (같은 루프 안 이중 배정 방지)."""
    free = sorted(nodes, reverse=True)
    for need in sorted(pods, reverse=True):
        for i, f in enumerate(free):
            if f >= need:
                free[i] -= need
                break
    return free


def eligible(c: Cluster, target=None, exclude=frozenset(), blocked=frozenset()) -> bool:
    if not c.active or c.fenced or c.name in exclude or c.name in blocked:
        return False
    return target is None or c.name == target


def choose(pods: list, clusters: list, *, recovery=False, target=None,
           exclude=frozenset(), blocked=frozenset()):
    """빈 패킹: 들어가는 클러스터 중 넣고 나서 남는 GPU가 가장 적은 곳. 없으면 None."""
    need = sum(pods)
    best = None
    for c in clusters:
        if not eligible(c, target, exclude, blocked):
            continue
        quota = c.quota_for(recovery)
        if quota < need or not fits(pods, c.nodes_for(recovery)):
            continue
        key = (quota - need, c.name)
        if best is None or key < best[0]:
            best = (key, c)
    return best[1].name if best else None


def reserve_for_starving(pods: list, clusters: list, exclude=frozenset()):
    """오래 기다린 큰 잡을 위해 비워 둘 클러스터: 지금 가장 많이 비어 있는 곳.
    그 클러스터에는 다른 잡을 새로 보내지 않아서, 돌던 잡이 끝나면 큰 잡이 들어간다."""
    best = None
    for c in clusters:
        if not eligible(c, exclude=exclude):
            continue
        key = (-min(c.quota_free, sum(c.node_free)), c.name)
        if best is None or key < best[0]:
            best = (key, c)
    return best[1].name if best else None
