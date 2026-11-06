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

    def place(self, pods: list, recovery: bool):
        """잡 하나가 이 클러스터에 들어갔다고 기록 (쿼터 + 노드). 같은 루프 안 이중 배정 방지.
        노드는 일반 빈자리와 예비 자리를 따로 셈: 복구 잡은 둘 다 쓸 수 있지만(같은 크기면 예비부터),
        쓰고 남은 예비 자리가 일반 빈자리로 섞이면 학습 잡이 예비 노드를 빈자리로 보게 된다."""
        self.use(sum(pods), recovery)
        slots = [[f, False] for f in self.node_free]
        if recovery:
            slots += [[f, True] for f in self.spare_free]
        slots.sort(key=lambda s: (s[0], s[1]), reverse=True)
        for need in sorted(pods, reverse=True):
            for s in slots:
                if s[0] >= need:
                    s[0] -= need
                    break
        self.node_free = [f for f, spare in slots if not spare and f > 0]
        if recovery:
            self.spare_free = [f for f, spare in slots if spare and f > 0]


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


def reserve_for_starving(pods: list, clusters: list, *, recovery=False, target=None,
                         exclude=frozenset(), prev=None):
    """오래 기다린 큰 잡을 위해 비워 둘 클러스터: 그 잡이 갈 수 있는 곳(target·exclude·펜싱) 중 가장 많이 비어 있는 곳.
    그 클러스터에는 다른 잡을 새로 보내지 않아서, 돌던 잡이 끝나면 큰 잡이 들어간다.
    prev(지난 루프에 비워 두던 곳)가 아직 갈 수 있는 곳이면 그대로 둔다 (루프마다 바뀌면 어느 곳도 안 비워짐)."""
    by_name = {c.name: c for c in clusters}
    if prev in by_name and eligible(by_name[prev], target, exclude):
        return prev
    best = None
    for c in clusters:
        if not eligible(c, target, exclude):
            continue
        key = (-min(c.quota_for(recovery), sum(c.nodes_for(recovery))), c.name)
        if best is None or key < best[0]:
            best = (key, c)
    return best[1].name if best else None
