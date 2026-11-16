"""ADR-38: moveToSpare keeps its action and falls back from a full source to another worker."""
import pytest

from harness import recovery as R
from harness.env import K, WORKERS
from harness.jobs import submit
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc('ADR-38 원래 EKS 부족 → 다른 EKS')]


@pytest.mark.parametrize('action', ['restartInPlace', 'moveToSpare'])
def test_cross_cluster(kube, check, info, action):
    """A의 가용 GPU 3장·B 4장 이상: moveToSpare → B에 4개 Pod, 원래 실행 종료·펜싱 없음."""
    src, dst = WORKERS[:2]
    spare = R.spare_nodes(kube, src)
    precondition(len(spare) == 1, f'{src} 예비 노드 1대 필요: {spare}')
    R.set_cordon(kube, src, spare[0], True)
    # 3개 워커 프로필에서도 반드시 B로 가는 상황을 만든다.
    for i, worker in enumerate(WORKERS[2:]):
        jid = f'block-cross-{i}'
        submit(kube, jid, 4, 'queue-heavy', '30m', {K.ANN_TARGET: worker})
        precondition(R.wait_placed(kube, R.jsname(jid), 120) == worker, f'{worker} 채우기 실패')
    submit(kube, 'cross', 4, 'queue-heavy', '30m', {K.ANN_TARGET: src})
    old = R.jsname('cross')
    precondition(R.wait_placed(kube, old, 120) == src, f'{old}가 {src}에 배치되지 않음')
    precondition(R.wait_running(kube, src, old, 4) is not None, '원래 실행의 Pod 4개 필요')
    bad = R.first_node(kube, src, old)
    if action == 'restartInPlace':
        # restart는 노드를 격리하지 않으므로, 외부 용량 감소로 A가 부족한 상황을 만든다.
        R.set_cordon(kube, src, bad, True)

    with R.DoubleRunWatch(kube, 'cross') as watch:
        new = R.relocate(kube, 'rr-cross', 'evt-cross', action, 'cross',
                         nodes=[bad] if action == 'moveToSpare' else None)
        status = R.wait_rr(kube, 'rr-cross', R.FINAL, 180)
        R.wait_running(kube, dst, new, 4, 90)

    info['RR'] = R.stage_summary(status)
    info['이동'] = f'{src} → {R.placed_on(kube, new)}'
    check.eq('RR Done', status.get('phase'), 'Done')
    check.eq('원래 클러스터', status.get('sourceCluster'), src)
    check.eq('실제 배치 클러스터 기록', status.get('targetCluster'), dst)
    check.eq('B에 새 실행 배치', R.placed_on(kube, new), dst)
    check.eq('B에서 새 세대 Pod 4개 Running', len(R.running_nodes(kube, dst, new)), 4)
    check.eq('A에 새 세대 Pod 없음', len(R.pods(kube, src, new)), 0)
    check.eq('A의 옛 Pod 종료', len(R.pods(kube, src, old)), 0)
    check.eq('노드 격리 여부', R.isolated(R.node(kube, src, bad)), (True, action == 'moveToSpare'))
    check.eq('action 유지', R.rr(kube, 'rr-cross')['spec']['action'], action)
    check.true('A 클러스터 펜싱 없음', not R.fenced(kube, src))
    check.true('두 세대 동시 실행 없음', not watch.overlaps, '; '.join(watch.overlaps))
    R.check_resubmission(check, kube, 'cross', new, 'rr-cross', 'evt-cross', preferred=src)
