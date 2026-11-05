"""배분 조정 컨트롤러(allocator.py)의 판단 로직. 쿠버네티스 API를 부르지 않는 순수 함수만 둔다 (단위 테스트 대상)."""


def aged(wait_seconds: float, gpus: int, threshold: int) -> bool:
    """대기 시간 x GPU 수가 기준을 넘었나. GPU를 많이 요구할수록 빨리 앞에 선다."""
    return wait_seconds * max(gpus, 1) >= threshold


def want_tight(tight: bool, heavy_wait, idle_for, tighten_after: int, release_after: int) -> bool:
    """Light 빌려 쓰기를 조여야 하나.
    heavy_wait: 쿼터 못 받은 Heavy 잡 중 가장 오래 기다린 시간(초). 없으면 None
    idle_for:   Heavy 대기가 0인 상태가 이어진 시간(초). 대기가 있으면 None
    """
    if heavy_wait is not None:
        # 조이는 건 오래 기다린 잡이 있을 때만. 이미 조였으면 막 들어온 잡이라도 대기가 있으니 유지
        return tight or heavy_wait >= tighten_after
    if not tight:
        return False
    return idle_for is None or idle_for < release_after
