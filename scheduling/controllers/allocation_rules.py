"""배분 조정 컨트롤러(allocator.py)의 판단 로직. 쿠버네티스 API를 부르지 않는 순수 함수만 둔다 (단위 테스트 대상)."""


def aged(wait_seconds: float, gpus: int, threshold: int) -> bool:
    """대기 시간 x GPU 수가 기준을 넘었나. GPU를 많이 요구할수록 빨리 앞에 선다."""
    return wait_seconds * max(gpus, 1) >= threshold
