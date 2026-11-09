"""판정 4종.

PASS          기대대로 됨
FAIL          상황은 제대로 만들어졌는데 기대와 다름 = 스케줄링 문제
INCONCLUSIVE  검사하려던 상황 자체가 안 만들어짐 (전제 조건 실패) = 케이스 설계·타이밍 문제
INVALID       인프라 문제로 결과를 믿을 수 없음 (웹훅 timeout, 컨트롤러 종료, API 서버 응답 없음)
"""


class Inconclusive(Exception):
    pass


class Invalid(Exception):
    pass


def precondition(cond, msg):
    """검사하려던 상황이 만들어졌는지. 아니면 INCONCLUSIVE로 끝낸다 (PASS로 새지 않게)."""
    if not cond:
        raise Inconclusive(msg)


class Checks:
    """한 케이스 안의 여러 판정 항목. 하나라도 틀리면 케이스는 FAIL, 항목은 전부 기록."""

    def __init__(self):
        self.items = []   # (이름, 통과, 설명)

    def true(self, name, ok, detail=""):
        self.items.append((name, bool(ok), detail))
        return bool(ok)

    def eq(self, name, actual, expected):
        ok = actual == expected
        return self.true(name, ok, "" if ok else f"기대 {expected!r}, 실제 {actual!r}")

    @property
    def failed(self):
        return [i for i in self.items if not i[1]]
