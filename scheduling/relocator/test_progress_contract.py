"""python -m unittest -v test_progress_contract.py
relocator가 status.recoveryProgress에 쓰는 결과를 복구 컨트롤러의 실제 검사(relocation_status.py)에 그대로 넣어 본다.
두 컨트롤러 계약이 어긋나면 여기서 깨진다. recovery-controller 코드가 없으면 건너뜀."""
import copy
import sys
import unittest
from pathlib import Path
from unittest import mock

import relocator
from test_relocator_flow import ACK, FakeCO, ctx, on_worker, elapsed

RC = Path(__file__).resolve().parents[2] / "recovery-controller"
try:
    sys.path.append(str(RC))
    from integrations.relocation_status import RelocationStatusInput   # noqa: E402
    from integrations.dynamodb import RecoveryBlocked                  # noqa: E402
except Exception as e:  # noqa: BLE001
    RelocationStatusInput = None
    WHY = f"recovery-controller import 실패: {e}"
else:
    WHY = ""


class Recorder:
    def __init__(self):
        self.calls = []

    def record_relocation_phase(self, event, phase, extra, stop_evidence=None):
        self.calls.append((phase, dict(extra), stop_evidence))


EVENT = {"job_id": "abc", "generation": 1, "event_id": "r1", "cluster": "worker1"}


def as_request(x):
    req = copy.deepcopy(x.rr)
    req["metadata"]["creationTimestamp"] = relocator.kube.now_rfc3339()
    req["status"] = copy.deepcopy(x.co.status)
    return req


@unittest.skipIf(RelocationStatusInput is None, WHY)
class ControllerAcceptsRelocatorStatus(unittest.TestCase):
    def run_stop(self, x):
        x.co.delete_namespaced_custom_object = mock.Mock()
        with elapsed(30), on_worker(None), mock.patch.object(
                relocator, "gone_on", return_value=(True, "worker1에서도 사라짐")):
            relocator.stop_old(x)
        x.st = dict(x.co.status)
        x.rr["status"] = dict(x.co.status)

    def projector(self):
        rec = Recorder()
        return rec, RelocationStatusInput(rec, "training")

    def test_old_run_stopped_then_new_run_submitted(self):
        co = FakeCO()
        x = ctx(co, sourceCluster="worker1", isolatedAt="t")
        self.run_stop(x)
        rec, project = self.projector()
        self.assertTrue(project(EVENT, as_request(x)))
        self.assertEqual([c[0] for c in rec.calls], ["OLD_RUN_STOPPED"])
        stop_evidence = rec.calls[0][1]["old_run_stop_evidence"]

        x.rr["metadata"]["annotations"].update(ACK)
        relocator.resubmit(x)
        rec, project = self.projector()
        self.assertTrue(project(EVENT, as_request(x)))
        self.assertEqual([c[0] for c in rec.calls], ["OLD_RUN_STOPPED", "NEW_RUN_SUBMITTED"])
        self.assertEqual(rec.calls[1][1], {"new_jobset_name": "job-abc-g0002", "new_jobset_uid": "uid-new"})
        # 두 번째 기록도 첫 번째와 똑같은 종료 증거에 묶인다 (증거를 바꾸지 않음)
        self.assertEqual(rec.calls[1][2], stop_evidence)

    def test_ec2_evidence_is_accepted(self):
        co = FakeCO()
        x = ctx(co, sourceCluster="worker1", isolatedAt="t")
        co.delete_namespaced_custom_object = mock.Mock()
        with elapsed(30), on_worker(None), \
                mock.patch.object(relocator, "gone_on", return_value=(False, relocator.Unreachable("끊김"))), \
                mock.patch.object(relocator, "ec2_stop_evidence", return_value=(True, "인스턴스 전부 꺼짐")):
            relocator.stop_old(x)
        rec, project = self.projector()
        self.assertTrue(project(EVENT, as_request(x)))

    def test_manual_confirmation_is_not_accepted_by_controller(self):
        # 사람 확인(devoops/old-execution-stopped)은 relocator가 status에 kind=manual로 쓰지만
        # 지금 복구 컨트롤러 계약은 받지 않는다 (별도 인증 경로 필요) -> 운영에서는 HOLD로 남는다
        co = FakeCO()
        x = ctx(co, annotations={relocator.K.ANN_STOP_CONFIRMED: "true"}, sourceCluster="worker1", isolatedAt="t")
        co.delete_namespaced_custom_object = mock.Mock()
        with elapsed(1000), on_worker(None), \
                mock.patch.object(relocator, "old_gone_on_worker", return_value=(False, "끊김")):
            relocator.stop_old(x)
        _, project = self.projector()
        with self.assertRaises(RecoveryBlocked):
            project(EVENT, as_request(x))


if __name__ == "__main__":
    unittest.main()
