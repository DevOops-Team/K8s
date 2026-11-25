"""python -m unittest -v test_controller_contract.py  (GPU-276)
복구 컨트롤러가 실제로 만드는 RR(spec 입력 · 확인/취소 주석)을 relocator가 그대로 읽는지 본다.
test_progress_contract.py(relocator → 컨트롤러 status)의 반대 방향. recovery-controller 코드가 없으면 건너뜀."""
import copy
import sys
import unittest
from pathlib import Path
from unittest import mock

import relocator   # common/(keys)를 경로에 넣는다
import keys as K  # noqa: E402
from test_relocator_flow import FakeCO, original

RC = Path(__file__).resolve().parents[2] / "recovery-controller"
try:
    sys.path.append(str(RC))
    from integrations import relocation as controller   # noqa: E402
except Exception as e:  # noqa: BLE001
    controller = None
    WHY = f"recovery-controller import 실패: {e}"
else:
    WHY = ""

EVENT = {"event_id": "r1", "job_id": "abc", "generation": 1, "cluster": "worker1",
         "node": "gpu-node-1", "category": "hw"}
DEADLINE = 1_800_000_000


@unittest.skipIf(controller is None, WHY)
class RelocatorReadsControllerRequest(unittest.TestCase):
    def created(self):
        saved = {}

        def transport(method, path, document=None):
            if method == "POST":
                saved.update(copy.deepcopy(document))
            return saved
        inputs = lambda event: {"toGeneration": 2, "stopDeadline": DEADLINE, "jobSet": original()}  # noqa: E731
        controller.RelocationClient("training", transport, inputs).submit(EVENT, "moveToSpare")
        saved["metadata"].update(uid="u1", creationTimestamp="2026-11-06T00:00:00Z", resourceVersion="1")
        return saved

    def ctx(self, rr):
        x = relocator.Ctx(None, None, rr)
        x.co = FakeCO()
        return x

    def test_spec_inputs(self):
        x = self.ctx(self.created())
        self.assertEqual((x.job_id(), x.old_gen(), x.to_gen()), ("abc", 1, 2))
        self.assertEqual(x.original()["kind"], "JobSet")
        with mock.patch.object(relocator.time, "time", return_value=DEADLINE - 1):
            self.assertFalse(relocator.stop_held(x))
        with mock.patch.object(relocator.time, "time", return_value=DEADLINE):
            self.assertTrue(relocator.stop_held(x))

    def test_annotation_names_match(self):
        self.assertEqual(controller.STOP_RECORDED_ANNOTATION, K.ANN_STOP_RECORDED)
        self.assertEqual(controller.CANCEL_ANNOTATION, K.ANN_CANCEL)

    def test_acknowledgement_and_cancel_reach_relocator(self):
        rr = self.created()
        rr["status"] = {"phase": "Stopping", "recoveryProgress": {"phase": "OLD_RUN_STOPPED"}}

        def transport(method, path, document=None):
            if method == "PATCH":
                rr["metadata"].setdefault("annotations", {}).update(document["metadata"]["annotations"])
            return rr
        client = controller.RelocationClient("training", transport, status_input=lambda e, r: True)
        client.refresh(EVENT, rr["metadata"]["name"])
        self.assertEqual(self.ctx(rr).ann(K.ANN_STOP_RECORDED), "true")
        rr["status"]["phase"] = "Done"
        client.cancel(EVENT, rr["metadata"]["name"], "recovery authority changed")
        self.assertEqual(self.ctx(rr).ann(K.ANN_CANCEL), "recovery authority changed")
        self.assertTrue(relocator.wanted(rr))   # Done 요청도 취소가 붙으면 다시 본다


if __name__ == "__main__":
    unittest.main()
