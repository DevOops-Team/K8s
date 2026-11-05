"""python -m unittest -v test_relocator.py  (쿠버네티스 없이 돈다. 재제출 잡이 어디로 갈지만 본다)"""
import ast
import pathlib
import unittest

# relocator.py는 import하면 로깅·클라이언트 설정을 하므로 순수 함수만 떼어 읽는다
_src = pathlib.Path(__file__).with_name("relocator.py").read_text(encoding="utf-8")
_tree = ast.parse(_src)
_ns = {}
for node in _tree.body:
    if (isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "LEAVE_SOURCE" for t in node.targets)) or \
       (isinstance(node, ast.FunctionDef) and node.name == "placement_hints"):
        exec(compile(ast.Module([node], []), "relocator.py", "exec"), _ns)
hints = _ns["placement_hints"]


class PlacementHints(unittest.TestCase):
    def test_restart_in_place_pins_source(self):
        self.assertEqual(hints("restartInPlace", "worker2", None, None), ("worker2", []))

    def test_move_to_spare_pins_source(self):
        self.assertEqual(hints("moveToSpare", "worker2", "worker3", None), ("worker2", ["worker3"]))

    def test_pinned_source_never_excluded(self):
        self.assertEqual(hints("moveToSpare", "worker2", "worker2", None), ("worker2", []))

    def test_move_to_other_cluster_leaves_source(self):   # 예비 소진·SDC: 펜싱 없이 원래 클러스터만 뺌
        self.assertEqual(hints("moveToOtherCluster", "worker2", None, None), (None, ["worker2"]))

    def test_evacuate_leaves_source_and_keeps_old_excludes(self):
        self.assertEqual(hints("evacuateCluster", "worker2", "worker1", ["worker3"]),
                         (None, ["worker1", "worker2", "worker3"]))

    def test_not_dispatched_yet(self):
        self.assertEqual(hints("moveToOtherCluster", "", None, None), (None, []))
        self.assertEqual(hints("restartInPlace", "", None, None), (None, []))


if __name__ == "__main__":
    unittest.main()
