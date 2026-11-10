"""EC2로 옛 실행 종료 증거 모으기 — 워커 EKS API에 접속이 안 될 때만 쓴다.

K8s API가 죽어도 EC2 API는 살아 있을 수 있다. 옛 잡이 돌던 GPU 인스턴스가 전부 stopped/terminated면
그 위의 Pod도 돌 수 없으니 "옛 실행 멈춤" 증거로 인정한다 (체크포인트 규약 8절의 독립 종료 증거).

찾는 범위 (Karpenter 태그 기준, 학습·예비 NodePool만):
  tag:eks:eks-cluster-name = <EKS 클러스터 이름>   tag:karpenter.sh/nodepool ∈ EC2_NODEPOOLS
  1) 격리 때 기록한 옛 노드의 인스턴스 ID가 있으면 그것만
  2) 없으면 그 클러스터의 학습·예비 인스턴스 전부 (전부 꺼져 있어야 인정. 더 엄격)

인정하지 않는 것: 찾은 인스턴스 0개(태그·이름이 틀렸을 수 있음), running·pending·stopping·shutting-down 하나라도.
EC2 호출 실패도 증거 아님.

강제 종료(EC2_EVIDENCE=terminate): 격리 때 기록한 옛 노드만 TerminateInstances. 클러스터 전체는 절대 안 내림
(같은 클러스터의 다른 잡 노드까지 내리게 되므로). 기록이 없으면 사람 확인 경로로 남긴다.

설정 (기본 off — kind와 IAM 권한 받기 전에는 아무 것도 안 함):
  EC2_EVIDENCE     off | describe | terminate
  EC2_CLUSTERS     {"worker1": "devoops-eks-a", ...}  MultiKueueCluster 이름 -> EKS 클러스터 이름 (없으면 같은 이름)
  EC2_NODEPOOLS    학습·예비 NodePool 이름, 쉼표 구분 (기본 training,spare)
  AWS_REGION       recovery_store와 같은 값
필요 IAM (relocator 역할): ec2:DescribeInstances (Resource *),
  terminate 모드면 ec2:TerminateInstances (조건 aws:ResourceTag/karpenter.sh/nodepool = 학습·예비 NodePool)
"""
import json
import logging
import os

log = logging.getLogger("relocator")

DEAD = {"stopped", "terminated"}
TAG_CLUSTER = "eks:eks-cluster-name"   # Karpenter v1이 붙이는 클러스터 태그
TAG_NODEPOOL = "karpenter.sh/nodepool"


def instance_id(provider_id):
    """노드 spec.providerID 'aws:///us-east-2a/i-0abc' -> 'i-0abc'. 형식이 다르면 None"""
    if not provider_id or not provider_id.startswith("aws://"):
        return None
    last = provider_id.rstrip("/").rsplit("/", 1)[-1]
    return last if last.startswith("i-") else None


class Ec2Evidence:
    def __init__(self, ec2, clusters=None, nodepools=("training", "spare"), terminate=False):
        self.ec2, self.clusters, self.nodepools, self.can_terminate = ec2, clusters or {}, list(nodepools), terminate

    def _filters(self, src):
        return [{"Name": f"tag:{TAG_CLUSTER}", "Values": [self.clusters.get(src, src)]},
                {"Name": f"tag:{TAG_NODEPOOL}", "Values": self.nodepools}]

    def _describe(self, filters):
        out = {}
        for page in self.ec2.get_paginator("describe_instances").paginate(Filters=filters):
            for r in page["Reservations"]:
                for i in r["Instances"]:
                    out[i["InstanceId"]] = i["State"]["Name"]
        return out

    def check(self, src, old_ids=None):
        """(멈춤확인, 이유, {인스턴스ID: 상태})"""
        filters = self._filters(src)
        try:
            if old_ids:
                found = self._describe(filters + [{"Name": "instance-id", "Values": list(old_ids)}])
                # 격리 때 분명히 있던 인스턴스가 이제 안 보임 = 종료 후 목록에서 빠진 것 (terminated는 약 1시간 뒤 사라짐)
                states = {i: found.get(i, "terminated(목록에서 사라짐)") for i in old_ids}
                scope = f"옛 노드 인스턴스 {len(old_ids)}개"
            else:
                states = self._describe(filters)
                scope = f"{self.clusters.get(src, src)} 학습·예비 인스턴스 전부"
                if not states:
                    return False, f"EC2: {scope} 0개 - 태그 확인 필요, 증거 아님", {}
        except Exception as e:  # noqa: BLE001  권한·끊김
            return False, f"EC2 조회 실패 ({type(e).__name__}: {e})", {}
        alive = {i: s for i, s in states.items() if s.split("(")[0] not in DEAD}
        if alive:
            return False, f"EC2: {scope} 중 {len(alive)}개 아직 안 꺼짐 {alive}", states
        return True, f"EC2: {scope} 모두 꺼짐 {states}", states

    def terminate(self, src, old_ids):
        """옛 노드 인스턴스만 내린다. 범위 태그를 다시 걸어 조회한 것만 대상 (다른 클러스터·NodePool 인스턴스 보호)"""
        if not (self.can_terminate and old_ids):
            return []
        found = self._describe(self._filters(src) + [{"Name": "instance-id", "Values": list(old_ids)}])
        targets = [i for i, s in found.items() if s not in DEAD and s != "shutting-down"]
        if targets:
            self.ec2.terminate_instances(InstanceIds=targets)
            log.warning("EC2: %s 옛 노드 인스턴스 강제 종료 %s", src, targets)
        return targets


def from_env():
    mode = os.getenv("EC2_EVIDENCE", "off").strip().lower()
    if mode not in ("describe", "terminate"):
        return None
    import boto3   # kind·단위 테스트에서는 필요 없게 여기서 import
    clusters = json.loads(os.getenv("EC2_CLUSTERS", "{}") or "{}")
    pools = [p.strip() for p in os.getenv("EC2_NODEPOOLS", "training,spare").split(",") if p.strip()]
    ec2 = boto3.client("ec2", region_name=os.getenv("AWS_REGION", "us-east-2"))
    return Ec2Evidence(ec2, clusters, pools, terminate=(mode == "terminate"))
