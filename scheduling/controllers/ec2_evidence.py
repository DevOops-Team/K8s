"""EC2로 옛 실행 종료 증거 모으기 — 워커 EKS API에 접속이 안 될 때만 쓴다.

K8s API가 죽어도 EC2 API는 살아 있을 수 있다. 그 클러스터의 GPU 인스턴스가 전부 stopped/terminated면
그 위의 Pod도 돌 수 없으니 "옛 실행 멈춤" 증거로 인정한다 (체크포인트 규약 8절의 독립 종료 증거).

종료 증거 = 그 클러스터의 학습·예비 인스턴스 전부 꺼짐 (Karpenter 태그 기준):
  tag:eks:eks-cluster-name = <EKS 클러스터 이름>   tag:karpenter.sh/nodepool ∈ EC2_NODEPOOLS
  격리 때 기록한 옛 노드만 보지 않는다: 워커가 우리 쪽에서만 끊기고(망 분리) 안에서는 살아 있으면
  JobSet이 Pod를 기록에 없는 다른 노드에 다시 띄울 수 있다.

인정하지 않는 것: 찾은 인스턴스 0개(태그·이름이 틀렸을 수 있음), running·pending·stopping·shutting-down 하나라도.
EC2 호출 실패도 증거 아님.

강제 종료(EC2_EVIDENCE=terminate): 격리 때 기록한 옛 노드만 TerminateInstances. 클러스터 전체는 절대 안 내림
(같은 클러스터의 다른 잡 노드까지 내리게 되므로). 옛 실행을 실제로 끊는 용도이고, 다른 노드가 살아 있으면
종료 증거는 여전히 안 되므로 사람 확인(어노테이션)으로 넘어간다. 대상은 ID로만 조회한 뒤 태그를 하나씩 맞춰 본다.

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
                    tags = {t["Key"]: t["Value"] for t in i.get("Tags") or []}
                    out[i["InstanceId"]] = (i["State"]["Name"], tags)
        return out

    def check(self, src):
        """그 클러스터 학습·예비 인스턴스가 전부 꺼졌는지. (멈춤확인, 이유, {인스턴스ID: 상태})"""
        scope = f"{self.clusters.get(src, src)} 학습·예비 인스턴스 전부"
        try:
            states = {i: st for i, (st, _) in self._describe(self._filters(src)).items()}
        except Exception as e:  # noqa: BLE001  권한·끊김
            return False, f"EC2 조회 실패 ({type(e).__name__}: {e})", {}
        if not states:
            return False, f"EC2: {scope} 0개 - 태그 확인 필요, 증거 아님", {}
        alive = {i: s for i, s in states.items() if s not in DEAD}
        if alive:
            return False, f"EC2: {scope} 중 {len(alive)}개 아직 안 꺼짐 {alive}", states
        return True, f"EC2: {scope} 모두 꺼짐 {states}", states

    def terminate(self, src, old_ids):
        """옛 노드 인스턴스만 내린다. ID로만 조회하고 태그를 하나씩 맞춰 본 것만 대상
        (다른 클러스터·NodePool 인스턴스 보호, 태그가 하나라도 안 맞으면 아무것도 안 내림)"""
        if not (self.can_terminate and old_ids):
            return []
        found = self._describe([{"Name": "instance-id", "Values": list(old_ids)}])
        want = self.clusters.get(src, src)
        wrong = [i for i, (_, t) in found.items()
                 if t.get(TAG_CLUSTER) != want or t.get(TAG_NODEPOOL) not in self.nodepools]
        if wrong:
            raise PermissionError(f"EC2: {wrong}가 {want} 학습·예비 NodePool 태그가 아님 -> 강제 종료 안 함")
        targets = [i for i, (s, _) in found.items() if s not in DEAD and s != "shutting-down"]
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
