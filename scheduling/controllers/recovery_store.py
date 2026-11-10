"""복구 원본(jobset.json)과 펜싱 기록 읽기·쓰기 (체크포인트 규약 4·5·8절, IAM 답변 11/05).

RECOVERY_STORE=aws       우리 망. S3 GetObject code/{job_id}/jobset.json, DynamoDB GetItem·UpdateItem(테이블 1개)
RECOVERY_STORE=configmap kind. 관리 클러스터 전용 네임스페이스 scheduling-local-store의 ConfigMap으로 흉내
                         (platform의 다른 ConfigMap에 권한이 닿지 않게 따로 둠)
                         recovery-code-<job_id>    data.jobset.json  (= S3 code/{job_id}/jobset.json)
                         recovery-fencing-<job_id> data.<필드>       (= DynamoDB 항목, 숫자도 문자열로)
                         e2e 하네스(harness/jobs.py·recovery.py)가 백엔드·복구 컨트롤러 몫을 대신 쓴다.

재배치 실행기가 쓰는 필드는 recovery_phase(OLD_RUN_STOPPED, NEW_RUN_SUBMITTED) · new_jobset_name · new_jobset_uid ·
recovery_updated_at 뿐이고, 전부 조건부 갱신이다:
    recovery_id = 이번 요청 AND recovery_to_gen = 새 세대 AND recovery_phase IN (넘어올 수 있는 단계)
"""
import json
import os
from datetime import datetime, timezone

from kubernetes import client

NUMERIC = {"generation", "approved_gen", "approved_step", "prev_approved_gen", "prev_approved_step",
           "recovery_from_gen", "recovery_to_gen", "retry_count", "ttl"}


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Conflict(Exception):
    """동시에 다른 주체가 바꿈 -> 다음 루프에서 다시 읽고 판단"""


class AwsStore:
    def __init__(self):
        import boto3   # kind·단위 테스트에서는 필요 없게 여기서 import
        from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
        region = os.getenv("AWS_REGION", "us-east-2")
        self.bucket = os.environ["CKPT_BUCKET"]
        self.table = os.environ["FENCING_TABLE"]
        self.s3 = boto3.client("s3", region_name=region)
        self.ddb = boto3.client("dynamodb", region_name=region)
        self._de, self._se = TypeDeserializer(), TypeSerializer()

    def get_jobset(self, job_id):
        body = self.s3.get_object(Bucket=self.bucket, Key=f"code/{job_id}/jobset.json")["Body"].read()
        return json.loads(body)

    def get_fencing(self, job_id):
        item = self.ddb.get_item(TableName=self.table, Key={"job_id": {"S": job_id}}, ConsistentRead=True).get("Item")
        if not item:
            return None
        rec = {k: self._de.deserialize(v) for k, v in item.items()}
        return {k: (int(v) if k in NUMERIC and v is not None else v) for k, v in rec.items()}

    def set_phase(self, job_id, recovery_id, to_gen, allowed_from, new_phase, extra=None):
        values = {"phase": new_phase, "now": _now(), "rid": recovery_id, "gen": to_gen, **(extra or {})}
        sets = ["recovery_phase = :phase", "recovery_updated_at = :now"] + [f"{k} = :{k}" for k in (extra or {})]
        froms = [f":from{i}" for i in range(len(allowed_from))]
        values.update({f"from{i}": p for i, p in enumerate(allowed_from)})
        try:
            self.ddb.update_item(
                TableName=self.table, Key={"job_id": {"S": job_id}},
                UpdateExpression="SET " + ", ".join(sets),
                ConditionExpression=f"recovery_id = :rid AND recovery_to_gen = :gen AND recovery_phase IN ({', '.join(froms)})",
                ExpressionAttributeValues={f":{k}": self._se.serialize(v) for k, v in values.items()})
            return True
        except self.ddb.exceptions.ConditionalCheckFailedException:
            return False


class ConfigMapStore:
    def __init__(self, api, namespace=None):
        self.core = client.CoreV1Api(api)
        self.ns = namespace or os.getenv("LOCAL_STORE_NAMESPACE", "scheduling-local-store")

    def _read(self, name):
        try:
            return self.core.read_namespaced_config_map(name, self.ns)
        except client.ApiException as e:
            if e.status == 404:
                return None
            raise

    def get_jobset(self, job_id):
        cm = self._read(f"recovery-code-{job_id}")
        if cm is None:
            raise LookupError(f"복구 원본 recovery-code-{job_id} 없음 (S3 code/{job_id}/jobset.json 대신)")
        return json.loads(cm.data["jobset.json"])

    def get_fencing(self, job_id):
        cm = self._read(f"recovery-fencing-{job_id}")
        if cm is None:
            return None
        return {k: (int(v) if k in NUMERIC and v != "" else (v or None)) for k, v in (cm.data or {}).items()}

    def set_phase(self, job_id, recovery_id, to_gen, allowed_from, new_phase, extra=None):
        cm = self._read(f"recovery-fencing-{job_id}")
        if cm is None:
            return False
        d = dict(cm.data or {})
        if (d.get("recovery_id") != recovery_id or d.get("recovery_to_gen") != str(to_gen)
                or d.get("recovery_phase") not in allowed_from):
            return False
        d.update({"recovery_phase": new_phase, "recovery_updated_at": _now(),
                  **{k: str(v) for k, v in (extra or {}).items()}})
        cm.data = d
        try:   # resourceVersion을 실은 replace = 조건부 갱신
            self.core.replace_namespaced_config_map(cm.metadata.name, self.ns, cm)
        except client.ApiException as e:
            if e.status == 409:
                raise Conflict(job_id) from e
            raise
        return True


def from_env(api):
    kind = os.getenv("RECOVERY_STORE", "configmap")
    if kind == "aws":
        return AwsStore()
    if kind == "configmap":
        return ConfigMapStore(api)
    raise ValueError(f"RECOVERY_STORE={kind} (aws | configmap)")
