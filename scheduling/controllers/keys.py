"""다른 파트와 맞물리는 이름들. 이름을 바꿀 때는 이 파일만 고친다.

11/05 회고에서 확정 예정 (지금은 제안 값).
"""
import os

# --- Kueue / JobSet API
KUEUE_GROUP, KUEUE_VERSION = "kueue.x-k8s.io", "v1beta2"
JOBSET_GROUP, JOBSET_VERSION = "jobset.x-k8s.io", "v1alpha2"
KUEUE_NS = os.getenv("KUEUE_NAMESPACE", "kueue-system")
LABEL_QUEUE = "kueue.x-k8s.io/queue-name"
LABEL_PRIORITY = "kueue.x-k8s.io/priority-class"
GPU = "nvidia.com/gpu"

# --- 재배치 요청 CRD (장애복구 파트가 쓰고 재배치 실행기가 읽음)
RR_GROUP, RR_VERSION, RR_PLURAL = "scheduling.devoops.site", "v1alpha1", "relocationrequests"

# --- 잡(JobSet)에 붙이는 어노테이션: 디스패처가 읽음
ANN_TARGET = "devoops/target-cluster"        # 이 클러스터로만
ANN_EXCLUDE = "devoops/exclude-clusters"     # 쉼표 구분, 이 클러스터들은 빼고
ANN_MANUAL = "devoops/manual-dispatch"       # "true"면 디스패처가 손대지 않음 (실습용 수동 지명)

# --- 디스패처가 Workload에 남기는 기록
ANN_NOMINATED_AT = "devoops/nominated-at"    # 지명 시각 (epoch 초)
ANN_TRIED = "devoops/tried-clusters"         # 지명했지만 못 들어간 클러스터

# --- 재배치 실행기가 잡에 붙이는 라벨 (세대 = 같은 학습의 몇 번째 실행인지)
LABEL_JOB_ID = "devoops/job-id"
LABEL_GENERATION = "devoops/generation"
LABEL_REQUEST_ID = "devoops/request-id"

# --- 펜싱 / 노드
LABEL_FENCED = "devoops/fenced"              # MultiKueueCluster에 "true" -> 새 잡 안 보냄
TAINT_FAULTY = "devoops/faulty"              # 고장 노드 taint (NoSchedule)
GPU_NODE_SELECTOR = os.getenv("GPU_NODE_SELECTOR", "node-role=gpu")
PLACEHOLDER_PRIORITY_CLASS = "spare-placeholder"   # 예비 노드 자리 표시 Pod

# --- 큐 / 우선순위
NAMESPACE = os.getenv("JOB_NAMESPACE", "ml")
RECOVERY_QUEUE = os.getenv("RECOVERY_QUEUE", "lq-recovery")
PRIORITY_TRAINING = "training"
PRIORITY_AGED = "training-aged"
PRIORITY_RECOVERY = "recovery"
RECOVERY_PRIORITY_VALUE = 1000
