# 갱 스케줄링 실습 (우리 망 이식 대상 아님)

워커 Kueue의 `waitForPodsReady`(kueue/worker)가 "4장 중 3장만 뜬 잡"을 통째로 내리는지 확인한다.

```bash
cd scheduling/examples/gang
./run-scenario.sh setup     # 처음 한 번
./run-scenario.sh run       # 4분 관찰
./run-scenario.sh cleanup
```

우리 망으로 옮길 것은 `kueue/worker`의 `waitForPodsReady` 설정뿐이다. timeout은 EKS 이미지 pull 시간을 보고 오버레이에서 정한다.
