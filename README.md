# K8s

복구 컨트롤러, 디스패처·재배치 실행기, 학습 잡 투입기와 이미지 빌드 CI를 관리합니다.

| 경로 | 용도 |
| --- | --- |
| `recovery-controller/` | 복구 컨트롤러 코드·이미지·테스트 |
| `scheduling/controllers/` | 디스패처·재배치 실행기 코드·이미지·테스트 |
| `scheduling/cluster/`, `scheduling/e2e/`, `scheduling/examples/` | kind·KWOK 기반 실습 및 통합 테스트 |
| `scheduling/kueue/`, `scheduling/jobset/`, `scheduling/queues/` | 실습 클러스터에서 사용하는 설치·큐 구성 |

운영 배포 매니페스트는 [Gitops](https://github.com/DevOops-Team/Gitops)에 있습니다. Argo CD는 Gitops의 환경별 Application을 읽습니다. 클러스터 설치·부트스트랩·Secret 등록은 [IaC](https://github.com/DevOops-Team/IaC)에서 관리합니다.

이미지 빌드와 테스트는 K8s에서 실행합니다. 운영 이미지 digest·리소스 설정은 Gitops의 해당 환경에서 갱신합니다.

- [Gitops 등록 경로](docs/bootstrap.md)
- [기존 Application 인계](docs/migration.md)
- [복구 컨트롤러](recovery-controller/README.md)
- [스케줄링 테스트](scheduling/e2e/README.md)
