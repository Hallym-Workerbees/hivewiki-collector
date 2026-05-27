# HiveWiki Collector

HiveWiki Collector는 HiveWiki 파이프라인의 수집 단계입니다.

현재 collector는 RSS source를 주기적으로 polling하고, 새 문서를 데이터베이스에 저장한 뒤 downstream wikifier가 처리할 수 있도록 Redis queue로 dispatch합니다. 수집 대상의 원문 URL, 제목, 본문 요약, 발행 시각을 보존하고, source별 polling 상태와 dispatch 상태를 추적합니다.

## 역할

Collector는 다음 책임을 가집니다.

- 활성화된 RSS source 중 실행 시점이 된 source를 claim합니다.
- RSS feed를 가져오고 feed entry를 내부 문서 모델로 변환합니다.
- 이미 수집한 문서는 중복으로 저장하지 않습니다.
- 새 문서에 ingestion job을 만들고 Redis queue에 전달합니다.
- source의 성공, 실패, 다음 실행 시각, 연속 실패 횟수를 관리합니다.
- Prometheus 메트릭을 노출하여 수집, dispatch, queue 상태를 관측할 수 있게 합니다.

HTML crawling은 현재 collector의 범위에 포함되어 있지 않습니다.

## 큰 흐름

```text
RSS Source
  -> Collector polling loop
  -> RSS fetch and parse
  -> source_documents 저장
  -> ingestion_jobs 생성
  -> Redis queue dispatch
  -> Wikifier downstream
```

Collector loop는 대략 다음 순서로 동작합니다.

1. `sources.next_poll_at` 기준으로 실행 가능한 source를 가져옵니다.
2. source가 최초 backfill 대상이면 backfill limit만큼 RSS 문서를 수집합니다.
3. 이후 incremental polling에서는 기존 문서를 만날 때까지 RSS를 스캔합니다.
4. 새 문서를 `source_documents`에 저장하고 `ingestion_jobs`를 생성합니다.
5. publish 가능한 ingestion job을 Redis queue에 넣습니다.
6. source, document, queue, loop 상태를 metrics로 갱신합니다.

## 주요 구성

- `app/main.py`: collector loop, source 처리, dispatch orchestration
- `app/collectors/rss.py`: RSS fetch, parsing, document 변환
- `app/db.py`: PostgreSQL query와 상태 변경
- `app/publisher.py`: Redis queue payload 생성과 dispatch
- `app/probe.py`: `/healthz`, `/metrics` HTTP endpoint
- `app/metrics.py`: Prometheus metric 정의와 helper
- `schema.sql`: PostgreSQL schema

## 개발 환경

이 프로젝트는 다음 도구들을 사용합니다.

- Python 3.12
- uv
- pre-commit
- Ruff
- gitleaks
- commitizen

## 시작하기

```bash
git clone <repository-url>
cd hivewiki-collector
uv sync
uv run pre-commit install --hook-type pre-commit --hook-type commit-msg
```

이 프로젝트는 pre-commit hooks를 사용합니다. 코드가 자동으로 수정되면 커밋이 중단될 수 있으며, 수정된 파일을 확인한 뒤 다시 add하고 커밋하면 됩니다.

## 운영 엔드포인트

Collector는 probe HTTP server를 통해 상태와 메트릭을 노출합니다.

- Health check: `GET /healthz`
- Metrics: `GET /metrics`
- 기본 listen address: `0.0.0.0:8080`

Kubernetes manifest와 Prometheus scrape annotation은 별도 GitOps repository에서 관리합니다.

## 문서

- [기여 가이드](docs/contributing.md): 브랜치 이름, 커밋 메시지, 작업 규칙
- [데이터베이스 스키마](docs/schema.md): table, status, index 설명
- [Prometheus 메트릭](docs/metrics.md): `/metrics` endpoint와 `collector_*` metric 목록
