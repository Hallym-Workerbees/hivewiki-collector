# Prometheus 메트릭

HiveWiki Collector는 RSS 수집 파이프라인의 상태를 Prometheus 메트릭으로 노출합니다.

현재 collector는 RSS source만 수집합니다. HTML crawling metric은 의도적으로 포함하지 않습니다.

## 엔드포인트

메트릭은 기존 probe HTTP server에서 함께 노출합니다.

- Path: `/metrics`
- 기본 address: `0.0.0.0:8080`
- 로컬 확인: `curl http://localhost:8080/metrics`

같은 서버에서 `GET /healthz`도 제공합니다.

Kubernetes scrape 설정은 이 repository가 아니라 별도 GitOps repository에서 Prometheus annotation으로 관리합니다.

## Label 정책

메트릭 label은 cardinality가 낮게 유지되어야 합니다.

- `source`는 숫자 source id를 사용합니다.
- `target`은 `wikifier`처럼 안정적인 downstream 이름만 사용합니다.
- `reason`은 제한된 값만 사용합니다.

다음 값은 label로 사용하면 안 됩니다.

- URL
- canonical URL
- document id
- title
- RSS entry title

허용되는 dispatch failure reason은 다음과 같습니다.

- `http_error`
- `timeout`
- `parse_error`
- `db_error`
- `dispatch_error`
- `unknown`

## RSS Fetch 메트릭

| Metric | Type | Labels | 설명 |
| --- | --- | --- | --- |
| `collector_rss_fetch_total` | Counter | `source`, `status` | RSS fetch 시도 횟수입니다. |
| `collector_rss_fetch_duration_seconds` | Histogram | `source` | RSS fetch 소요 시간입니다. |
| `collector_rss_items_seen_total` | Counter | `source` | RSS feed에서 확인한 entry 수입니다. |
| `collector_rss_items_new_total` | Counter | `source` | 새 문서로 저장된 RSS 문서 수입니다. |
| `collector_rss_items_unchanged_total` | Counter | `source` | 이미 수집되어 변경 없음으로 처리된 RSS 문서 수입니다. |
| `collector_rss_items_failed_total` | Counter | `source` | item 단위 처리에 실패했거나 문서로 변환하지 못한 RSS entry 수입니다. |

`status`는 현재 `success`, `failure`를 사용합니다.

## 문서 변경 메트릭

| Metric | Type | Labels | 설명 |
| --- | --- | --- | --- |
| `collector_document_hash_changed_total` | Counter | `source` | 변경된 문서로 처리한 횟수입니다. |
| `collector_document_hash_unchanged_total` | Counter | `source` | 변경 없는 문서로 처리한 횟수입니다. |

현재 schema에는 별도의 document hash column이 없습니다. 따라서 이 metric은 현재 collector의 문서 식별 기준을 따릅니다.

- 새로 insert된 `(source_id, canonical_url)` 문서는 changed로 기록합니다.
- 이미 존재하는 `(source_id, canonical_url)` 문서는 unchanged로 기록합니다.

## Dispatch 메트릭

| Metric | Type | Labels | 설명 |
| --- | --- | --- | --- |
| `collector_dispatch_total` | Counter | `source`, `target`, `status` | downstream dispatch 시도 횟수입니다. |
| `collector_dispatch_failed_total` | Counter | `source`, `target`, `reason` | 실패 reason별 dispatch 실패 횟수입니다. |
| `collector_dispatch_duration_seconds` | Histogram | `source`, `target` | dispatch 소요 시간입니다. |

현재 RSS collector의 dispatch target은 `wikifier`입니다.

## Source 상태 메트릭

| Metric | Type | Labels | 설명 |
| --- | --- | --- | --- |
| `collector_last_success_timestamp` | Gauge | `source` | source가 마지막으로 성공 처리된 Unix timestamp입니다. |
| `collector_last_failure_timestamp` | Gauge | `source` | source가 마지막으로 실패 처리된 Unix timestamp입니다. |
| `collector_consecutive_failures` | Gauge | `source` | source의 현재 연속 실패 횟수입니다. |
| `collector_source_next_run_timestamp` | Gauge | `source` | source의 다음 실행 예정 Unix timestamp입니다. |
| `collector_source_enabled` | Gauge | `source` | source가 활성화되어 있으면 `1`, 아니면 `0`입니다. |

## Queue와 Source 수 메트릭

| Metric | Type | Labels | 설명 |
| --- | --- | --- | --- |
| `collector_pending_documents` | Gauge | 없음 | 현재 pending 문서 수입니다. |
| `collector_failed_documents` | Gauge | 없음 | 현재 failed 문서 수입니다. |
| `collector_dead_documents` | Gauge | 없음 | 현재 dead 문서 수입니다. |
| `collector_enabled_sources` | Gauge | 없음 | 현재 enabled source 수입니다. |
| `collector_disabled_sources` | Gauge | 없음 | 현재 disabled source 수입니다. |
| `collector_active_sources` | Gauge | 없음 | 현재 실행 시점이 된 enabled source 수입니다. |

## Loop 메트릭

| Metric | Type | Labels | 설명 |
| --- | --- | --- | --- |
| `collector_loop_duration_seconds` | Histogram | 없음 | collector polling loop 전체 소요 시간입니다. |
| `collector_loop_errors_total` | Counter | 없음 | collector loop에서 발생한 예상 밖 오류 횟수입니다. |

## Logging 정책

정상적인 메트릭 update는 log를 남기지 않습니다.

`/metrics` scrape 요청도 INFO log를 남기지 않습니다. probe server의 request log는 debug level로만 기록되어 scrape log noise를 줄입니다.
