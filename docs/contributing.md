# 기여 가이드

이 문서는 HiveWiki Collector repository에서 작업할 때의 기본 규칙을 정리합니다.

## 브랜치 이름

브랜치 이름은 작업 목적이 드러나도록 짧고 명확하게 작성합니다.

권장 prefix:

- `feat/`: 기능 추가
- `fix/`: 버그 수정
- `docs/`: 문서 변경
- `refactor/`: 동작 변경 없는 구조 개선
- `test/`: 테스트 추가 또는 수정
- `chore/`: 설정, 의존성, 빌드 등 기타 작업

예시:

```text
feat/prometheus-metrics
fix/rss-timeout-handling
docs/schema-reference
refactor/source-claiming
```

로컬 Git ref 충돌 등으로 slash 형태를 사용할 수 없는 경우에는 같은 의미를 유지한 hyphen 형태를 사용할 수 있습니다.

```text
feat-prometheus-metrics
```

## 커밋 메시지

커밋 메시지는 영어로 작성하고 Conventional Commits 형식을 따릅니다.

기본 형식:

```text
<type>: <summary>
```

예시:

```text
feat: expose prometheus metrics
fix: handle rss fetch timeout
docs: add schema reference
chore: update dependencies
```

자주 사용하는 type:

- `feat`: 사용자 또는 운영자가 체감하는 기능 추가
- `fix`: 버그 수정
- `docs`: 문서만 변경
- `refactor`: 외부 동작 변경 없는 코드 구조 개선
- `test`: 테스트 추가 또는 수정
- `chore`: 도구, 설정, dependency 등 보조 작업

커밋 summary는 소문자로 시작하는 짧은 영어 문장으로 작성합니다. 마침표는 붙이지 않습니다.

## 작업 원칙

- Kubernetes manifest는 이 repository에서 수정하지 않습니다.
- GitOps repository에서 관리되는 배포 설정은 별도 PR로 다룹니다.
- RSS collector의 label, log, metric은 cardinality와 log volume을 낮게 유지합니다.
- URL, title, document id, canonical URL은 metric label로 사용하지 않습니다.
- 정상적인 high-frequency loop에서는 불필요한 INFO log를 추가하지 않습니다.
- schema나 queue payload를 바꿀 때는 관련 문서를 함께 갱신합니다.

## 개발 환경

초기 설정:

```bash
uv sync
uv run pre-commit install --hook-type pre-commit --hook-type commit-msg
```

pre-commit hook이 파일을 자동 수정하면 변경사항을 확인한 뒤 다시 stage하고 commit합니다.
