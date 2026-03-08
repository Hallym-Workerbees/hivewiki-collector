# HiveWiki Collector

HiveWiki Collector는 HiveWiki 프로젝트에서 사용할 데이터를 수집하는 서비스입니다.  
외부 데이터 소스를 크롤링하거나 수집하여 이후 파이프라인 단계에서 위키 콘텐츠로 변환할 수 있도록 데이터를 저장합니다.

---

## 개발 환경

이 프로젝트는 다음 도구들을 사용합니다.

- Python 3.12
- uv (dependency management)
- pre-commit (코드 품질 자동 검사)
- Ruff (Python lint + formatter)
- gitleaks (secret detection)
- commitizen (commit message validation)

---

## 시작하기

```bash
git clone <repository-url>
cd hivewiki-collector
uv sync
uv run pre-commit install --hook-type pre-commit --hook-type commit-msg
```

이 프로젝트는 pre-commit hooks를 사용합니다.  
코드가 자동으로 수정되면 커밋이 중단될 수 있으며, 수정된 파일을 확인한 뒤 다시 add하고 커밋하면 됩니다.

Commit message는 **영어로 작성해야 하며**, Conventional Commits 규칙을 따릅니다.  
자세한 규칙은 이 [문서](https://commitizen-tools.github.io/commitizen/tutorials/writing_commits/)를 참고하세요.
