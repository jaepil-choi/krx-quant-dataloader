# kor-quant-dataloader (kqdl)

## Overview
`kor-quant-dataloader` (kqdl)는 `pykrx`와 같은 서드파티 라이브러리를 위한 wrapper로서, 한국 주식 시장에 중점을 둔 퀀트 리서치를 위해 설계되었습니다. 이 라이브러리는 금융 데이터의 로딩 및 처리 과정을 간소화하여, 상장 폐지 주식을 포함한 포괄적인 데이터셋을 제공합니다. 이를 통해 생존 편향을 제거하고, 다양한 분석 요구에 맞는 데이터 형식을 지원합니다.

`kor-quant-dataloader` (kqdl) is designed as a wrapper for third-party libraries such as `pykrx`, specifically created for quantitative research focused on the Korean stock market. The library simplifies the process of loading and processing financial data, offering a comprehensive dataset that includes data for delisted stocks. This approach effectively eliminates survivorship bias and supports various data formats to meet diverse analytical requirements.

## Features
- **시작 및 종료 날짜, 유니버스 선택**: 사용자가 지정한 시작 및 종료 날짜와 주식 유니버스를 선택하여, 해당 설정에 기반한 데��터셋을 검색할 수 있습니다.
- **생존 편향 없는 데이터 포함**: 상장 폐지 주식 데이터 포함.
- **다양한 데이터 형식 지원**: 전통적 분석을 위한 와이드 포맷 및 ML 기반 접근을 위한 멀티 인덱스 포맷 지원.
- **공휴일 처리 옵션**: 데이터셋에서 공휴일 제외.
- **로컬 캐시 지원**: 자주 사용하는 데이터를 로컬에 캐시하여 빠른 접근 지원.

- **Flexible Date Range and Customizable Universe**: Users can specify start and end dates and select a specific universe of stocks.
- **Survivorship Bias-Free**: Includes data for delisted stocks.
- **Various Data Formats**: Supports wide format for traditional analysis and multi-index format for ML-based approaches.
- **Holiday Handling**: Option to exclude holidays from the dataset.
- **Local Cache Support**: Cache frequently used data locally for faster access.

## Installation

**Note**: `kor-quant-dataloader` is currently under development and has not yet been deployed to PyPI. As such, it cannot be installed via `pip` at this moment. This section will be updated once the package is available on PyPI.

To keep informed about the release and availability of `kor-quant-dataloader` on PyPI, you can watch or star the repository on GitHub for updates.

- Required Dependency
    - `pykrx`
    - `pandas`
    - `pathlib`

## Quick Start
```python
import kor_quant_dataloader as kqdl

# 기본 설정으로 초기화
loader = kqdl.DataLoader(
    source='pykrx',
    start_date='2023-12-01',
    end_date='2023-12-10',
    universe=['005930', '000660'],
    remove_holidays=True,
    cache_dir='./data/cache'  # 선택적 캐시 디렉토리 지정
)

# 단일 필드 조회 (wide format)
df_close = loader.get_data('종가')

# 다중 필드 조회 (multi-index format)
df_multi = loader.get_data(['종가', 'PER', 'PBR'])

# 캐시 무시하고 새로운 데이터 다운로드
df = loader.get_data(['종가', 'PER'], download=True)

# 30일 이상된 캐시 파일 삭제
loader.clear_cache(older_than_days=30)

# 사용 가능한 필드 조회
available_fields = loader.show_available_fields()
```

## Architecture
```
[User Interface]
    DataLoader
        - 사용자 인터페이스 제공
        - 설정 관리
        |
        v
[Controller Layer]
    RequestHandler
        - 데이터 소스 관리
        - 요청 처리 조정
        |
        +------------+-------------+
        v            v             v
    DataFormatter  CacheManager  DataSource
        |             |             |
        |             |             |
        v             v             v
    데이터 포맷     캐시 처리      데이터 수집
    변환 담당       및 관리        (PyKrx 등)
```

각 컴포넌트의 책임:
- **DataLoader**: 사용자 인터페이스 제공, 설정 관리
- **RequestHandler**: 요청 분석 및 처리 조정, 각 컴포넌트 간 통신 관리
- **DataFormatter**: 데이터 포맷 변환 (wide/multi-index format)
- **CacheManager**: 로컬 캐시 관리, 캐시 수명주기 관리
- **DataSource**: 실제 데이터 수집 (PyKrx 등의 소스에서)

## Changelog

### Version 0.1.0 (2024-02-06)
- 초기 버전 릴리스
- 기본적인 데이터 로딩 기능 구현
- PyKrx 데이터 소스 지원

### Version 0.1.1 (2024-02-07)
#### 추가된 기능
- 로컬 캐시 시스템 구현
  - 캐시 디렉토리 설정 옵션
  - 캐시 수명 관리
  - 강제 다운로드 옵션
- 모듈 구조 개선
  - 컨트롤러 레이어 도입
  - 책임 분리 개선
  - 확장성 향상

#### 변경사항
- DataLoader 클래스에 캐시 관련 파라미터 추가
- RequestHandler에 캐시 관리 기능 통합
- 데이터 소스와 포맷터의 책임 명확화

## Upcoming Features

**Korean:**
- `kqdl.show_catalog()`를 통해 사용 가능한 data source 및 데이터 조회.
- 분기별 재무제표 등 특정 데이터에 대한 forward-filling와 같은 데이터 처리 옵션.
- `FinanceDataReader` 및 `OpenDartReader`와 같은 다른 서드파티 라이브러리에 대한 향후 지원.
- 유동성 상위 100, 500, 1000, 2000 종목과 같은 사전 계산된 유니버스 제공.
- 캐시 메타데이터 관리 기능 강화
- 멀티프로세스 안전한 캐시 접근
- 캐시 무결성 검사 기능

**English:**
- Display of available data sources and queryable data through `kqdl.show_catalog()`.
- Data processing options such as forward-filling for specific datasets.
- Future support for additional third-party libraries.
- Pre-calculated universes based on liquidity.
- Enhanced cache metadata management
- Thread-safe cache access
- Cache integrity verification

## Contributing
`kor_quant_dataloader`에 기여하는 것을 환영합니다. 기여 방법에 대한 지침은 `CONTRIBUTING.md` 파일을 참조해 주세요.
Contributions to `kor_quant_dataloader` are welcome! Please refer to the `CONTRIBUTING.md` file for guidelines on how to contribute.

## License
`kor-quant-dataloader`는 MIT 라이선스에 따라 제공되며, 라이선스 파일에서 확인할 수 있습니다.
`kor-quant-dataloader` is MIT licensed, as found in the LICENSE file.
