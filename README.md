# krx-quant-dataloader

**한국 주식시장 데이터를 퀀트 분석에 최적화된 형태로 제공하는 Python 라이브러리**

`krx-quant-dataloader`는 [PyKRX](https://github.com/sharebook-kr/pykrx)의 한계를 개선하여 KRX 데이터를 퀀트 분석에 적합한 형태로 제공합니다. 기업행위(Corporate Action)가 조정된 수정종가를 제공하고, 미리 만들어진 생존편향 없는 유니버스를 제공해 현실적인 백테스팅을 돕습니다. Wide-format(날짜 × 종목코드) DataFrame 출력으로 즉시 전략 리서치에 활용할 수 있습니다.

**핵심 기능:**

- 🔄 **정확한 수정주가 계산**: 수정계수를 역산하여 올바른 수정종가 제공
- 🎲 **생존편향 없는 유니버스**: 과거 각 시점별 유동성이 높았던 종목 기반 동적 유니버스 제공 (`univ100`/`univ200`/`univ500`/`univ1000`)
- 📊 **Wide-format 출력**: 백테스팅에 최적화된 날짜(index) × 종목코드(columns) 2D DataFrame 형태로 데이터 반환
- 📁 **다양한 필드 지원**: 가격, 거래량, 거래대금 (추후 추가 필드 제공 예정)
- 🚦 **KRX API Rate Limit 준수**: 서버 부하 방지를 위한 자동 속도 제한 (1 req/sec)
- 🔄 **재개 가능한 데이터 수집**: 중단 후 재실행 시 누락 날짜만 추가 수집, atomic write로 안전성 보장
- ⚡ **로컬 Parquet DB 캐싱**: 초기화 시 자동 DB 구축 후 밀리초 단위 빠른 조회
- 🔒 **투명한 데이터 처리**: 암묵적 변환 없음, 모든 조정은 명시적 파라미터로 제어

---

## 빠른 데모

**import 포함 3줄로 데이터를 불러올 수 있습니다.**

```python
from krx_quant_dataloader import DataLoader

# 1. DataLoader 초기화 (자동으로 로컬 DB 구축)
loader = DataLoader(start_date='20180425', end_date='20180510')
# [Stage 0-1] 로컬 DB 확인 → 없으면 KRX API에서 자동 다운로드
# [Stage 2]   수정계수 계산 (기준시가를 이용한 역산)
# [Stage 3]   유동성 랭킹 계산 (유니버스 구축)
# → 결과: 10 dates × 2314 stocks 로컬 DB 구축 완료

# 2. 전체 종목 조회 (Raw 가격)
all_stocks = loader.get_data('close', adjusted=False)
print(all_stocks.shape)
# (10, 2314) - 10개 거래일 × 2314개 전체 종목

# 3. 유동성 Top 100 종목만 조회 (생존편향 없음)
top100 = loader.get_data('close', universe='univ100', adjusted=True)
print(top100.shape)
# (10, 285) - 각 날짜별로 유동성 Top 100 종목만 선택

print(top100.head())
# ISU_SRT_CD  000030  000120   000210  ...
# TRD_DD
# 20180425    15950.0     NaN  83900.0  ...  <- 이 날 Top 100에 없던 종목은 NaN
# 20180426    15950.0     NaN  85500.0  ...
# 20180427    16000.0     NaN  82800.0  ...

# 4. 특정 종목만 조회 (삼성전자, SK하이닉스, 카카오)
stocks = loader.get_data('close', universe=['005930', '000660', '035720'], adjusted=False)
print(stocks)
# ISU_SRT_CD  000660   005930  035720
# TRD_DD
# 20180425     82400  2520000  115500  <- 삼성전자 분할 전 가격
# 20180426     86500  2607000  114000
# 20180427     87100  2650000  113500
# ...
# 20180503     82900  2650000  113500  <- 분할 직전일
# 20180504     83000    51900  112000  <- 분할일 (2,650,000 → 51,900)
# 20180508     83500    52600  112000

# 5. 수정주가로 조회 (기업행위 자동 조정)
stocks_adj = loader.get_data('close', universe=['005930'], adjusted=True)
print(stocks_adj)
# ISU_SRT_CD  005930
# TRD_DD
# 20180425     50400  <- Raw 2,520,000원이 수정계수(0.02)로 정규화
# 20180426     52140  <- Raw 2,607,000원 → 52,140원
# 20180427     53000  <- Raw 2,650,000원 → 53,000원
# ...
# 20180503     53000  <- 분할 전 동일 스케일 유지
# 20180504     51900  <- 분할 후 가격
# 20180508     52600

# → 수정주가 덕분에 분할 전/후 가격이 동일 스케일로 정규화
#    올바른 수익률 계산 가능: (53000 - 50400) / 50400 = 5.16%

# 6. 수익률 계산
returns = stocks_adj.pct_change()
print(returns.head())
# ISU_SRT_CD    005930
# TRD_DD
# 20180425         NaN
# 20180426    0.034524  
# 20180427    0.016493  
```

---

## 설치

```bash
# PyPI 릴리스 시 설치 방법이 추가될 예정입니다
# 현재는 로컬에서 클론하여 설치:
git clone https://github.com/jaepil-choi/krx-quant-dataloader.git
cd krx-quant-dataloader
poetry install
```

---

## 요구사항

### 시스템 요구사항
- **Python**: ≥ 3.12, < 3.13
- **디스크 공간**: 로컬 Parquet DB 저장용 (1년 데이터 기준 ~100MB)
- **인터넷 연결**: KRX API 접근 필요

### Python 라이브러리 의존성
- `pyyaml` (≥ 6.0.2) - YAML 설정 파일 처리
- `curl-cffi` (≥ 0.13.0) - HTTP 요청 (KRX API 호출)
- `requests` (≥ 2.32.5) - HTTP 클라이언트
- `pydantic` (≥ 2.11.9) - 데이터 검증 및 모델링
- `pydantic-settings` (≥ 2.11.0) - 설정 관리
- `pyarrow` (≥ 21.0.0) - Parquet 파일 읽기/쓰기
- `pandas` (≥ 2.3.3) - DataFrame 처리 및 분석
- `pytest` (≥ 8.4.2) - 테스트 프레임워크

모든 의존성은 Poetry를 통해 자동으로 설치됩니다 (`pyproject.toml` 참조).

---

## 향후 계획

- **추가 필드 제공**: 시가, 고가, 저가 등 OHLC 데이터 전체 지원
- **밸류에이션 지표**: PER, PBR, ROE 등 재무 비율 추가 예정
- **기타 버그 수정 및 안정성 개선**

---

## 기여

현재 외부 기여는 받지 않고 있습니다. 향후 프로젝트가 안정화되면 기여 가이드라인을 공개할 예정입니다.

---

## 라이선스

본 프로젝트는 [MIT 라이선스](LICENSE) 하에 배포됩니다.

---

## 인용

학술 연구나 프로젝트에서 `krx-quant-dataloader`를 사용하는 경우 다음과 같이 인용해 주세요:

```bibtex
@software{krx_quant_dataloader_2025,
  title={krx-quant-dataloader: Korean Stock Market Data Loader for Quantitative Analysis},
  author={Jaepil Choi},
  year={2025},
  version={0.1.0},
  url={https://github.com/jaepil-choi/krx-quant-dataloader}
}
```

---

## ⚠️ 주의사항

- **데이터 사용 책임**: 본 라이브러리는 기능만 제공할 뿐, 데이터 수집, 저장 및 사용에 대한 책임은 전적으로 사용자에게 있습니다.
- **데이터 삭제**: 본 라이브러리를 사용해 퀀트 분석을 완료한 후에는 로컬에 저장된 데이터(`data/` 디렉토리)를 수동으로 삭제해 주시기 바랍니다.