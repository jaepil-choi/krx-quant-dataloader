# Critical diagnostics of data fetching and IO in `pykrx`

- Evidence indicates the library wraps KRX internal endpoints via a single POST endpoint with a `bld` selector and minimal HTTP concerns handled. Key risks are: no timeout/retry/session handling, use of plain HTTP rather than HTTPS, inconsistent response-root extraction, and range-chunking logic that assumes a specific JSON shape. These raise reliability, maintainability, and silent failure risks when KRX responses change or when requesting large date ranges.

## Architecture overview (focused on IO)

- **Public API layer**: High-level functions in `stock/` and `bond/` call into `website/krx/*/core.py` classes that each map to a specific KRX report via a `bld` string and a `fetch(...)` method.
- **Website wrappers**: Each endpoint wrapper inherits `KrxWebIo`, sets `bld`, and calls `self.read(...)`, then selects a root key in the returned JSON (varies across endpoints).
- **Transport layer**: Implemented in `website/comm/webio.py` and extended in `website/krx/krxio.py`. Requests are made with `requests` directly, using static headers and no retries/timeout.

### Transport implementation (current)

```1:15:pykrx/website/comm/webio.py
import requests
from abc import abstractmethod


class Get:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0", 
            "Referer": "http://data.krx.co.kr/"
        }

    def read(self, **params):
        resp = requests.get(self.url, headers=self.headers, params=params)
        return resp
```

```22:38:pykrx/website/comm/webio.py
class Post:
    def __init__(self, headers=None):
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "http://data.krx.co.kr/"
        }
        if headers is not None:
            self.headers.update(headers)

    def read(self, **params):
        resp = requests.post(self.url, headers=self.headers, data=params)
        return resp

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError
```

### KRX IO wrapper and range chunking

```22:62:pykrx/website/krx/krxio.py
class KrxWebIo(Post):
    def read(self, **params):
        params.update(bld=self.bld)
        if 'strtDd' in params and 'endDd' in params:
            dt_s = pd.to_datetime(params['strtDd'])
            dt_e = pd.to_datetime(params['endDd'])
            delta = pd.to_timedelta('730 days')

            result = None
            while dt_s + delta < dt_e:
                dt_tmp = dt_s + delta
                params['strtDd'] = dt_s.strftime("%Y%m%d")
                params['endDd'] = dt_tmp.strftime("%Y%m%d")
                dt_s += delta + pd.to_timedelta('1 days')
                resp = super().read(**params)
                if result is None:
                    result = resp.json()
                else:
                    result['output'] += resp.json()['output']

                # 초당 2년 데이터 조회
                time.sleep(1)

            if dt_s <= dt_e:
                params['strtDd'] = dt_s.strftime("%Y%m%d")
                params['endDd'] = dt_e.strftime("%Y%m%d")
                resp = super().read(**params)

                if result is not None:
                    result['output'] += resp.json()['output']
                else:
                    result = resp.json()
            return result
        else:
            resp = super().read(**params)
            return resp.json()

    @property
    def url(self):
        return "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
```

## Findings and evidence

- **Single-endpoint POST with `bld` selector**: All KRX calls are POSTs to `getJsonData.cmd`, with a per-report `bld` string injected. The same handler is used for many disparate responses.

- **Response schema variability (root key differences)**: Endpoint wrappers extract different root keys, e.g. `output`, `OutBlock_1`, or `block1`.

- Example expecting `OutBlock_1` while using date ranges:

```233:266:pykrx/website/krx/market/core.py
class 전종목등락률(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT01602"

    def fetch(self, strtDd: str, endDd: str, mktId: str, adjStkPrc: int) \
            -> DataFrame:
        ...
        result = self.read(mktId=mktId, adjStkPrc=adjStkPrc, strtDd=strtDd,
                           endDd=endDd)
        return DataFrame(result['OutBlock_1'])
```

- Example expecting `output`:

```123:145:pykrx/website/krx/etx/core.py
class 전종목시세_ETF(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT04301"

    def fetch(self, date: str) -> DataFrame:
        ...
        result = self.read(trdDd=date)
        return DataFrame(result['output'])
```

- **Range-chunking assumes `output`**: In `KrxWebIo.read(...)` the accumulation path uses `result['output'] += resp.json()['output']`. If a date-ranged endpoint returns `OutBlock_1` or `block1`, this will mis-accumulate or raise a KeyError. Risk is latent until ranges exceed 2 years (triggering chunking), at which point behavior diverges by endpoint.

- **HTTP transport concerns**:
  - Plain HTTP used for KRX (`http://...`), not HTTPS.
  - No `timeout`, no `raise_for_status`, no retry/backoff policy, no connection pooling (`requests.Session`).
  - Global static headers include KRX referer even for non-KRX hosts (e.g., Naver), and lack endpoint-specific headers when KRX tightens checks.

- **Potential dead code/inconsistency**: Some legacy code appears to call `self.post(...)` which is not defined in the current base class.

```12:17:pykrx/website/krx/bond/core.py
    def fetch(self, fromdate, todate):
        try:
            result = self.post(fr_work_dt=fromdate, to_work_dt=todate)
            if len(result['block1']) == 0:
                return None
```

This likely used to be a different base or helper (e.g., `post()` returning parsed JSON). It will fail in the current structure unless monkey-patched.

## Impact

- **Reliability**: Network blips, 5xx, or 429 will cause brittle failures; large ranges may mis-accumulate for certain endpoints.
- **Maintainability**: Response-root variability forces each wrapper to hardcode keys; adding endpoints duplicates boilerplate and schema handling.
- **Security/compatibility**: Using HTTP rather than HTTPS is avoidable and can trigger middlebox/proxy issues.

## Minimal blueprint for a cleaner wrapper (reuse current structure)

- **Introduce a shared HTTP client** (singleton or DI-injected):
  - Use `requests.Session` with connection pooling, default `timeout` (e.g., 10s connect/read), and `raise_for_status()`.
  - Attach `HTTPAdapter` with retry/backoff for idempotent POSTs (KRX tolerates; tune to 3 retries on 502/503/504, jittered backoff).
  - Switch to `https://data.krx.co.kr/...` and set per-host headers; keep KRX referer but isolate Naver client.
  - Parameterize rate-limits; move `time.sleep(1)` to a throttler util so per-call code remains pure.

- **Normalize JSON extraction**:
  - Add a helper: `extract_rows(json, preferred_keys=('output','OutBlock_1','block1'))` returning a list (or empty) and failing loudly with a structured error if none are present.
  - Allow endpoint wrappers to specify a `json_root_key` override only when necessary.

- **Fix range chunking**:
  - Refactor chunking to accumulate rows via the extraction helper rather than assuming `output`.
  - Generalize date param detection: allow endpoints to declare `date_params=('strtDd','endDd')` and chunk size (730d default). Return a merged JSON with the same root key as the first chunk.

- **Error handling and observability**:
  - Log request metadata (endpoint `bld`, params sans sensitive fields, status code) at debug level; warn on retries.
  - Apply `util.dataframe_empty_handler` (or equivalent) at the wrapper boundary to avoid hard crashes while preserving logs.

- **Gradual adoption path**:
  - Keep public API unchanged. Implement a new `krxio2.py` with `KrxWebClient` and `KrxEndpointBase` and update a few wrappers first (e.g., endpoints with large date ranges and `OutBlock_1`).
  - After validation, migrate remaining wrappers, then retire old `KrxWebIo.read`.

### Sketch (proposed)

```python
class KrxWebClient:
    def __init__(self, base_url='https://data.krx.co.kr', headers=None, timeout=(5, 15)):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://data.krx.co.kr/'
        })
        if headers:
            self.session.headers.update(headers)
        self.timeout = timeout
        # mount retry-enabled adapter here

    def post_json(self, path, data):
        resp = self.session.post(f"{self.base_url}{path}", data=data, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

def extract_rows(payload, keys=("output","OutBlock_1","block1")):
    for k in keys:
        if k in payload:
            return payload[k], k
    raise KeyError(f"None of {keys} in response")

class KrxEndpointBase:
    path = "/comm/bldAttendant/getJsonData.cmd"
    bld = None
    json_keys = ("output","OutBlock_1","block1")
    date_params = ("strtDd","endDd")
    chunk_days = 730

    def __init__(self, client: KrxWebClient):
        self.client = client

    def fetch_json(self, **params):
        params = {**params, 'bld': self.bld}
        # chunk if both date params present
        # accumulate via extract_rows
        return payload
```

## Actionable recommendations

- Replace HTTP with HTTPS and add timeouts, retries, and a session-based client.
- Centralize row extraction with ordered key fallback to handle `output`/`OutBlock_1`/`block1` uniformly.
- Refactor range-chunk accumulation to use the centralized extractor instead of assuming `output`.
- Move throttling to a reusable rate-limiter and make it configurable.
- Clean up legacy calls (e.g., `self.post(...)`), or provide a compatibility shim.
- Add lightweight logging around IO for debuggability; wrap DataFrame conversion with a safe decorator where appropriate.

## Conclusion

- The current structure is serviceable but fragile at the IO boundaries and inconsistent in schema handling. Introducing a thin, robust transport/client layer and a unified response-extraction mechanism enables you to reuse the existing endpoint class structure while significantly improving reliability and maintainability, without a ground-up rewrite.
