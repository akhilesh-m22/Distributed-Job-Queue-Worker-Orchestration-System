# Verification notes

Validated locally on Windows with Python 3.11.9 and Node.js 24.19.0.

| Check | Result |
|---|---|
| Python test suite | 75 passed |
| API proxy regression suite | 6 passed |
| Frontend ESLint | Passed, no warnings |
| Next.js production build | Passed |
| Standalone production launcher | Starts broker, dashboard, three workers |
| Bounded launcher cleanup | Integration test verifies service ports close |
| Recovery load test | 102/102 jobs succeeded after a worker was hard-killed |
| Browser workflow | Scenario, inspection, retry, invalid JSON, custom submission verified |
| Browser console | No warnings/errors during successful workflow |
| Responsive layout | Desktop 1440px and mobile 390px checked; no page overflow at mobile width |

The load run used 100 burst jobs and two slow jobs, with three initial workers.
It recorded 123 claims, including retries and recovery, and about 12.4 completed
jobs/second on this machine. This is one demonstration run, not a capacity claim.
The killed worker's job was reclaimed and completed by another worker.

Browser testing found and fixed a same-origin proxy check that incorrectly
compared against Next's normalized localhost URL. A regression test now covers
requests using an incoming 127.0.0.1 Host header. Mobile testing also found and
removed an out-of-flow table label that created horizontal page overflow.

## Not verified here

Docker is not installed on the development machine, so image builds and Compose
startup have not been executed locally. The CI workflow includes a Compose build,
health check, and cleanup, but has not been run on a remote CI host during this work.

No cloud deployment, TLS proxy, cross-host network partition, or production
capacity test has been performed. This remains a single-broker design without
leader election, task execution deadlines, or automated data retention.

## Reproduce

```bash
pip install -r requirements-dev.txt
npm --prefix frontend ci
python -m pytest tests -q
npm --prefix frontend run lint
npm --prefix frontend test
npm --prefix frontend run build
python scripts/load_test.py --jobs 100 --workers 3 --slow-jobs 2 --slow-delay 3
python scripts/demo.py --production
```

![Verified Relay dashboard](dashboard.jpg)
