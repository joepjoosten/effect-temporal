---
"@effect-temporal/workflow": patch
"@effect-temporal/testing": patch
---

Await worker shutdown before releasing native connections, including interrupted and never-started workers. Use the owned lifecycle in the test harness.
