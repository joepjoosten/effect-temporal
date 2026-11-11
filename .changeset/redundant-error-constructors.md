---
"@effect-temporal/client": patch
"@effect-temporal/workflow": patch
---

Remove the redundant constructors from the tagged error classes; they inherit the identical constructor from `Data.TaggedError`. No behaviour change.
