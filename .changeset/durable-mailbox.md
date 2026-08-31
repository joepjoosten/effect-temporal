---
"@effect-temporal/workflow": patch
---

`TemporalDurableMailbox`: schema-typed inbound message mailboxes for entity
workflows. Define a mailbox once with
`TemporalDurableMailbox.make(name, { payload: Schema })`; the workflow body
consumes with `take` (durably waits on a Temporal condition) or `poll`
(non-blocking), clients send with
`TemporalWorkflowInteractions.offerMailbox`, and workflows can signal each
other with `TemporalDurableMailbox.offer`. Messages arrive through a
reserved signal into an append-only per-run log; consumption cursors reset
on every pass of the body, so a resumed run replays already-consumed
messages deterministically.
