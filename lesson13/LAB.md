# Lesson 13 Lab — Dataflow model with Apache Beam

**Prereq:** read `lecture.md`. JDK 17 + Gradle 8.8 installed.
No Kafka, no Docker, no EC2 — everything runs locally on Beam's
DirectRunner over fixed input.

**How to work:** for each part, read the input, guess the output, run the
command, compare. If your guess is wrong, read the "Why" line.

## Setup (run once)

```bash
cd ~/REPOS/teaching/kafka/lesson13/beam-dataflow-java
gradle build --no-daemon
```

---

## Part 1 · ParDo + GroupByKey (What)

Input: `alice, bob, alice, charlie, alice, bob` → map to `(name,1)`, group,
sum. Guess each count, then:

```bash
gradle ex1 --no-daemon
```

Expected:
```
KV{alice, 3}   KV{bob, 2}   KV{charlie, 1}
```

Two primitives only (`lecture.md` §6): `ParDo` = per element, `GroupByKey`
= gather per key. Everything else is built from these.

---

## Part 2 · Fixed windows (Where)

Input, 1-min fixed windows: `alice@0s  alice@30s  alice@70s  bob@10s`.
Guess which window each event lands in and the sums, then:

```bash
gradle ex2 --no-daemon
```

Expected:
```
KV{alice, 2}  window=[00:00..00:01)
KV{alice, 1}  window=[00:01..00:02)
KV{bob, 1}    window=[00:00..00:01)
```

alice's 0s+30s in the first minute → 2; 70s in the second → 1. Each event
is in exactly one fixed window.

---

## Part 3 · Sliding windows (overlap)

Input, size 2min / period 1min: `alice@30s  alice@90s`. Guess: in how many
windows does the 90s event appear?

```bash
gradle ex3 --no-daemon
```

Expected: `[-60s,60s)`→1, `[0s,120s)`→2, `[60s,180s)`→1. The 90s event is
counted in two overlapping windows.

**Try:** change `.every(standardMinutes(1))` to `standardMinutes(2)`,
rerun. Overlap disappears — period = size = fixed window.

---

## Part 4 · Session windows (data-driven)

Input, gap 1min: `alice@0s  alice@30s  alice@120s  bob@10s`. Guess which
events merge into one session.

```bash
gradle ex4 --no-daemon
```

Expected:
```
KV{alice, 2}  window=[00:00..00:01:30)   <- 0s + 30s merged
KV{alice, 1}  window=[00:02..00:03)      <- 120s new session
KV{bob, 1}    window=[00:00:10..00:01:10)
```

0s and 30s are within the gap → merge (session grows to 90s). 120s is too
far → new session. Session end = last event + gap.

**Try:** change alice's `120_000` to `80_000`, rerun. Now it merges → sum 3.

---

## Part 5 · Watermark + late data (When) — the core

Window `[0s,60s)`. Sequence: events @0s,@20s arrive → watermark moves to
60s (window "done") → event @30s arrives late (behind the watermark).
Guess: how many panes, and what values?

```bash
gradle ex5 --no-daemon
```

Expected:
```
KV{alice, 2}  pane=ON_TIME#0        <- watermark said "done"
KV{alice, 3}  pane=LATE#1 (final)   <- late 30s event refined it
```

The watermark was **wrong** — the 30s event belonged in the window but
arrived after. `withLateFirings` emits the second pane (`lecture.md` §8–9).

**Try (important):** delete the `.withLateFirings(...)` line, rerun. Guess
first — how many panes now? (Answer: one; the late event is buffered but
never emitted. This is the slide-code bug from §10.)

---

## Part 6 · Accumulating vs discarding (How)

Same input/trigger as Ex5, two modes side by side. Guess the LATE pane
value for each mode.

```bash
gradle ex6 --no-daemon
```

Expected:
```
[accum  ] KV{alice, 3}  pane=LATE#1   <- full refined total
[discard] KV{alice, 1}  pane=LATE#1   <- late event alone
```

Accumulating refines the total; discarding emits only the new delta. Pick
by sink: overwrite → accumulating, add-delta → discarding.

---

## Self-check quizzes

**A — windowing/triggers.** Match 1–4 to A–D:
1. clicks into sessions by 5-min inactivity, emit after close
2. orders per minute, partial every 10s, lateness 1 min
3. 5-min moving avg every 30s, by processing time, ignore late
4. exact daily report, once after day ends, lateness 1h

A. Session gap 5min, watermark, no lateness · B. Fixed 1min, early 10s +
watermark, lateness 1min · C. Sliding 5min/30s, processing-time trigger ·
D. Fixed 24h, watermark once, lateness 1h

**B — patterns.** Match 1–4 to A–D:
1. add-to-cart → recompute total → CRM · 2. alert if >100 fails/min ·
3. avg basket every 5min · 4. join orders + courier location for ETA

A. Transformation · B. Filters & thresholds · C. Windowed aggregation ·
D. Stream join

<details><summary>Answers</summary>
A: 1A 2B 3C 4D. B: 1A 2B 3C 4D. (B/1 is Transformation — fires per event
and republishes to CRM, no time window.)
</details>

---

## Done when

- [ ] Your predicted windows in Ex2/Ex4 matched the output
- [ ] You can say why Ex5 emits two panes
- [ ] You ran the "delete withLateFirings" experiment and know the result
- [ ] You can explain why Ex6 discard=1 but accum=3

## Troubleshooting

| Symptom | Fix |
|---|---|
| `gradle: command not found` | Gradle 8.8 not on PATH (lesson 8). |
| class file version error | Default JDK not 17; check `java -version`. |
| windows print `1970-...` | Expected — epoch timestamps, only offsets matter. |
| Ex5 late pane missing | You deleted `withLateFirings` — that's the experiment. |

Docs: Beam windowing & triggers — https://beam.apache.org/documentation/programming-guide/#windowing
