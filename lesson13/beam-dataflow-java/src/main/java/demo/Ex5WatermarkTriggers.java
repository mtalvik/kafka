package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Ex5 — watermark, triggers, and LATE data ("When").
 *
 * TestStream lets us drive the watermark by hand, so late data is
 * reproducible instead of a race. The scenario:
 *
 *   1. Two events for alice arrive inside the window [0s, 60s).
 *   2. We advance the watermark PAST the end of that window. The default
 *      AfterWatermark trigger fires the ON_TIME pane: alice = 2.
 *   3. THEN a third event arrives with event-time 30s — inside the same
 *      window, but the watermark has already passed it. This is late
 *      data behind the watermark: exactly the case a watermark cannot
 *      prevent (it was an estimate, not a guarantee).
 *   4. withLateFirings(...) fires a LATE pane. Because the mode is
 *      accumulating, the late pane is the full refined sum alice = 3,
 *      not a bare delta.
 *
 * Expected two panes for alice: ON_TIME#0 value 2, then LATE#1 value 3.
 * Compare Ex6 to see how the mode changes that second number.
 */
public class Ex5WatermarkTriggers {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        TestStream<KV<String, Long>> stream = TestStream
                .create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                .addElements(
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(0L)),
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(20_000L)))
                // watermark passes end of [0s,60s) -> ON_TIME pane fires
                .advanceWatermarkTo(new Instant(60_000L))
                // event-time 30s, but watermark already at 60s -> LATE
                .addElements(
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(30_000L)))
                .advanceWatermarkToInfinity();

        p.apply(stream)
                .apply(Window.<KV<String, Long>>into(
                                FixedWindows.of(Duration.standardMinutes(1)))
                        .triggering(AfterWatermark.pastEndOfWindow()
                                .withLateFirings(AfterPane.elementCountAtLeast(1)))
                        .withAllowedLateness(Duration.standardMinutes(10))
                        .accumulatingFiredPanes())
                .apply(Sum.longsPerKey())
                .apply(ParDo.of(new Utils.PrintFn<>("wm")));

        p.run().waitUntilFinish();
    }
}
