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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Ex6 — accumulating vs discarding ("How").
 *
 * Same input and same trigger as Ex5, run through two windows that
 * differ only in refinement mode. The ON_TIME pane is identical in both
 * (value 2). The difference shows on the LATE pane:
 *
 *   accumulating -> LATE value 3  (window kept; late event refines the
 *                                  running total into a full result)
 *   discarding   -> LATE value 1  (window cleared after the ON_TIME
 *                                  pane; the late pane holds only the
 *                                  late event as an independent delta)
 *
 * Which one you want depends on the downstream sink: overwrite-in-place
 * wants accumulating; add-the-delta wants discarding.
 */
public class Ex6AccumulatingVsDiscarding {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        TestStream<KV<String, Long>> stream = TestStream
                .create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                .addElements(
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(0L)),
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(20_000L)))
                .advanceWatermarkTo(new Instant(60_000L))
                .addElements(
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(30_000L)))
                .advanceWatermarkToInfinity();

        PCollection<KV<String, Long>> in = p.apply(stream);

        in.apply("AccumWindow", Window.<KV<String, Long>>into(
                                FixedWindows.of(Duration.standardMinutes(1)))
                        .triggering(AfterWatermark.pastEndOfWindow()
                                .withLateFirings(AfterPane.elementCountAtLeast(1)))
                        .withAllowedLateness(Duration.standardMinutes(10))
                        .accumulatingFiredPanes())
                .apply("AccumSum", Sum.longsPerKey())
                .apply("AccumPrint", ParDo.of(new Utils.PrintFn<>("accum")));

        in.apply("DiscardWindow", Window.<KV<String, Long>>into(
                                FixedWindows.of(Duration.standardMinutes(1)))
                        .triggering(AfterWatermark.pastEndOfWindow()
                                .withLateFirings(AfterPane.elementCountAtLeast(1)))
                        .withAllowedLateness(Duration.standardMinutes(10))
                        .discardingFiredPanes())
                .apply("DiscardSum", Sum.longsPerKey())
                .apply("DiscardPrint", ParDo.of(new Utils.PrintFn<>("discard")));

        p.run().waitUntilFinish();
    }
}
