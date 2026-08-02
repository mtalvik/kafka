package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Ex2 — fixed (tumbling) windows ("Where").
 *
 * Each event carries an event-time timestamp. FixedWindows cuts event
 * time into non-overlapping 1-minute slices; Sum.perKey then reduces
 * within each (key, window). An event belongs to exactly one fixed
 * window.
 *
 * Expected: alice has two events inside [0s,60s) -> 2, and one in
 * [60s,120s) -> 1. bob has one event in the first window -> 1.
 */
public class Ex2FixedWindows {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        p.apply(Create.timestamped(
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(0L)),
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(30_000L)),
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(70_000L)),
                        TimestampedValue.of(KV.of("bob", 1L), new Instant(10_000L))))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(Sum.longsPerKey())
                .apply(ParDo.of(new Utils.PrintFn<>("fixed")));

        p.run().waitUntilFinish();
    }
}
