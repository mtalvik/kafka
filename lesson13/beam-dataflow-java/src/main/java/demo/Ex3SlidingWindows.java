package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Ex3 — sliding windows ("Where", overlapping).
 *
 * Window size 2 minutes, period 1 minute: because period < size, the
 * windows overlap and a single event lands in TWO windows at once.
 * Watch the output — the event at t=90s appears in both [0s,120s) and
 * [60s,180s). Fixed windows are just the special case size == period.
 *
 * Expected: alice's single event at 90s is counted in two overlapping
 * windows, so you see it summed into each.
 */
public class Ex3SlidingWindows {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        p.apply(Create.timestamped(
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(90_000L)),
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(30_000L))))
                .apply(Window.into(
                        SlidingWindows.of(Duration.standardMinutes(2))
                                .every(Duration.standardMinutes(1))))
                .apply(Sum.longsPerKey())
                .apply(ParDo.of(new Utils.PrintFn<>("sliding")));

        p.run().waitUntilFinish();
    }
}
