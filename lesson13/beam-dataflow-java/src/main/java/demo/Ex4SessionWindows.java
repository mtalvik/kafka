package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Ex4 — session windows ("Where", data-driven / unaligned).
 *
 * A session extends 1 minute past each event; events closer together
 * than the gap merge into one session, events farther apart start a new
 * one. Sessions are per-key (unaligned) — this is the case earlier
 * systems could not express and the main contribution of the model.
 *
 * Expected: alice's events at 0s and 30s merge into one session (gap
 * < 60s) summing to 2; her event at 120s is more than a minute after
 * the previous, so it opens a second session summing to 1. bob's single
 * event is its own session.
 */
public class Ex4SessionWindows {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        p.apply(Create.timestamped(
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(0L)),
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(30_000L)),
                        TimestampedValue.of(KV.of("alice", 1L), new Instant(120_000L)),
                        TimestampedValue.of(KV.of("bob", 1L), new Instant(10_000L))))
                .apply(Window.into(
                        Sessions.withGapDuration(Duration.standardMinutes(1))))
                .apply(Sum.longsPerKey())
                .apply(ParDo.of(new Utils.PrintFn<>("session")));

        p.run().waitUntilFinish();
    }
}
