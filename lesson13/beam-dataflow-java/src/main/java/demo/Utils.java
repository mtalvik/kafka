package demo;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;

/**
 * Shared helpers for the Dataflow-model examples.
 *
 * There is no broker, no SASL, no external config here. Every example
 * runs entirely in-process on the DirectRunner over bounded, in-memory
 * input, so output is deterministic and reproducible on any JDK 17.
 */
public final class Utils {

    private Utils() {
    }

    /**
     * Logs each element together with the two things that make streaming
     * output legible: the window it belongs to, and the pane metadata
     * (timing + index) that says WHEN in processing time this result
     * fired and whether it is a refinement of an earlier one.
     *
     * <p>{@code pane.getTiming()} is one of EARLY / ON_TIME / LATE — the
     * "When" axis of the Dataflow model made visible.
     */
    public static class PrintFn<T> extends DoFn<T, T> {

        private final String tag;

        public PrintFn(String tag) {
            this.tag = tag;
        }

        @ProcessElement
        public void process(@Element T element,
                            @Timestamp Instant ts,
                            BoundedWindow window,
                            PaneInfo pane,
                            OutputReceiver<T> out) {
            System.out.printf(
                    "[%-8s] %-24s ts=%s window=%s pane=%s#%d%s%n",
                    tag,
                    element,
                    ts,
                    window,
                    pane.getTiming(),
                    pane.getIndex(),
                    pane.isLast() ? " (final)" : "");
            out.output(element);
        }
    }
}
