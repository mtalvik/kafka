package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

/**
 * Ex1 — the two core primitives ("What").
 *
 * ParDo does element-wise work (name -> (name, 1)). GroupByKey collects
 * all values for a key into one iterable. Everything higher-level
 * (Sum.perKey, Count.perKey, joins) is built from these two. This runs
 * on bounded data with the default global window — no windowing needed
 * because the input is finite and terminates on its own.
 */
public class Ex1CorePrimitives {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        List<String> events = Arrays.asList(
                "alice", "bob", "alice", "charlie", "alice", "bob");

        p.apply(Create.of(events))
                // ParDo: element-wise map to (key, 1)
                .apply("ToKV", MapElements
                        .into(TypeDescriptors.kvs(
                                TypeDescriptors.strings(),
                                TypeDescriptors.longs()))
                        .via(name -> KV.of(name, 1L)))
                // GroupByKey: gather all 1s per key
                .apply(GroupByKey.create())
                // ParDo again: reduce the grouped iterable to a sum
                .apply("Sum", ParDo.of(
                        new DoFn<KV<String, Iterable<Long>>, KV<String, Long>>() {
                            @ProcessElement
                            public void process(
                                    @Element KV<String, Iterable<Long>> in,
                                    OutputReceiver<KV<String, Long>> out) {
                                long sum = 0;
                                for (Long v : in.getValue()) {
                                    sum += v;
                                }
                                out.output(KV.of(in.getKey(), sum));
                            }
                        }))
                .apply("Print", ParDo.of(new Utils.PrintFn<>("count")));

        p.run().waitUntilFinish();
    }
}
