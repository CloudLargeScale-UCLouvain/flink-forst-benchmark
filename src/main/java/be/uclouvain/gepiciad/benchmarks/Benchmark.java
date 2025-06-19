package be.uclouvain.gepiciad.benchmarks;

import be.uclouvain.gepiciad.sources.Event;
import be.uclouvain.gepiciad.sources.EventSource;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.util.ParameterTool;

import java.util.concurrent.atomic.AtomicReference;

public class Benchmark {

    public static void main(String[] args) throws Exception{
        final ParameterTool pt = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.configure(new Configuration().set(
                StateBackendOptions.STATE_BACKEND,
                pt.get("state-backend", "rocksdb")
        )); // "rocksdb" or "forst"

        env.addSource(createEventSource(pt))
                .name("Source")
                .uid("Source")
                .keyBy(Event::getKey)
                .enableAsyncState()
                .map(new Mapper())
                .sinkTo(new DiscardingSink<>());

        env.execute("Simple Benchmark");
    }

    public static SourceFunction<Event> createEventSource(ParameterTool pt) {
        return new EventSource(
                pt.getInt("keyspace", 1000),
                pt.getInt("payload-size", 1000)
        );
    }

    public static class Mapper extends RichMapFunction<Event, String> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<String> valueState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            int index = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            valueState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "valueState" + index, StringSerializer.INSTANCE));
        }

        @Override
        public String map(Event event) throws Exception {
            AtomicReference<String> res = new AtomicReference<>();
            valueState.asyncValue().thenAccept(currentValue -> {
                res.set(currentValue);
                valueState.asyncUpdate(event.getPayload());
            });
            return res.get();
        }
    }
}
