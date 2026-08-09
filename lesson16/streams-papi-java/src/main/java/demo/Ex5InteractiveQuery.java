package demo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Ex5 - Interactive Queries across two instances.
 *
 *   gradle ex5 --no-daemon -Pport=8080     terminal A
 *   gradle ex5 --no-daemon -Pport=8081     terminal C
 *
 * Counts events per key into "count-store" and serves GET /count?key=<key>.
 *
 * Plain com.sun.net.httpserver from the JDK, not Spring: two Spring Boot JVMs
 * next to the broker do not fit on a t3.small.
 *
 * The point of the exercise: BOTH ports return the same answer for every key,
 * although neither instance holds all the data. iq-events has 3 partitions,
 * two instances split them 2+1, and queryMetadataForKey routes each key to
 * whoever owns it.
 */
public final class Ex5InteractiveQuery {

    private static final String STORE = "count-store";
    private static volatile KafkaStreams streams;
    private static HostInfo self;

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("port", "8080"));
        self = new HostInfo("localhost", port);

        Properties props = Utils.streamProps("lesson16-ex5");
        // How this instance advertises itself to the rest of the group.
        // Streams gossips it through the consumer group protocol; without it
        // queryMetadataForKey has no host to return.
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, self.host() + ":" + self.port());
        // Each instance needs its own state directory on this shared host.
        props.put(StreamsConfig.STATE_DIR_CONFIG, "state/lesson16-ex5-" + port);

        StreamsBuilder builder = new StreamsBuilder();
        // Consumed.with is not optional here: without it the stream is typed
        // KStream<Object, Object> from the default serdes, and count() then
        // demands a Materialized<Object, Long, ...> that will not match.
        builder.stream("iq-events", Consumed.with(Serdes.String(), Serdes.String()))
               .groupByKey()
               .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE)
                       .withKeySerde(Serdes.String())
                       .withValueSerde(Serdes.Long()));

        streams = new KafkaStreams(builder.build(), props);

        // The endpoint returns 503 until this prints RUNNING. That is not a
        // bug - it is the InvalidStateStoreException window from the lecture,
        // handled instead of discovered.
        streams.setStateListener((newState, oldState) ->
                System.out.printf("state=%s (was %s)%n", newState, oldState));

        startHttp(port);
        System.out.printf("listening on http://localhost:%d/count?key=<key>%n", port);

        Utils.runUntilShutdown(streams);
    }

    private static void startHttp(int port) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/count", Ex5InteractiveQuery::handleCount);
        server.setExecutor(null);   // single-threaded is plenty for a lab
        server.start();
    }

    private static void handleCount(HttpExchange exchange) {
        try {
            Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
            String key = query.get("key");
            if (key == null || key.isBlank()) {
                respond(exchange, 400, "missing ?key=\n");
                return;
            }

            // A forwarded request carries local=1 so the receiving instance
            // answers from its own store instead of routing again. Without it
            // two instances could bounce a request back and forth.
            boolean localOnly = "1".equals(query.get("local"));
            if (localOnly) {
                respondLocal(exchange, key);
                return;
            }

            KeyQueryMetadata metadata = streams.queryMetadataForKey(
                    STORE, key, Serdes.String().serializer());

            // During a rebalance the assignment is unknown and the sentinel
            // host is HostInfo("unavailable", -1). Never treat it as an address.
            if (metadata == null || KeyQueryMetadata.NOT_AVAILABLE.equals(metadata)) {
                respond(exchange, 503, "rebalancing, try again\n");
                return;
            }

            HostInfo active = metadata.activeHost();
            if (self.equals(active)) {
                System.out.printf("query key=%s -> local%n", key);
                respondLocal(exchange, key);
            } else {
                System.out.printf("query key=%s -> remote %s:%d%n",
                        key, active.host(), active.port());
                respondRemote(exchange, active, key);
            }
        } catch (Exception e) {
            respond(exchange, 500, "error: " + e.getMessage() + "\n");
        }
    }

    private static void respondLocal(HttpExchange exchange, String key) {
        ReadOnlyKeyValueStore<String, Long> store;
        try {
            store = streams.store(
                    org.apache.kafka.streams.StoreQueryParameters.fromNameAndType(
                            STORE, QueryableStoreTypes.keyValueStore()));
        } catch (InvalidStateStoreException e) {
            // Starting up, or the store is restoring from the changelog after
            // a rebalance. Both are temporary and both are a 503, not a 500.
            respond(exchange, 503, "store not available yet\n");
            return;
        }

        Long count = store.get(key);
        if (count == null) {
            // Unknown key is a 404, not a NullPointerException. This is the
            // bug the lecture warns about.
            respond(exchange, 404, "no such key: " + key + "\n");
            return;
        }
        respond(exchange, 200, count + "\n");
    }

    private static void respondRemote(HttpExchange exchange, HostInfo host, String key) {
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(String.format("http://%s:%d/count?key=%s&local=1",
                            host.host(), host.port(), key)))
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());
            respond(exchange, response.statusCode(), response.body());
        } catch (Exception e) {
            // The owning instance was killed and the group has not rebalanced
            // yet - this is phase 1 of LAB step 6.3.
            respond(exchange, 502, "owner " + host.host() + ":" + host.port()
                    + " unreachable: " + e.getClass().getSimpleName() + "\n");
        }
    }

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> out = new HashMap<>();
        if (raw == null) {
            return out;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                out.put(pair.substring(0, eq), pair.substring(eq + 1));
            }
        }
        return out;
    }

    private static void respond(HttpExchange exchange, int status, String body) {
        try (OutputStream out = exchange.getResponseBody()) {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            out.write(bytes);
        } catch (Exception e) {
            System.err.println("failed to respond: " + e.getMessage());
        }
    }
}
