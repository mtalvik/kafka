package otus.kafka.streams;

import org.apache.kafka.streams.StreamsConfig;

import java.io.InputStream;
import java.util.Properties;

/**
 * Loads streams.properties, sets the per-exercise application.id, and forces
 * replication.factor=1 for internal (changelog/repartition) topics — without
 * that the app fails at startup on the single-node broker.
 */
final class Config {

    private Config() {
    }

    static Properties load(String applicationId) {
        Properties p = new Properties();
        try (InputStream in = Config.class.getResourceAsStream("/streams.properties")) {
            if (in == null) {
                throw new IllegalStateException("streams.properties not found on classpath");
            }
            p.load(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        p.putIfAbsent(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        // ex1 caching demo: -Pcache=0 forwards every update instead of deduplicating.
        String cache = System.getProperty("cache");
        if (cache != null) {
            p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, Long.parseLong(cache));
        }
        return p;
    }
}
