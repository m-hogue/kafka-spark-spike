package monkey.mikeyo.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import monkey.mikeyo.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A simple kafka producer that produces {@link RandomObject}s to a configured kafka topic.
 */
public class RandomObjectProducer {
    private final KafkaProducer producer;

    private final Config config;

    public RandomObjectProducer(final Config config) {
        checkNotNull(config);

        final Config conf = config.getConfig("kafka.producers");
        this.config = conf.getConfig("random-producer");
        final Config producerConfig = this.config.getConfig(KafkaConfig.PRODUCER_CONFIG.getKey());

        // flatten config keys
        Properties props = new Properties();
        for(Map.Entry<String, ConfigValue> e : producerConfig.entrySet()) {
            props.put(e.getKey(), producerConfig.getString(e.getKey()));
        }
        System.out.println("Props: " + props);

        this.producer = new KafkaProducer(props);
    }

    public Future<RecordMetadata> send(final RandomObject object) {
        checkNotNull(object);

        return this.producer.send(new ProducerRecord<String, RandomObject>(this.config.getString(KafkaConfig.TOPIC.getKey()), object));
    }

    public Map<MetricName, ? extends Metric> metrics() {
        return this.producer.metrics();
    }

    public void close() {
        this.producer.close();
    }
}
