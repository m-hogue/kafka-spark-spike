package monkey.mikeyo.kafka;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import monkey.mikeyo.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Basic producer to take send a BogusObject to a kafka queue
 */
public class BogusProducer {

    private final Producer<String, BogusObject> producer;

    private final Config config;

    public BogusProducer(final Config config) {
        checkNotNull(config);

        final Config conf = config.getConfig("kafka.producers");
        this.config = conf.getConfig("bogus-producer");
        final Config producerConfig = this.config.getConfig(KafkaConfig.PRODUCER_CONFIG.getKey());

        // flatten config keys
        Properties props = new Properties();
        for(Map.Entry<String, ConfigValue> e : producerConfig.entrySet()) {
            props.put(e.getKey(), producerConfig.getString(e.getKey()));
        }
        System.out.println("Props: " + props);

        this.producer = new KafkaProducer(props);
    }

    public Future<RecordMetadata> send(final BogusObject object) {
        checkNotNull(object);

        return this.producer.send(new ProducerRecord<String, BogusObject>(this.config.getString(KafkaConfig.TOPIC.getKey()), object));
    }

    public Map<MetricName, ? extends Metric> metrics() {
        return this.producer.metrics();
    }

    public void close() {
        this.producer.close();
    }
}
