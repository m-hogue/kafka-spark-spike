package monkey.mikeyo.kafka;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import monkey.mikeyo.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

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
        final ConfigObject producerConfigObj = this.config.getObject(KafkaConfig.PRODUCER_CONFIG.getKey());
        this.producer = new KafkaProducer(producerConfigObj.unwrapped());
    }

    public Future<RecordMetadata> send(final BogusObject object) {
        checkNotNull(object);

        return this.producer.send(new ProducerRecord<String, BogusObject>(this.config.getString(KafkaConfig.TOPIC.getKey()), object));
    }
}
