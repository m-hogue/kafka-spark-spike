package monkey.mikeyo.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class BogusProducerTest {
    private KafkaServerStartable kafkaServerStartable;
    private ZkClient zkClient;

    @Before
    public void setup() throws Exception {
        final TestingServer testingServer = new TestingServer(2181);

        // mock kafka
        final Properties props = new Properties();
        props.put("broker.id", "0");
        props.put("host.name", "localhost");
        props.put("port", "9092");
        props.put("log.dir", "/tmp/tmp_kafka_dir");
        props.put("zookeeper.connect", testingServer.getConnectString());
        props.put("replica.socket.timeout.ms", "1500");
        final KafkaConfig config = new KafkaConfig(props);

        this.kafkaServerStartable = new KafkaServerStartable(config);
        this.kafkaServerStartable.startup();

        // create topic
        this.zkClient = new ZkClient(testingServer.getConnectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
        AdminUtils.createTopic(this.zkClient, "bogus-topic", 10, 1, new Properties());
    }

    @Test
    public void testProduce() throws ExecutionException, InterruptedException {

        final Config config = ConfigFactory.parseFile(new File("kafka-producer-spike/src/test/resources/reference.conf"));
        BogusProducer producer = new BogusProducer(config);

        BogusObject object = new BogusObject("name", "message");
        Future<RecordMetadata> metadataFuture = producer.send(object);
        // purely to block until the message has been delivered to topic & acked so that metrics are available.
        RecordMetadata metadata = metadataFuture.get();

        System.out.println("Metrics:");
        Map<MetricName, ? extends Metric> metrics = producer.metrics();
        for(MetricName m : metrics.keySet()) {
            System.out.println(m.name() + " : " + metrics.get(m).value());
        }

        // I don't have a consumer implemented here, or i'd assert that the consumer was able to consume the message
        // that i sent. Doesn't look like the metrics has anything useful to assert on.

        producer.close();
    }

    @After
    public void after() {
        this.kafkaServerStartable.shutdown();
        this.zkClient.close();
    }
}
