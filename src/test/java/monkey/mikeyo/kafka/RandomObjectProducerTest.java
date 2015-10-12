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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;

/**
 * Simple JUnit class that tests the {@link RandomObjectProducer} kafka producer
 */
public class RandomObjectProducerTest {

    private KafkaServerStartable kafkaServerStartable;
    private ZkClient zkClient;
    private Random random;

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

        this.random = new Random();
    }

    @Test
    public void testProduce() throws ExecutionException, InterruptedException {
        final Config config = ConfigFactory.load();
        RandomObjectProducer producer = new RandomObjectProducer(config);

        RandomObject object = new RandomObject("name", UUID.randomUUID().toString(), random.nextInt());
        Future<RecordMetadata> metadataFuture = producer.send(object);
        // purely to block until the message has been delivered to topic & acked so that metrics are available.
        RecordMetadata metadata = metadataFuture.get();

        assertNotNull(metadata);

        producer.close();
    }

    @After
    public void after() {
        this.kafkaServerStartable.shutdown();
        this.zkClient.close();
    }
}
