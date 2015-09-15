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
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class BogusProducerTest {

    @Before
    public void setup() throws Exception {
        final TestingServer testingServer = new TestingServer(2181);

        final ZkClient zkClient = new ZkClient(testingServer.getConnectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
        AdminUtils.createTopic(zkClient, "bogus-topic", 10, 1, new Properties());

        // mock kafka
        final Properties props = new Properties();
        props.put("broker.id", "0");
        props.put("host.name", "localhost");
        props.put("port", "9092");
        props.put("log.dir", "/tmp/tmp_kafka_dir");
        props.put("zookeeper.connect", testingServer.getConnectString());
        props.put("replica.socket.timeout.ms", "1500");
        final KafkaConfig config = new KafkaConfig(props);

        final KafkaServerStartable kafkaServerStartable = new KafkaServerStartable(config);
        kafkaServerStartable.startup();
    }

    @Test
    public void testProduce() throws ExecutionException, InterruptedException {
        final Config config = ConfigFactory.parseFile(new File("src/test/resources/reference.conf"));
        BogusProducer producer = new BogusProducer(config);

        BogusObject object = new BogusObject("name", "message");
        Future<RecordMetadata> metadataFuture = producer.send(object);
        RecordMetadata metadata = metadataFuture.get();
        System.out.println(metadata);
    }
}
