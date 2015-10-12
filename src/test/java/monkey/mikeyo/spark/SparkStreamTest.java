package monkey.mikeyo.spark;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kafka.admin.AdminUtils;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import monkey.mikeyo.kafka.RandomObject;
import monkey.mikeyo.kafka.RandomObjectProducer;
import monkey.mikeyo.kafka.serializer.RandomObjectDecoder;
import monkey.mikeyo.spark.config.SparkConfig;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class SparkStreamTest implements Serializable {
    private static String master;
    private static String appName;
    private static String tmpSparkDir;
    private static final String topic = "random-topic";
    private static final HashMap<String, Integer> topics = new HashMap();

    private static KafkaServerStartable kafkaServerStartable;
    private static TestingServer testingServer;
    private static ZkClient zkClient;
    private static final Random random = new Random();
    private static long numSent;
    private static RandomObjectProducer producer;

    @Before
    public void before() throws Exception {
        final Config sparkConfig = ConfigFactory.load().getConfig("spark");
        master = sparkConfig.getString(SparkConfig.MASTER.getKey());
        appName = sparkConfig.getString(SparkConfig.APP_NAME.getKey());
        tmpSparkDir = sparkConfig.getString(SparkConfig.CHECKPOINT_DIR.getKey());

        testingServer = new TestingServer(2181);
        // mock kafka
        final Properties props = new Properties();
        props.put("broker.id", "0");
        props.put("host.name", "localhost");
        props.put("port", "9092");
        props.put("log.dir", "/tmp/tmp_kafka_dir");
        props.put("zookeeper.connect", testingServer.getConnectString());
        props.put("replica.socket.timeout.ms", "1500");
        final KafkaConfig kafkaConfig = new KafkaConfig(props);

        kafkaServerStartable = new KafkaServerStartable(kafkaConfig);
        kafkaServerStartable.startup();

        // create topic
        zkClient = new ZkClient(testingServer.getConnectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
        AdminUtils.createTopic(zkClient, topic, 10, 1, new Properties());

        // produce 5 random messages
        final Config config = ConfigFactory.load();
        producer = new RandomObjectProducer(config);
        numSent = 5L;
        for(int i = 0; i < numSent; i++) {
            final RandomObject object = new RandomObject("name", UUID.randomUUID().toString(), random.nextInt());
            final Future<RecordMetadata> metadataFuture = producer.send(object);
            final RecordMetadata metadata = metadataFuture.get();

            // assert that message was successfully sent.
            assertNotNull(metadata);
            System.out.println("message produced to partition: " + metadata.partition());
        }

    }

    @Test
    public void testStreaming() throws ExecutionException, InterruptedException {
        topics.put(topic, 1);

        final SparkConf sparkConf = new SparkConf()
                .setMaster(master)
                .setAppName(appName);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        jssc.checkpoint(tmpSparkDir);

        final HashMap<String, String> kafkaParams = new HashMap();
        kafkaParams.put("zookeeper.connect", testingServer.getConnectString());
        kafkaParams.put("group.id", "test-consumer-" + random.nextInt(10000));
        kafkaParams.put("auto.offset.reset", "smallest");

        final JavaPairDStream<String, RandomObject> stream = KafkaUtils.createStream(jssc,
                String.class,
                RandomObject.class,
                StringDecoder.class,
                RandomObjectDecoder.class,
                kafkaParams,
                topics,
                StorageLevel.MEMORY_ONLY_SER());

        // grab all the values
        System.out.println("Mapping...");
        final JavaDStream<RandomObject> records = stream.map(Tuple2::_2);

        // count the occurrences
        System.out.println("Counting values...");
        final LongAdder longAdder = new LongAdder();
        records.count().foreachRDD(rdd -> {
            rdd.collect().forEach(longAdder::add);
            return null;
        });

        System.out.println("Counting objects by name...");
        // count the number of messages by RandomObject name. In this case, all objects have the same name
        final Map<String, Long> nameCountsMap = new HashMap();
        final JavaDStream<String> names = records.map(RandomObject::getName);
        names.countByValue().foreachRDD(rdd -> {
            for(Tuple2<String,Long> tuple : rdd.collect()) {
                if(nameCountsMap.containsKey(tuple._1())) {
                    nameCountsMap.put(tuple._1(), nameCountsMap.get(tuple._1()) + tuple._2());
                } else {
                    nameCountsMap.put(tuple._1(), tuple._2());
                }
            }
            return null;
        });


        jssc.start();
        jssc.awaitTermination(3000);

        // assert that the spark job processed the same number of messages we produced to the kafka topic 'random-topic'
        assertSame(numSent, longAdder.sum());
        assertEquals(1, nameCountsMap.keySet().size());
        assertEquals("name", nameCountsMap.keySet().iterator().next());
        assertEquals(5L, nameCountsMap.get("name").longValue());
    }

    @After
    public void after() {
        producer.close();

        // clean up kafka partitions
        File logDir = new File("/tmp/tmp_kafka_dir");
        for(File f : logDir.listFiles()) {
            Utils.deleteRecursively(f);
        }
        // clean up spark tmp dir
        logDir = new File(tmpSparkDir);
        for(File f : logDir.listFiles()) {
            Utils.deleteRecursively(f);
        }

        kafkaServerStartable.shutdown();
        zkClient.close();
    }
}
