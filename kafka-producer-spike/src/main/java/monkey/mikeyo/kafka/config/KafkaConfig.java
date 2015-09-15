package monkey.mikeyo.kafka.config;

public enum KafkaConfig {

    TOPIC,

    PRODUCER_CONFIG

    ;

    public String getKey() {
        return this.name().replaceAll("_",".").toLowerCase();
    }
}
