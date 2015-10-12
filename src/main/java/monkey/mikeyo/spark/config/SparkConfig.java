package monkey.mikeyo.spark.config;

public enum SparkConfig {
    MASTER,

    APP_NAME,

    CHECKPOINT_DIR

    ;

    public String getKey() {
        return this.name().replaceAll("_",".").toLowerCase();
    }
}
