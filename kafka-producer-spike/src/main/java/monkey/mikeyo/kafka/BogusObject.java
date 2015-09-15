package monkey.mikeyo.kafka;

public class BogusObject {

    private final String name;
    private final String message;

    public BogusObject(final String name, final String message) {
        this.name = name;
        this.message = message;
    }

    public String getName() {
        return name;
    }

    public String getMessage() {
        return message;
    }
}
