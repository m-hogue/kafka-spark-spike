package monkey.mikeyo.kafka;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * A class to hold randomly generated information used for consumption in a spark job.
 */
public class RandomObject implements Serializable {
    private String name;
    private String strValue;
    private Integer intValue;

    public RandomObject(){}

    public RandomObject(final String name, final String strValue, final Integer intValue) {
        this.name = name;
        this.strValue = strValue;
        this.intValue = intValue;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setStrValue(String strValue) {
        this.strValue = strValue;
    }

    public void setIntValue(Integer intValue) {
        this.intValue = intValue;
    }

    public String getName() {
        return this.name;
    }

    public String getStrValue() {
        return this.strValue;
    }

    public Integer getIntValue() {
        return this.intValue;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", name)
                .append("strValue", strValue)
                .append("intValue", intValue)
                .toString();
    }
}
