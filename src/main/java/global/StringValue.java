package global;

public class StringValue extends ValueClass {
	
    String value;

    public StringValue(String val) {
        super();
        this.value = val;
    }

    public StringValue(Object val) {
        this.value = (String)val;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(Object val) {
        this.value = (String)val;
    }
}