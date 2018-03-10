package global;

public class StringValue extends ValueClass {

    String value;


    public StringValue(String val) {
        super(val);
        this.value = val;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(Object val) {
        this.value = (String) val;
    }


    public boolean isequal(Object obj) {
        if (obj instanceof StringValue) {
            if (((String) ((StringValue) obj).getValue()).equals(this.getValue())) {
                return true;
            }
        }
        return false;
    }
}