package global;

public class IntegerValue extends ValueClass {
    int value;

    public IntegerValue(Object obj) {
        value = (Integer) obj;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object val) {
        this.value = (Integer) val;
    }
}