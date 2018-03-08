package global;

public class IntegerValue extends ValueClass {
    int value;

    public IntegerValue(int val) {
        super();
        this.value = val;
    }

    public IntegerValue(Object val) {
        this.value = ((Integer)val).intValue();
    }

    public Object getValue() {
        return this.value;
    }

    public void setValue(Object v) {
        this.value = ((Integer)v).intValue();
    }
}