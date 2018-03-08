package global;

public class FloatValue extends ValueClass {
    float value;

    public FloatValue(float val) {
        super();
        this.value = val;
    }

    public FloatValue(Object val) {
        super();
        this.value = ((Float)val).floatValue();
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object v) {
        this.value =((Float)v).floatValue();
    }
    
    
}