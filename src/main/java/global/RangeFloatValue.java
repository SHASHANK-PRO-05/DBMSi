package global;

public class RangeFloatValue extends ValueClass{
    Float value1;
    Float value2;
    //value1 must be smaller than value2
    public Float getFloatValue1() {
        return value1;
    }

    public void setFloatValue1(Float value1) {
        this.value1 = value1;
    }

    public Float getFloatValue2() {
        return value2;
    }

    public void setFloatValue2(Float value2) {
        this.value2 = value2;
    }
}
