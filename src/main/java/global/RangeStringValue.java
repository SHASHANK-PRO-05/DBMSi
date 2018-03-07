package global;

public class RangeStringValue extends ValueClass {
    String value1;
    String value2;

    //value1 must be smaller than value2


    public String getStringValue1() {
        return value1;
    }

    public void setStringValue1(String value1) {
        this.value1 = value1;
    }

    public String getStringValue2() {
        return value2;
    }

    public void setStringValue2(String value2) {
        this.value2 = value2;
    }
}
