package global;

public class RangeIntValue extends ValueClass {
    int value1;
    int value2;

    //value1 must be smaller than value2

    public int getIntValue1() {
        return value1;
    }

    public void setIntValue1(int value1) {
        this.value1 = value1;
    }

    public int getIntValue2() {
        return value2;
    }

    public void setIntValue2(int value2) {
        this.value2 = value2;
    }
}
