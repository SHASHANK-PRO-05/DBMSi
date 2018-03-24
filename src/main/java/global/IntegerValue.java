package global;

public class IntegerValue extends ValueClass {
    int value;

    public IntegerValue(int val) {
        super(val);
        this.value = (val);

    }

    @Override
    public int compare(ValueClass valueClass) {
        IntegerValue init = (IntegerValue) valueClass;

        if (value < (Integer) init.getValue()) {
            return -1;
        }

        if (value == (Integer) init.getValue()) {
            return 0;
        }

        return 1;
    }

    public Object getValue() {
        return this.value;
    }

    public void setValue(Object v) {
        this.value = ((Integer) v).intValue();
    }


    public boolean isequal(Object obj) {
        if (obj instanceof IntegerValue) {
            //System.out.println((Integer)obj);
            if (((IntegerValue) obj).getValue() == this.getValue()) {
                return true;
            }
        }

        return false;
    }
}