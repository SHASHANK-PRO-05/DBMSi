package global;

/*Value Class
Dependencies:
1.ColumnarFile
a. ValueClass getValue(TID tid, column)
Read the value with the given column and tid from the columnar file.
b. boolean createBitMapIndex(int columnNo, ValueClass value)
If it doesn't exist, create a bitmap index for the given column and value.
2. BitMapFile
Constructor Parameter - An index file with given file name should not already exist; this creates the BitMapFile from scratch.
*/


public abstract class ValueClass {


    public abstract Object getValue();

    public abstract void setValue(Object o);

    public abstract int compare(ValueClass valueClass);

    public int getValueType() {
        if (this instanceof IntegerValue)
            return 1;
        else if (this instanceof StringValue)
            return 0;
        else
            return -1; //invalid type

    }

    public ValueClass(Object obj) {


    }

    @Override
    public boolean equals(Object obj) {
        ValueClass temp = (ValueClass) obj;
        return this.getValue().equals(temp.getValue());
    }

    @Override
    public int hashCode() {
        return this.getValue().hashCode();
    }

    public abstract boolean isequal(Object obj);
    
    public int  cmp(ValueClass val) {
    	int result = 0;
    	if(this.getValueType() == val.getValueType() && this.getValueType() == 1) {
    		return this.compare((IntegerValue)val);
    	}else if(this.getValueType() == val.getValueType() && this.getValueType() ==0) {
    		return this.compare((StringValue)val);
    	}
    	
		return 0;
    	
    }
    
    


}