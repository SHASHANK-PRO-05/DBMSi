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


public abstract class ValueClass{
    private Object obj;
	
	public ValueClass() {
    	
    	
    }
	
	
	
    public Object getObj() {
		return obj;
	}



	public void setObj(Object obj) {
		this.obj = obj;
	}



	public abstract Object getValue();
    public abstract void setValue(Object o);

    public int getValueType() {
        if (this instanceof IntegerValue)
            return 1;
        else if (this instanceof StringValue)
            return 2;
        else if (this instanceof FloatValue)
            return 3;
        else
            return -1; //invalid type

    }
    
    
    public boolean isequal(Object obj) {
    	if(obj instanceof IntegerValue) {
    		if((Integer)obj == (Integer)getObj()) {
    			return true;
    		}
    	}
    	
    	else if(obj instanceof StringValue) {
    		if((Integer)obj == (Integer)getObj()) {
    			return true;
    		}
    	}
    	
    	else if(obj instanceof StringValue) {
    		if((Integer)obj == (Integer)getObj()) {
    			return true;
    		}
    	}
    	return false;
    }

}