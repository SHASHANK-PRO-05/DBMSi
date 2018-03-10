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


	public abstract Object getValue();
    public abstract void setValue(Object o);
    

    public int getValueType() {
        if (this instanceof IntegerValue)
            return 1;
        else if (this instanceof StringValue)
            return 2;
        else
            return -1; //invalid type
        
    }
    public ValueClass(Object obj) {
    	
    	
    }
    
    public abstract boolean isequal(Object obj);

}