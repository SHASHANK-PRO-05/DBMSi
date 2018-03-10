package global;

public class IntegerValue extends ValueClass {
    int value;

     public IntegerValue(int val) {
    	super(val);
        this.value = (val);
        
	}
 
    public Object getValue() {
        return this.value;
    }

    public void setValue(Object v) {
        this.value = ((Integer)v).intValue();
    }
    
    
    public boolean isequal(Object obj) {
   	 if(obj instanceof IntegerValue) {
   		//System.out.println((Integer)obj);
   		if(((IntegerValue)obj).getValue() == this.getValue()) {
   			return true;
   		}
	}
   
   	return false;
   	}
}