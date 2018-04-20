package global;

public class StringValue extends ValueClass implements Comparable<StringValue>{

    String value;

 
    public StringValue(String val) {
        super(val);
        this.value = val;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(Object val) {
        this.value = (String) val;
    }

    @Override
    public int compare(ValueClass valueClass) {
        StringValue temp = (StringValue) valueClass;
        int res = this.getValue().compareTo(temp.getValue());
        if(res<0) {
        	return -1;
        }else if (res>0) {
        	return 1;
        }
        else 
        	return 0;
    }

    public boolean isequal(Object obj) {
        if (obj instanceof StringValue) {
            if (((String) ((StringValue) obj).getValue()).equals(this.getValue())) {
                return true;
            }
        }
        return false;
    }


	public int compareTo(StringValue o) {
		// TODO Auto-generated method stub
		return this.value.compareTo(o.value);
	}

	
    
    
}