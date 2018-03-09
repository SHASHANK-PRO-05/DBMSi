package columnar;

import java.io.IOException;

import global.Convert;
import global.GlobalConst;

public class TupleColumnar implements GlobalConst{



	    /**
	     * Maximum size of any tuple
	     */
	    public static final int max_size = MINIBASE_PAGESIZE;

	    /**
	     * a byte array to hold data
	     */
	    private byte[] data;

	    /**
	     * length of this tuple
	     */
	    private int tuple_length;

	    /**
	     * Class constructor
	     * Create a new tuple with length = max_size,tuple offset = 0.
	     */

	    public TupleColumnar() {
	        // Create a new tuple
	        data = new byte[max_size];
	        tuple_length = max_size;
	    }

	    /**
	     * Constructor
	     *
	     * @param atuple a byte array which contains the tuple
	     * @param offset the offset of the tuple in the byte array
	     * @param length the length of the tuple
	     */

	    public TupleColumnar(byte[] atuple, int length) {
	        data = atuple;
	        tuple_length = length;
	    }

	    /**
	     * Constructor(used as tuple copy)
	     *
	     * @param fromTuple a byte array which contains the tuple
	     */
	    public TupleColumnar(TupleColumnar fromTuple) {
	        data = fromTuple.getTupleByteArray();
	        tuple_length = fromTuple.getLength();
	        
	    }

	    /**
	     * Class constructor
	     * Creat a new tuple with length = size,tuple offset = 0.
	     */

	    public TupleColumnar(int size) {
	        // Creat a new tuple
	        data = new byte[size];
	        tuple_length = size;
	    }

	    /**
	     * Copy a tuple to the current tuple position
	     * you must make sure the tuple lengths must be equal
	     *
	     * @param fromTuple the tuple being copied
	     */
	    public void tupleCopy(TupleColumnar fromTuple) {
	        byte[] temparray = fromTuple.getTupleByteArray();
	        System.arraycopy(temparray, 0, data,0, tuple_length);
//	       fldCnt = fromTuple.noOfFlds(); 
//	       fldOffset = fromTuple.copyFldOffset(); 
	    }

	    /**
	     * This is used when you don't want to use the constructor
	     *
	     * @param atuple a byte array which contains the tuple
	     * @param offset the offset of the tuple in the byte array
	     * @param length the length of the tuple
	     */

	    public void tupleInit(byte[] atuple, int length) {
	        data = atuple;
	        tuple_length = length;
	    }

	    /**
	     * Set a tuple with the given tuple length and offset
	     *
	     * @param record a byte array contains the tuple
	     * @param offset the offset of the tuple ( =0 by default)
	     * @param length the length of the tuple
	     */
	    public void tupleSet(byte[] record, int length) {
	        System.arraycopy(record, 0, data, 0, length);
	        tuple_length = length;
	    }

	    /**
	     * get the length of a tuple, call this method if you did not
	     * call setHdr () before
	     *
	     * @return length of this tuple in bytes
	     */
	    public int getLength() {
	        return tuple_length;
	    }

	    
	    /**
	     * Copy the tuple byte array out
	     *
	     * @return byte[], a byte array contains the tuple
	     * the length of byte[] = length of the tuple
	     */

	    public byte[] getTupleByteArray() {
	        byte[] tuplecopy = new byte[tuple_length];
	        System.arraycopy(data,0, tuplecopy, 0, tuple_length);
	        return tuplecopy;
	    }

	    /**
	     * return the data byte array
	     *
	     * @return data byte array
	     */

	    public byte[] returnTupleByteArray() {
	        return data;
	    }

	    /**
	     * Convert this field into integer
	     *
	     * @param fldNo the field number
	     * @return the converted integer if success
	     * @throws IOException                    I/O errors
	     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
	     */

	    public int getIntFld() throws IOException {
	        int val;
	        
	            val = Convert.getIntValue(0, data);
	            return val;
	        
	    }

	    /**
	     * Convert this field in to float
	     *
	     * @param fldNo the field number
	     * @return the converted float number  if success
	     * @throws IOException                    I/O errors
	     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
	     */

	    public float getFloFld()
	            throws IOException{
	        float val;
	        
	            val = Convert.getFloatValue(0,data);
	            return val;
	        
	    }


	    /**
	     * Convert this field into String
	     *
	     * @param fldNo the field number
	     * @return the converted string if success
	     * @throws IOException                    I/O errors
	     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
	     */

	    public String getStrFld()
	            throws IOException{
	        String val;
	       
	            val = Convert.getStringValue(0, data,tuple_length); //strlen+2
	            return val;
	       
	    }

	    /**
	     * Convert this field into a character
	     *
	     * @param fldNo the field number
	     * @return the character if success
	     * @throws IOException                    I/O errors
	     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
	     */

	    public char getCharFld()
	            throws IOException {
	        char val;
	        
	            val = Convert.getCharValue(0, data);
	            return val;
	        

	    }

	    /**
	     * Set this field to integer value
	     *
	     * @param fldNo the field number
	     * @param val   the integer value
	     * @throws IOException                    I/O errors
	     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
	     */

	    public TupleColumnar setIntFld( int val)
	            throws IOException {
	       
	            Convert.setIntValue(val, 0, data);
	            return this;
	        
	    }

	    /**
	     * Set this field to float value
	     *
	     * @param fldNo the field number
	     * @param val   the float value
	     * @throws IOException                    I/O errors
	     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
	     */

	    public TupleColumnar setFloFld(float val)
	            throws IOException {
	         
	            Convert.setFloatValue(val,0, data);
	            return this;
	        

	    }

	    /**
	     * Set this field to String value
	     *
	     * @param fldNo the field number
	     * @param val   the string value
	     * @throws IOException                    I/O errors
	     * @throws FieldNumberOutOfBoundException Tuple field number out of bound
	     */

	    public TupleColumnar setStrFld(String val)
	            throws IOException{
	            Convert.setStringValue(val,0, data);
	            return this;
	        
	    }


	}



