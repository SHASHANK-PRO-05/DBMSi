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
    ValueClass() {

    }

    public int getValueType() {
        if (this instanceof IntegerValue)
            return 1;
        else if (this instanceof StringValue)
            return 2;
        else if (this instanceof FloatValue)
            return 3;
        else if (this instanceof RangeIntValue)
            return 4;
        else if (this instanceof RangeStringValue)
            return 5;
        else if (this instanceof RangeFloatValue)
            return 6;
        else
            return -1; //invalid type

    }

}