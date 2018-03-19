package columnar;

import global.AttrType;
import heap.Tuple;

import java.util.ArrayList;

public class ByteToTuple {
    private int size;
    private int[] postion;
    
    public ByteToTuple() {
		// TODO Auto-generated constructor stub
	}

    public ByteToTuple(AttrType[] attrTypes) {
        //TODO: Check that we need the attrsize should
        //TODO: not exceed that what is there
        postion = new int[attrTypes.length];
        size = attrTypes[0].getSize();
        for (int i = 1; i < attrTypes.length; i++) {
            postion[i] = size;
            size += (attrTypes[i].getSize());
        }
    }

    public ArrayList<byte[]> setTupleBytes(byte[] bytePtr) {
        ArrayList<byte[]> arrayList = new ArrayList<byte[]>();
        for (int i = 0; i < postion.length - 1; i++) {
            byte[] bytes = new byte[postion[i + 1] - postion[i]];
            System.arraycopy(bytePtr, postion[i], bytes, 0, postion[i + 1] - postion[i]);
            arrayList.add(bytes);
        }
        byte[] bytes = new byte[bytePtr.length - postion[postion.length - 1]];
        System.arraycopy(bytePtr, postion[postion.length - 1],
                bytes, 0, bytePtr.length - postion[postion.length - 1]);
        arrayList.add(bytes);
        return arrayList;
    }
    

    /**
     * Private function that merges tuples from different columns into a single tuple
     *
     * @param tuples Tuples to be merged
     * @return Resultant tuple from the merge
     */
    public  Tuple mergeTuples(Tuple tuples[], int size) {
        byte[] insert = new byte[size];
        int pos = 0;
        for (int i = 0; i < tuples.length; i++) {
            System.arraycopy(tuples[i].getTupleByteArray()
                    , 0, insert, pos, tuples[i].getLength());
            pos += tuples[i].getLength();
        }
        return new Tuple(insert, 0, size);
    }
}
