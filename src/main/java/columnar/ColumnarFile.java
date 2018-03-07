package columnar;

import java.io.IOException;
import global.AttrType;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
public class ColumnarFile {

	public ColumnarHeader columnarHeader;
	public Heapfile heapFileNames[];
	static int numColumns;
	AttrType[] type;
	
	public ColumnarFile(String name, int numColumns, AttrType[] type) throws HFDiskMgrException, HFBufMgrException, HFException, IOException {
		
		columnarHeader = new ColumnarHeader(name, numColumns, type);
		
		for(int i=0;i<numColumns;i++) {
			String fileNum = Integer.toString(i);
			String fileName = name + "." + fileNum;
			Heapfile f = new Heapfile(fileName);
			heapFileNames[i]=f;
		}
		
		
	}
	
}
