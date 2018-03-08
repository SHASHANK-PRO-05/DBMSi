package columnar;

import java.io.IOException;
import global.AttrType;
import global.SystemDefs;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;

public class ColumnarFile {

	public ColumnarHeader columnarHeader;
	public Heapfile heapFileNames[];
	static int numColumns;
	AttrType[] type;

	public ColumnarFile(String name, int numColumns, AttrType[] type)
			throws HFDiskMgrException, HFBufMgrException, HFException, IOException {

		columnarHeader = new ColumnarHeader(name, numColumns, type);

		for (int i = 0; i < numColumns; i++) {
			String fileNum = Integer.toString(i);
			String fileName = name + "." + fileNum;
			Heapfile f = new Heapfile(fileName);
			heapFileNames[i] = f;
		}

	}

	public void deleteColumnarFile() 
			throws InvalidSlotNumberException, 
			FileAlreadyDeletedException,
			InvalidTupleSizeException, 
			HFBufMgrException, 
			HFDiskMgrException, 
			IOException {

		deleteFileEntry(columnarHeader.hdrFile);
		for (int i = 0; i < numColumns; i++) {
			heapFileNames[i].deleteFile();
			deleteFileEntry(heapFileNames[i].toString());
		}

		deleteFileEntry(columnarHeader.hdrFile);

	}

	private void deleteFileEntry(String filename) 
			throws HFDiskMgrException {

		try {
			SystemDefs.JavabaseDB.deleteFileEntry(filename);
		} catch (Exception e) {
			throw new HFDiskMgrException(e, "Heapfile.java: delete_file_entry() failed");
		}

	}
}