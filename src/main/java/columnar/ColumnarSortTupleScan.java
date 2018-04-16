package columnar;

import java.io.IOException;

import bitmap.AddFileEntryException;
import bitmap.ConstructPageException;
import bitmap.GetFileEntryException;
import bitmap.PinPageException;
import bitmap.UnpinPageException;
import diskmgr.DiskMgrException;
import global.AttrType;
import global.Convert;
import global.RID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Tuple;
import iterator.ColumnSort;
import iterator.ColumnSortScan;
import iterator.ColumnarSort;

public class ColumnarSortTupleScan {

	private String heapfilename;
	Heapfile heapFile;
	private String order;
	private Heapfile heapfiles[];
	private ColumnSortScan scan;
	private int noOfColumns;
	private ColumnarFile columnarFile;
	private AttrType[] attrtypes;
	/**
	 * Column Index no in the Columnar File starting from 0
	 */
	private short columnNo;
	private ByteToTuple byteToTuple;

	public ColumnarSortTupleScan(String columnarfile, int columnNo, String order)
			throws InvalidTupleSizeException, Exception {
		columnarFile = new ColumnarFile(columnarfile);
		this.heapfilename = columnarfile;
		this.columnNo = (short) columnNo;
		this.order = order;
		noOfColumns = columnarFile.getHeapFileNames().length;
		scan = new ColumnSortScan(columnarfile, (short) columnNo, order);
		heapfiles = new Heapfile[noOfColumns];
		heapfiles = columnarFile.getHeapFileNames();
		attrtypes = columnarFile.getColumnarHeader().getColumns();
		byteToTuple = new ByteToTuple(attrtypes);
	}

	public Tuple getNext() throws columnar.InvalidTupleSizeException, InvalidTupleSizeException, IOException,
			ColumnarFilePinPageException, InvalidSlotNumberException, HFBufMgrException, ColumnarFileUnpinPageException,
			HFDiskMgrException {
		RID rid = new RID();
		Tuple tuple = new Tuple();
		tuple = scan.getNext(rid);
		int position = getPositionfromSortByte(tuple.getTupleByteArray());
		Tuple[] sortedtuples = new Tuple[noOfColumns];

		byte[] val = new byte[attrtypes[columnNo].getSize()];
		System.arraycopy(tuple.getTupleByteArray(), 0, val, 0, attrtypes[columnNo].getSize());
		tuple = new Tuple(val, 0, attrtypes[columnNo].getSize());
		sortedtuples[columnNo] = tuple;
		int size = 0;

		for (int i = 0; i < noOfColumns; i++) {
			size += attrtypes[i].getSize();
			if (i == columnNo)
				continue;
			sortedtuples[i] = heapfiles[i].getRecordAtPosition(position);
		}
		Tuple res = byteToTuple.mergeTuples(sortedtuples, size);

		return res;

	}

	int getPositionfromSortByte(byte[] record) throws IOException, ColumnarFilePinPageException,
			InvalidSlotNumberException, HFBufMgrException, ColumnarFileUnpinPageException {
		int size = columnarFile.getColumnInfo(columnNo).getSize();
		int pos = -1;
		pos = Convert.getIntValue(size, record);

		return pos;
	}

}
