package iterator;

import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import heap.Scan;
import heap.Tuple;

import sun.security.util.Length;

import java.io.IOException;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IndexFile;
import btree.IndexFileScan;
import btree.KeyDataEntry;
import btree.PinPageException;
import btree.StringKey;
import columnar.ByteToTuple;
import columnar.ColumnarFile;
import columnar.IndexInfo;
import global.AttrType;
import global.IndexType;


public class BtreeScan extends Iterator {
	private ColumnarFile columnarFile;
	private CondExpr[] condExprs;
	private FldSpec[] projList;
	private AttrType[] attrTypes;
	private short[] stringSizes;
	private int attrLength;
	private int nOutFields;
	private Tuple tuple;
	private Scan[] scan;
	private int tupleSize;
	private int[] columnNosArray;
	private IndexFile     indFile;
	private IndexFileScan indScan;
	private boolean indexOnly;
	ByteToTuple byteToTuple;
	CondExprEval condExprEval;
	

	public BtreeScan(String fileName, AttrType[] attrTypes, short stringSizes[], int attrLength, int nOutFields,
			FldSpec[] projList, CondExpr[] condExprs, boolean indexOnly)
			throws 
			ColumnarFileScanException, 
			HFBufMgrException, 
			InvalidSlotNumberException, 
			IOException, 
			GetFileEntryException, 
			PinPageException, 
			ConstructPageException {
		this.attrTypes = attrTypes;
		this.projList = projList;
		this.columnNosArray = new int[attrTypes.length];
		this.condExprs = condExprs;
		this.stringSizes = stringSizes;
		this.attrLength = attrLength;
		this.nOutFields = nOutFields;
		this.tuple = new Tuple();
		this.indexOnly = indexOnly;
		this.byteToTuple = new ByteToTuple(attrTypes);
		// Check if the columnar file exist
		try {
			columnarFile = new ColumnarFile(fileName);
		} catch (Exception e) {
			throw new ColumnarFileScanException(e, "Not able to create columnar file");
		}
		// check if the index exist
		IndexInfo indexinfo = columnarFile.getColumnarHeader().getIndex(attrTypes[attrLength - 2].getColumnId(),
				new IndexType(3));
		if (indexinfo == null) {
			//"Throws error or print the Btree does not exixst "
		}
		//Then we will open the index file.
		indFile = new BTreeFile(indexinfo.getFileName()); 
		//this.condExprEval = new CondExprEval(attrTypes, condExprs);

		for (int i = 0; i < attrTypes.length; i++)
			columnNosArray[i] = attrTypes[i].getColumnId();
		//initialize a scan
//		try {
//			indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
//		} catch (Exception e) {
//			throw new IndexException(e,
//					"IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
//		}

		

		// try {
		// scan = new Scan[attrLength];
		// for (int i = 0; i < attrLength; i++) {
		// scan[i] = new Scan(columnarFile, (short) columnNosArray[i]);
		// }
		//
		//
		// } catch (Exception e) {
		// throw new ColumnarFileScanException(e, "Not able to initiate scan");
		// }
	}

	public Tuple getNext() throws Exception {
		KeyDataEntry nextentry = null;
		
		nextentry = indScan.get_next();
		
		while (nextentry!=null) {
			if(indexOnly) {
				// only need to return the key
				// taking one attrtype
				// return the key inserting in the tuple
			}
			//take the individual rids and 
			//merge them and return tuple 

		}
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}
}
