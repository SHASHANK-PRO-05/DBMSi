package iterator;

import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import heap.Scan;
import heap.Tuple;

import java.io.IOException;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IndexFile;
import btree.IndexFileScan;
import btree.IntegerKey;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.PinPageException;
import btree.StringKey;
import columnar.ByteToTuple;
import columnar.ColumnarFile;
import columnar.IndexInfo;
import global.AttrType;
import global.Convert;
import global.IndexType;
import global.TID;


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
			ConstructPageException,
			IndexException {
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
		IndexInfo indexinfo = columnarFile.getColumnarHeader().getIndex(attrTypes[attrLength - 1].getColumnId(),
				new IndexType(3));
		if (indexinfo == null) {
			//"Throws error or print the Btree does not exixst "
		}
		//Then we will open the index file.
		indFile = new BTreeFile("Employee.1.btree");
		//this.condExprEval = new CondExprEval(attrTypes, condExprs);

		for (int i = 0; i < attrTypes.length; i++)
			columnNosArray[i] = attrTypes[i].getColumnId();
	
		try {
			indScan = (BTFileScan) BtreeUtils.BTree_scan(condExprs, indFile);
		} catch (Exception e) {
			throw new IndexException(e,
					"IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
		}
	}

	public Tuple getNext() throws Exception {
		KeyDataEntry nextentry = null;
		Tuple tuple;
		nextentry = indScan.get_next();
		TID tid;
		while (nextentry!=null) {
			if(indexOnly) {
				int size  = attrTypes[0].getSize();
				byte[] byteArray = new byte[size];
				if(attrTypes[0].getAttrType()== AttrType.attrInteger)
					Convert.setIntValue(((IntegerKey) nextentry.key).getKey().intValue(), 0, byteArray);
				else if(attrTypes[0].getAttrType() == AttrType.attrString)
					Convert.setStringValue(((StringKey) nextentry.key).getKey(), 0, byteArray);
				tuple = new Tuple(byteArray,0,size);
				return tuple;
				
			}
			tid = ((LeafData) nextentry.data).getData();
			System.out.println(tid.getRecordIDs()[0].slotNo);
			
			
			
			
			
			
			
			
			
			
			
			
			
			nextentry = indScan.get_next();
			
			
		}
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}
}
