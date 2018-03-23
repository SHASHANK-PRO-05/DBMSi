package iterator;

import heap.HFBufMgrException;
import heap.HFPage;
import heap.Heapfile;
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
import diskmgr.Page;
import global.AttrType;
import global.Convert;
import global.IndexType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import global.TID;

public class BtreeScan extends Iterator {
	private ColumnarFile columnarFile;
	private CondExpr[] condExprs;
	private FldSpec[] projList;
	private AttrType[] attrTypes;
	private short[] stringSizes;
	private int attrLength;
	private int nOutFields;
	private int[] columnNosArray;
	private IndexFile indFile;
	private IndexFileScan indScan;
	private boolean indexOnly;
	ByteToTuple byteToTuple;
	CondExprEval condExprEval;

	public BtreeScan(String fileName, AttrType[] attrTypes, short stringSizes[], int attrLength, int nOutFields,
			FldSpec[] projList, CondExpr[] condExprs, boolean indexOnly)
			throws ColumnarFileScanException, 
			HFBufMgrException, 
			InvalidSlotNumberException, 
			IOException,
			GetFileEntryException, PinPageException, ConstructPageException, IndexException {
		this.attrTypes = attrTypes;
		this.projList = projList;
		this.columnNosArray = new int[attrTypes.length];
		this.condExprs = condExprs;
		this.stringSizes = stringSizes;
		this.attrLength = attrLength;
		this.nOutFields = nOutFields;
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
		try {
			indFile = new BTreeFile("Employee.1.btree");
		}catch(Exception e) {
			throw new GetFileEntryException(null,"Btree Index does not exist");
		}
		// this.condExprEval = new CondExprEval(attrTypes, condExprs);

		for (int i = 0; i < attrTypes.length; i++)
			columnNosArray[i] = attrTypes[i].getColumnId();

		try {
			indScan = (BTFileScan) BtreeUtils.BTree_scan(condExprs, indFile);
		} catch (Exception e) {
			throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
		}
	}

	public Tuple getNext() throws Exception {
		KeyDataEntry nextentry = null;
		Tuple tuple;
		nextentry = indScan.get_next();
		TID tid;
		while (nextentry != null) {
			if (indexOnly) {
				int size = attrTypes[0].getSize();
				byte[] byteArray = new byte[size];
				if (attrTypes[0].getAttrType() == AttrType.attrInteger)
					Convert.setIntValue(((IntegerKey) nextentry.key).getKey().intValue(), 0, byteArray);
				else if (attrTypes[0].getAttrType() == AttrType.attrString)
					Convert.setStringValue(((StringKey) nextentry.key).getKey(), 0, byteArray);
				tuple = new Tuple(byteArray, 0, size);
				return tuple;

			}
			Tuple[] tuples = new Tuple[projList.length];
			tid = ((LeafData) nextentry.data).getData();
			int size = 0;
			RID [] rid = new RID[projList.length];
			
			for (int i = 0; i < projList.length; i++) {
				rid[i] = tid.getRecordIDs()[i];
				HFPage pg = new HFPage();
				pinPage(rid[i].pageNo,pg);
				tuples[i] = pg.returnRecord(rid[i]);
				unpinPage(rid[i].pageNo, true);
				size = size + attrTypes[i].getSize();
				
			}
			return byteToTuple.mergeTuples(tuples, size);

		}
		return null;
	}

	@Override
	public void close() throws IOException, iterator.IndexException {
		// TODO Auto-generated method stub
		if (!closeFlag) {
			if (indScan instanceof BTFileScan) {
				try {
					((BTFileScan) indScan).DestroyBTreeFileScan();
				} catch (Exception e) {
					throw new IndexException(e, "BTree error in destroying index scan.");
				}
			}

			closeFlag = true;
		}

	}
	
	
	private void unpinPage(PageId pageno, boolean dirty) throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: in Column Header, unpinPage() failed");
        }

    }

    private void pinPage(PageId pageId, Page page) throws HFBufMgrException {
        try {
            SystemDefs.JavabaseBM.pinPage(pageId, page, false);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: in Column Header, pinPage() failed");
        }
    }
}
