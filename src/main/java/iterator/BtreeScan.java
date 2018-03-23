package iterator;

import btree.*;
import columnar.ByteToTuple;
import columnar.ColumnarFile;
import columnar.IndexInfo;
import global.AttrType;
import global.Convert;
import global.IndexType;
import global.TID;
import heap.*;

import java.io.IOException;


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
    private IndexFile indFile;
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


            for (int i = 0; i < projList.length; i++) {
                Heapfile heapfile = columnarFile.getHeapFileNames()[projList[i].offset];
                tuples[i] = heapfile.getRecordAtPosition(tid.getPosition());
                size = size + attrTypes[i].getSize();
            }
            return byteToTuple.mergeTuples(tuples, size);
        }
        return null;
    }


    @Override
    public void close() throws IOException, iterator.IndexException {
        // TODO Auto-generated method stub
        try {
            ((BTreeFile) indFile).close();
        } catch (Exception e) {

        }
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
}
