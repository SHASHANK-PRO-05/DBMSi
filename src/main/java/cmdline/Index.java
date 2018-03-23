package cmdline;

import bitmap.BitMapFile;
import bitmap.GetFileEntryException;
import btree.*;
import columnar.ColumnarFile;
import columnar.IndexInfo;
import columnar.TupleScan;
import global.*;
import heap.Scan;
import heap.Tuple;

import java.util.HashSet;
import java.util.Set;

public class Index {
    private static String columnDBName;
    private static String columnarFileName;
    private static int columnId;
    private static String indexMethod;
    private static String columnName;
    private static ColumnarFile columnarFile;
    private static AttrType[] attrTypes;
    private static AttrType attrType;
    private static IndexInfo indexInfo = new IndexInfo();

    public static void main(String argv[]) throws Exception {
        initFromArgs(argv);
    }
    
    private static void initFromArgs(String argv[]) throws Exception {
        columnDBName = argv[0];
        columnarFileName = argv[1];
        columnName = argv[2];
        indexMethod = argv[3];

        SystemDefs systemDefs = new SystemDefs(columnDBName, 0, 4000, "LRU");


        columnarFile = new ColumnarFile(columnarFileName);

        attrTypes = columnarFile.getColumnarHeader().getColumns();
        int columnCount = attrTypes.length;
        for (int i = 0; i < columnCount; i++) {
            if (attrTypes[i].getAttrName().equals(columnName)) {
                attrType = attrTypes[i];
                columnId = i;
                break;
            }
        }
        setupIndex();
    }

    private static void setupIndex() throws Exception {
        long count = columnarFile.getTupleCount();
        Scan scan = new Scan(columnarFile, (short) columnId);

        indexInfo.setColumnNumber(columnId);
        IndexType indexType = null;
        if (indexMethod.equals("BITMAP")) {
            indexType = new IndexType(3);
            indexInfo.setIndexType(indexType);
            setupBitMapIndexes(scan);
        } else if (indexMethod.equals("BTREE")) {
            indexType = new IndexType(2);
            indexInfo.setIndexType(indexType);
            setupBTreeIndexes(scan);
        }
        scan.closeScan();
        SystemDefs.JavabaseBM.flushAllPages();
    }
    

    private static void setupBTreeIndexes(Scan scan) throws Exception {
        String fileName = columnarFileName + "." + columnId + ".btree";
        BTreeFile bTreeFile = new BTreeFile(fileName, attrType.getAttrType(), attrType.getSize(), 1);
        int pos = 0;
        int colcount = columnarFile.getNumColumns();
        RID[] rids = new RID[colcount];
        for(int i =0 ;i<colcount;i++)rids[i]=new RID();
        TID tid = new TID (colcount,0,rids);
        TupleScan tupleScan = new TupleScan(columnarFile);
        Tuple tuple = tupleScan.getNext(tid);
        ValueClass valueClass;
        KeyClass keyClass;
        
        while (tuple != null) {
            if (attrType.getAttrType() == 0) {
                valueClass = new StringValue(Convert
                        .getStringValue(0, tuple.getTupleByteArray(), attrType.getSize()));
                keyClass = new StringKey((String) valueClass.getValue());
            } else {
                valueClass = new IntegerValue(Convert.getIntValue(0, tuple.getTupleByteArray()));
                keyClass = new IntegerKey((Integer) valueClass.getValue());
            }
            bTreeFile.insert(keyClass, tid);
            pos++;
            tid.setPosition(pos);
            tuple = tupleScan.getNext(tid);
        }
        BT.printAllLeafPages(bTreeFile.getHeaderPage());
        bTreeFile.close();
    }
    
    
    
    private static void setupBitMapIndexes(Scan scan) throws Exception {

        //What type of of unique values are required
        Set uniqueClass;
        if (attrType.getAttrType() == 0) {
            uniqueClass = new HashSet<StringValue>();
        } else {
            uniqueClass = new HashSet<IntegerValue>();
        }

        RID rid = new RID();
        Tuple tuple = scan.getNext(rid);

        while (tuple != null) {
            ValueClass valueClass;
            if (attrType.getAttrType() == 0) {
                valueClass = new StringValue(Convert
                        .getStringValue(0, tuple.getTupleByteArray(), attrType.getSize()));
            } else {
                valueClass = new IntegerValue(Convert
                        .getIntValue(0, tuple.getTupleByteArray()));
            }
            uniqueClass.add(valueClass);
            tuple = scan.getNext(rid);
        }
        System.out.println(uniqueClass.size());
        for (Object valueClass : uniqueClass) {
            String indexFileName = columnarFileName + "." + columnId + ".";
            if (attrType.getAttrType() == 0) {
                StringValue stringValue = (StringValue) valueClass;
                indexFileName = indexFileName + stringValue.getValue();
                new BitMapFile(indexFileName, columnarFile, columnId, stringValue);
                indexInfo.setValue(stringValue);
                indexInfo.setFileName(indexFileName);
                columnarFile.getColumnarHeader().setIndex(indexInfo);
            } else {
                IntegerValue integerValue = (IntegerValue) valueClass;
                indexFileName = indexFileName + integerValue.getValue();
                new BitMapFile(indexFileName, columnarFile, columnId, integerValue);
                indexInfo.setValue(integerValue);
                indexInfo.setFileName(indexFileName);
                columnarFile.getColumnarHeader().setIndex(indexInfo);
            }
        }

    }

    

}