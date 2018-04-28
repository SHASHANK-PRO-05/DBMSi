package cmdline;

import bitmap.BitMapFile;
import bitmap.GetFileEntryException;
import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyClass;
import btree.StringKey;
import columnar.ColumnarFile;
import columnar.IndexInfo;
import columnar.TupleScan;
import global.*;
import heap.Scan;
import heap.Tuple;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class Index {
    private String columnDBName;
    private String columnarFileName;
    private int columnId;
    private String indexMethod;
    private String columnName;
    private ColumnarFile columnarFile;
    private AttrType[] attrTypes;
    private AttrType attrType;
    private IndexInfo indexInfo = new IndexInfo();

    public static void main(String argv[]) throws Exception {
        if (argv.length != 4) {
            System.out.println("--- Usage of the command ---");
            System.out.println("index COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE");
        } else {
            Index index = new Index();
            index.initFromArgs(argv);
        }
    }

    private void initFromArgs(String argv[]) throws Exception {
        columnDBName = argv[0];

        // The file does not exists
        if (!(new File(columnDBName).isFile())) {
            throw new Exception("The DB does not exists");
        }
        SystemDefs systemDefs = new SystemDefs(columnDBName, 0, 4000, "LRU");
        columnarFileName = argv[1];
        if (getFileEntry(columnarFileName) == null)
            throw new Exception("The specified table does not exists");
        columnName = argv[2];
        indexMethod = argv[3];
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

        if (!indexMethod.equals("BTREE") && !indexMethod.equals("BITMAP"))
            throw new Exception("Only BTREE and BITMAP indexing allowed");


        if (attrType == null)
            throw new Exception("The specified column does not exists in the table");


        setupIndex();
    }

    private void setupIndex() throws GetFileEntryException, java.lang.Exception {
        long count = columnarFile.getTupleCount();

        Scan scan = new Scan(columnarFile, (short) columnId);

        indexInfo.setColumnNumber(columnId);
        IndexType indexType = null;
        double startTime = System.currentTimeMillis();
        if (indexMethod.equals("BITMAP")) {
            indexType = new IndexType(3);
            indexInfo.setIndexType(indexType);
            setupBitMapIndexes(scan);
        } else if (indexMethod.equals("BTREE")) {
            indexType = new IndexType(1);
            indexInfo.setIndexType(indexType);
            setupBTreeIndexes(scan);
        }
        scan.closeScan();

        SystemDefs.JavabaseBM.flushAllPages();
        double endTime = System.currentTimeMillis();
        double duration = (endTime - startTime);
        System.out.println("Time taken (Seconds)" + duration / 1000);
        System.out.println("Tuples in the table now:" + columnarFile.getColumnarHeader().getReccnt());
        System.out.println("Write count: " + SystemDefs.pCounter.getwCounter());
        System.out.println("Read count: " + SystemDefs.pCounter.getrCounter());
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
    }


    private void setupBTreeIndexes(Scan scan) throws java.lang.Exception {
        String fileName = columnarFileName + "." + columnId + ".btree";
        BTreeFile bTreeFile = new BTreeFile(fileName, attrType.getAttrType(), attrType.getSize(), 1);
        int pos = 0;
        AttrType attrTypes[] = columnarFile.getColumnarHeader().getColumns();
        int colcount = attrTypes.length;
        RID[] rids = new RID[colcount];
        int[] posArray = new int[colcount];
        int position = 0;
        for (int i = 0; i < colcount; i++) {
            rids[i] = new RID();
            posArray[i] = position;
            position = position + attrTypes[i].getSize();
        }
        TID tid = new TID(colcount, 0, rids);
        TupleScan tupleScan = new TupleScan(columnarFile);
        Tuple tuple = tupleScan.getNext(tid);
        ValueClass valueClass = null;
        KeyClass keyClass;

        while (tuple != null) {
            if (attrType.getAttrType() == 0) {
                valueClass = new StringValue(Convert
                        .getStringValue(posArray[attrType.getColumnId()], tuple.getTupleByteArray(), attrType.getSize()));
                keyClass = new StringKey((String) valueClass.getValue());
            } else {
                valueClass = new IntegerValue(Convert.getIntValue(posArray[attrType.getColumnId()], tuple.getTupleByteArray()));
                keyClass = new IntegerKey((Integer) valueClass.getValue());
            }
            //System.out.println(valueClass.getValue());
            bTreeFile.insert(keyClass, tid);
            pos++;
            tid.setPosition(pos);
            tuple = tupleScan.getNext(tid);
        }
        bTreeFile.close();
        tupleScan.closeTupleScan();
        indexInfo.setValue(valueClass);
        indexInfo.setFileName(fileName);

        columnarFile.getColumnarHeader().setIndex(indexInfo);
    }


    private void setupBitMapIndexes(Scan scan) throws Exception {

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
                try {
                    new BitMapFile(indexFileName, columnarFile, columnId, integerValue);
                } catch (Exception e) {
                    System.out.println("Either Index already Exists/ Or Something is wrong");
                    return;
                }
                indexInfo.setValue(integerValue);
                indexInfo.setFileName(indexFileName);
                columnarFile.getColumnarHeader().setIndex(indexInfo);
            }
        }

    }

    private PageId getFileEntry(String fileName) throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.getFileEntry(fileName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }
}