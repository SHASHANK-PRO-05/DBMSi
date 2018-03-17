package cmdline;

import bitmap.BitMapFile;
import bitmap.GetFileEntryException;
import columnar.ColumnarFile;
import columnar.IndexInfo;
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
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.setColumnNumber(columnId);
        IndexType indexType = null;
        if (indexMethod.equals("BITMAP")) {
            indexType = new IndexType(3);
            indexInfo.setIndexType(indexType);
            setupBitMapIndexes(indexInfo, scan);
        } else if (indexMethod.equals("BTREE")) {
            indexType = new IndexType(2);
            indexInfo.setIndexType(indexType);
        }
        scan.closeScan();
        SystemDefs.JavabaseBM.flushAllPages();
    }

    private static void setupBitMapIndexes(IndexInfo indexInfo, Scan scan) throws Exception {

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
            } else {
                IntegerValue integerValue = (IntegerValue) valueClass;
                indexFileName = indexFileName + integerValue.getValue();
                new BitMapFile(indexFileName, columnarFile, columnId, integerValue);
            }
        }

    }

    private static PageId getFileEntry(String fileName) throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.getFileEntry(fileName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

}