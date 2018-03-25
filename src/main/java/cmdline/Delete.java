package cmdline;

import columnar.ByteToTuple;
import columnar.ColumnarFile;
import columnar.TupleScan;
import global.*;
import heap.Tuple;
import iterator.CondExpr;
import iterator.CondExprEval;
import iterator.FldSpec;
import iterator.RelSpec;

import java.util.ArrayList;

public class Delete {
    private static String columnDBName;
    private static String columnarFileName;
    private static String operator;
    private static String value;
    private static String conditionalColumn;
    private static int numBuf;
    private static IndexType indexType;
    private static ColumnarFile columnarFile;
    private static boolean purgeDB;
    private static AttrType[] attrTypes;

    public static void main(String argv[]) throws Exception {
        initFromArgs(argv);

        if (indexType.toString().equals("ColumnScan")) {
            deleteUsingColumnarFileScan();
        }
    }

    private static void initFromArgs(String argv[]) throws Exception {

        if (argv.length != 8) {
            throw new Exception("There should be 8 arguments to this program");
        }

        columnDBName = argv[0];
        columnarFileName = argv[1];
        conditionalColumn = argv[2];
        operator = argv[3];
        value = argv[4];
        numBuf = Integer.parseInt(argv[5]);
        indexType = getIndexType(argv[6]);
        purgeDB = Boolean.parseBoolean(argv[7]);

        SystemDefs systemDefs = new SystemDefs(columnDBName, 0, numBuf, "LRU");

        columnarFile = new ColumnarFile(columnarFileName);

        SystemDefs.JavabaseBM.flushAllPages();
    }

    /*
     * Function to get the values of the arguments and then call filescan
     */
    private static void deleteUsingColumnarFileScan()
        throws Exception {

        int conditionalColumnId = -1;
        AttrType condAttr = new AttrType();

        attrTypes = columnarFile.getColumnarHeader().getColumns();
        int[] columnsToScan = new int[attrTypes.length];

        int columnCount = attrTypes.length;

        for (int i = 0; i < columnCount; i++) {
            columnsToScan[i] = attrTypes[i].getColumnId();
            if (attrTypes[i].getAttrName().equals(conditionalColumn)) {
                condAttr = attrTypes[i];
                conditionalColumnId = attrTypes[i].getColumnId();
            }
        }

        CondExpr[] condition = new CondExpr[2];
        condition[1] = null;
        condition[0] = new CondExpr();
        condition[0].next = condition[1];
        condition[0].type1 = new AttrType(AttrType.attrSymbol);
        condition[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), conditionalColumnId);
        condition[0].op = parseOperator(operator);
        condition[0].type2 = new AttrType(condAttr.getAttrType());
        if (condAttr.getAttrType() == 1)
            condition[0].operand2.integer = Integer.parseInt(value);
        else if (condAttr.getAttrType() == 0)
            condition[0].operand2.string = value;

        CondExprEval condExprEval = new CondExprEval(attrTypes, condition);

        TupleScan tupleScan = new TupleScan(columnarFile, columnsToScan);
        RID[] rids = new RID[attrTypes.length];

        for (int i = 0; i < attrTypes.length; i++) {
            rids[i] = new RID();
        }

        TID tid = new TID(attrTypes.length, 0, rids);

        Tuple tuple = tupleScan.getNext(tid);
        ByteToTuple byteToTuple = new ByteToTuple(attrTypes);

        for (AttrType aProject : attrTypes) {
            System.out.print(aProject.getAttrName() + "\t");
        }

        System.out.print("\n");

        int position = 0;
        int totalRecordsDeleted = 0;

        while (tuple != null) {
            ArrayList<byte[]> arrayList = byteToTuple.setTupleBytes(tuple.getTupleByteArray());
            if (condExprEval.isValid(arrayList)) {
                columnarFile.markTupleDeleted(new TID(columnarFile.getNumColumns(), position));
                totalRecordsDeleted += 1;
            }

            tuple = tupleScan.getNext(tid);
            position++;
        }


        tupleScan.closeTupleScan();

        if (totalRecordsDeleted > 0) {
            SystemDefs.JavabaseBM.pinPage(columnarFile.getColumnarHeader().getHeaderPageId(), columnarFile.getColumnarHeader(), false);
            columnarFile.getColumnarHeader().setReccnt(columnarFile.getColumnarHeader().getReccnt() - totalRecordsDeleted);
            SystemDefs.JavabaseBM.unpinPage(columnarFile.getColumnarHeader().getHeaderPageId(), true);
        }

        if (purgeDB) columnarFile.purgeAllDeletedTuples();

        SystemDefs.JavabaseBM.flushAllPages();
    }

    /*
     * Function to parse the Index type
     */
    private static IndexType getIndexType(String indexName) {
        if (indexName.equals("FILESCAN"))
            return new IndexType(0);
        if (indexName.equals("COLUMNSCAN"))
            return new IndexType(4);
        if (indexName.equals("BTREE"))
            return new IndexType(1);
        if (indexName.equals("BITMAP"))
            return new IndexType(3);
        else
            return null;
    }
    /*
     * Functions to parse the operators
     */

    private static AttrOperator parseOperator(String operator) {

        if (operator.equals("=="))
            return new AttrOperator(0);
        if (operator.equals("<"))
            return new AttrOperator(1);
        if (operator.equals(">"))
            return new AttrOperator(2);
        if (operator.equals("!="))
            return new AttrOperator(3);
        if (operator.equals("<="))
            return new AttrOperator(4);
        if (operator.equals(">="))
            return new AttrOperator(5);
        else
            return null;
    }
}

