package cmdline;

import bitmap.BitMapScanException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import columnar.ColumnarFile;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import iterator.*;

import java.io.IOException;

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
        deleteSetup();

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
    private static void deleteSetup()
            throws Exception {
        int conditionalColumnId = -1;
        AttrType conditionAttr = null;
        AttrType[] in = new AttrType[1];
        attrTypes = columnarFile.getColumnarHeader().getColumns();
        FldSpec[] projList = new FldSpec[1];

        for (int i = 0; i < attrTypes.length; i++) {
            if (attrTypes[i].getAttrName().equals(conditionalColumn)) {
                conditionAttr = attrTypes[i];
                conditionalColumnId = i;
                break;
            }
        }
        projList[0] = new FldSpec(new RelSpec(0)
                , conditionAttr.getColumnId());
        in[0] = conditionAttr;
        if (conditionalColumnId == -1) {
            throw new Exception("No column found with that name");
        }

        CondExpr[] condition = new CondExpr[2];
        condition[1] = null;
        condition[0] = new CondExpr();
        condition[0].next = condition[1];
        condition[0].type1 = new AttrType(AttrType.attrSymbol);
        condition[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), conditionalColumnId);
        condition[0].op = parseOperator(operator);
        condition[0].type2 = new AttrType(conditionAttr.getAttrType());
        if (conditionAttr.getAttrType() == 1)
            condition[0].operand2.integer = Integer.parseInt(value);
        else if (conditionAttr.getAttrType() == 0)
            condition[0].operand2.string = value;
        SystemDefs.JavabaseBM.flushAllPages();
        Iterator iterator = getIterator(in, null
                , in.length, projList.length, projList, condition);
        int nextPos = iterator.getNextPosition();
        while (nextPos != -1) {
            columnarFile.markTupleDeleted(nextPos);
            nextPos = iterator.getNextPosition();
        }
        iterator.close();
        SystemDefs.JavabaseBM.flushAllPages();
    }

    private static Iterator getIterator(AttrType[] in, short[] strSizes, int counterIn, int counterFld, FldSpec[] projList, CondExpr[] condition)
            throws ColumnarFileScanException,
            HFBufMgrException,
            InvalidSlotNumberException,
            GetFileEntryException,
            PinPageException,
            ConstructPageException,
            IOException,
            IndexException,
            BitMapScanException,
            Exception {
        Iterator iter = null;
        if (indexType.indexType == IndexType.None) {
            iter = new ColumnarFileScan(columnarFileName, in, strSizes, counterIn, counterFld, projList, condition);
        }
        if (indexType.indexType == IndexType.ColumnScan) {
            iter = new ColumnScan(columnarFileName, in, strSizes, counterIn, counterFld, projList, condition);
        }
        if (indexType.indexType == IndexType.B_Index) {
            boolean indexOnly = false;
            if (counterIn == 2 && in[0].getAttrName().equals(in[1].getAttrName()))
                indexOnly = true;
            iter = new BtreeScan(columnarFileName, in, strSizes, counterIn, counterFld, projList, condition, indexOnly);
        }
        if (indexType.indexType == IndexType.BitMapIndex) {
            iter = new BitmapScan(columnarFileName, in, strSizes, counterIn, counterFld, projList, condition);
        }
        return iter;
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

