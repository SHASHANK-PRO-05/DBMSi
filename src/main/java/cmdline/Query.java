package cmdline;

import bitmap.BitMapScanException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import columnar.ByteToTuple;
import columnar.ColumnarFile;
import global.*;
import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import heap.Tuple;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;

public class Query {
    private static String columnDBName;
    private static String columnarFileName;
    private static ArrayList<String> targetColumnNames;
    private static String operator;
    private static String value;
    private static String conditonalColumn;
    private static int numBuf;
    private static IndexType indexType;
    private static AttrType[] attrTypes;

    public static void main(String argv[])
            throws Exception {
        initFromArgs(argv);
    }

    /*
     *	Function to parse the arguments
     */
    private static void initFromArgs(String argv[])
            throws Exception {
        int lengthOfArgv = argv.length;
        targetColumnNames = new ArrayList<String>();
        columnDBName = argv[0];
        columnarFileName = argv[1];
        indexType = getIndexType(argv[lengthOfArgv - 1]);
        numBuf = Integer.parseInt(argv[lengthOfArgv - 2]);
        value = argv[lengthOfArgv - 3];
        operator = argv[lengthOfArgv - 4];
        conditonalColumn = argv[lengthOfArgv - 5];
        for (int i = 3; i <= lengthOfArgv - 7; i++) {
            targetColumnNames.add(argv[i]);
        }

        setUpFileScan();
    }

    /*
     * Function to get the values of the arguments and then call filescan
     */
    private static void setUpFileScan()
            throws Exception {
        AttrType[] in = new AttrType[targetColumnNames.size() + 1];
        AttrType[] projectionBreak = new AttrType[targetColumnNames.size()];
        FldSpec[] projList = new FldSpec[targetColumnNames.size()];

        SystemDefs systemDefs = new SystemDefs(columnDBName, 0, numBuf, "LRU");

        ColumnarFile columnarFile = new ColumnarFile(columnarFileName);
        attrTypes = columnarFile.getColumnarHeader().getColumns();


        for (int i = 0; i < in.length; i++) {
            if (i < in.length - 1)
                for (int j = 0; j < attrTypes.length; j++) {
                    if (targetColumnNames.get(i).equals(attrTypes[j].getAttrName())) {
                        in[i] = attrTypes[j];
                        projectionBreak[i] = attrTypes[j];
                        projList[i] = new FldSpec(new RelSpec(0)
                                , attrTypes[j].getColumnId());
                        break;
                    }
                }
            else {
                for (int j = 0; j < attrTypes.length; j++) {
                    if (conditonalColumn.equals(attrTypes[j].getAttrName())) {
                        in[i] = attrTypes[j];
                    }
                }
            }
        }

        CondExpr[] condition = new CondExpr[2];
        condition[1] = null;
        condition[0] = new CondExpr();
        condition[0].next = condition[1];
        condition[0].operand1.symbol = new FldSpec(new RelSpec(0), in[in.length - 1]
                .getColumnId());
        condition[0].op = parseOperator(operator);
        condition[0].type1 = new AttrType(AttrType.attrSymbol);
        condition[0].type2 = new AttrType(in[in.length - 1].getAttrType());


        if (in[in.length - 1].getAttrType() == 1)
            condition[0].operand2.integer = Integer.parseInt(value);
        else if (in[in.length - 1].getAttrType() == 0)
            condition[0].operand2.string = value;

        Iterator iterator = getIterator(in, null
                , in.length, projList.length, projList, condition);
        double startTime = System.currentTimeMillis();
        Tuple tuple = iterator.getNext();
        ByteToTuple byteToTuple = new ByteToTuple(projectionBreak);
        int position = 0;


        String ans2 = "Count";
        System.out.print(ans2);
        int temp2 = 25 - ans2.length();
        for (int j = 0; j < temp2; j++)
            System.out.print(" ");
        for (int i = 0; i < projectionBreak.length; i++) {
            String ans = projectionBreak[i].getAttrName();
            System.out.print(ans);
            int temp = 25 - ans.length();
            for (int j = 0; j < temp; j++)
                System.out.print(" ");

        }
        System.out.println();
        int counter = 1;
        while (tuple != null) {
            ArrayList<byte[]> tuples = byteToTuple.setTupleBytes(tuple.getTupleByteArray());
            String ans1 = counter + "";
            System.out.print(ans1);
            int temp1 = 25 - ans1.length();
            for (int j = 0; j < temp1; j++)
                System.out.print(" ");

            counter++;
            for (int i = 0; i < projectionBreak.length; i++) {
                if (projectionBreak[i].getAttrType() == AttrType.attrString) {
                    String ans = Convert.getStringValue(0, tuples.get(i), projectionBreak[i].getSize());
                    System.out.print(ans);
                    int temp = 25 - ans.length();
                    for (int j = 0; j < temp; j++)
                        System.out.print(" ");
                } else {
                    int ans = Convert.getIntValue(0, tuples.get(i));
                    System.out.print(ans);
                    int temp = 25 - (ans + "").length();
                    for (int j = 0; j < temp; j++)
                        System.out.print(" ");
                }
            }
            System.out.println();
            tuple = iterator.getNext();
        }
        iterator.close();
        SystemDefs.JavabaseBM.flushAllPages();

        System.out.println("--------------------------------------------------");
        double endTime = System.currentTimeMillis();
        double duration = (endTime - startTime);
        System.out.println("Time taken (Seconds)" + duration / 1000);
        System.out.println("Tuples in the table now:" + columnarFile.getTupleCount());
        System.out.println("Write count: " + SystemDefs.pCounter.getwCounter());
        System.out.println("Read count: " + SystemDefs.pCounter.getrCounter());
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
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