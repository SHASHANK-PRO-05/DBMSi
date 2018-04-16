package cmdline;

import bitmap.AddFileEntryException;
import bitmap.UnpinPageException;
import columnar.ColumnarFile;
import columnar.ColumnarFileDoesExistsException;
import columnar.ColumnarFilePinPageException;
import columnar.ColumnarFileUnpinPageException;
import diskmgr.DiskMgrException;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ColumnarNestedLoopJoin {

    private static String columnDBName;
    // private static String columnarFileName;
    private static String outerFile;
    private static String innerFile;
    private static ArrayList<String> targetColumnNames;
    private static StringBuffer outerConst = new StringBuffer();
    private static StringBuffer innerConst = new StringBuffer();
    private static StringBuffer joinConst = new StringBuffer();
    private static int numBuf;
    private static IndexType indexType;
    private static AttrType[] in1;
    private static AttrType[] in2;
    private static CondExpr[] outerCondExpr;
    private static CondExpr[] innerCondExpr;
    private static CondExpr[] joinCondExpr;
    int c = 0;

    public static void main(String argv[]) throws Exception {
        initFromArgs(argv);
    }

    /*
     * Function to parse the arguments
     */
    private static void initFromArgs(String argv[]) throws Exception {
        int lengthOfArgv = argv.length;

        targetColumnNames = new ArrayList<String>();
        columnDBName = argv[0];
        outerFile = argv[1];
        innerFile = argv[2];
        int i = 0;
        if (argv[3].equals("[")) {
            i = 4;
            while (!argv[i].equals("]")) {
                if (argv[i].equals("(") || argv[i].equals(")"))
                    i++;
                else {
                    outerConst.append(argv[i] + " ");
                    i++;
                }
            }
        }
        if (argv[i + 1].equals("[")) {
            i = i + 2;
            while (!argv[i].equals("]")) {
                if (argv[i].equals("(") || argv[i].equals(")"))
                    i++;
                else {
                    innerConst.append(argv[i] + " ");
                    i++;
                }
            }
        }
        if (argv[i + 1].equals("[")) {
            i = i + 2;
            while (!argv[i].equals("]")) {
                joinConst.append(argv[i] + " ");
                i++;
            }
        }
        indexType = getIndexType(argv[i + 1]);
        i = i + 1;
        for (i = i + 2; i <= lengthOfArgv - 3; i++) {
            targetColumnNames.add(argv[i]);
        }
        numBuf = Integer.parseInt(argv[lengthOfArgv - 1]);

        SystemDefs systemDefs = new SystemDefs(columnDBName, 0, numBuf, "LRU");

        outerConst.deleteCharAt(outerConst.length() - 1);
        innerConst.deleteCharAt(innerConst.length() - 1);
        joinConst.deleteCharAt(joinConst.length() - 1);

        outerCondExpr = parseToCondExpr(outerFile, outerConst);
        innerCondExpr = parseToCondExpr(innerFile, innerConst);
        CondExpr joinExpr = parseJoinConstr(outerFile, innerFile, joinConst);

        joinCondExpr = new CondExpr[1];
        joinCondExpr[0] = joinExpr;

        ColumnarFile outerColumnarFile = new ColumnarFile(outerFile);
        in1 = outerColumnarFile.getColumnarHeader().getColumns();

        FldSpec[] outerFldSpecs = new FldSpec[in1.length];

        for (int j = 0; j < in1.length; j++) {
            outerFldSpecs[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }

        FldSpec[] projectionFldSpecs = new FldSpec[targetColumnNames.size()];

        ColumnarFile innerColumnarFile = new ColumnarFile(innerFile);
        in2 = innerColumnarFile.getColumnarHeader().getColumns();

        IndexType[] indexTypes = new IndexType[2];
        indexTypes[0] = new IndexType(IndexType.ColumnScan);
        indexTypes[1] = new IndexType(IndexType.B_Index);

        ColumnarIndexScan outerScan = new ColumnarIndexScan(outerFile, null, indexTypes, null, in1,
            null, in1.length, in1.length, outerFldSpecs, outerCondExpr);

        NestedLoopJoins nestedLoopJoins = new NestedLoopJoins(in1, null, in2, null, 10,
            outerScan, innerFile, joinCondExpr, innerCondExpr, outerFldSpecs, outerFldSpecs.length, indexTypes);

        Tuple tuple = nestedLoopJoins.getNext();

        while (tuple != null) {
            System.out.println(tuple);

            tuple = nestedLoopJoins.getNext();
        }

        System.out.println("Iteration Complete");
    }

    private static CondExpr[] parseToCondExpr(String fileName,
                                              StringBuffer constr) throws HFBufMgrException,
        InvalidSlotNumberException, IOException, DiskMgrException,
        ColumnarFileDoesExistsException, ColumnarFilePinPageException,
        HFException, HFDiskMgrException, ColumnarFileUnpinPageException,
        bitmap.PinPageException, AddFileEntryException, UnpinPageException,
        bitmap.ConstructPageException, bitmap.GetFileEntryException {
        // TODO Auto-generated method stub
        int index = -1;
        ColumnarFile columnarFile = new ColumnarFile(fileName);
        AttrType attrTypes[] = columnarFile.getColumnarHeader().getColumns();
        List<String> columnName = new ArrayList<String>();
        for (int i = 0; i < attrTypes.length; i++) {
            columnName.add(attrTypes[i].getAttrName());
        }
        String[] temp1 = constr.toString().split(" AND ");
        ArrayList<CondExpr> constParser = new ArrayList<CondExpr>();
        for (int i = 0; i < temp1.length; i++) {
            String[] temp2 = temp1[i].toString().split(" OR ");
            CondExpr[] cond = new CondExpr[temp2.length];
            for (int k = 0; k < temp2.length; k++)
                cond[k] = new CondExpr();
            for (int j = 0; j < temp2.length; j++) {
                cond[j].op = parseOperator(temp2[j]);
                String op = getOperatorSymbol(cond[j].op);
                String splitExpr[] = temp2[j].split(" " + op + " ");
                if (columnName.toString().contains(splitExpr[0])) {
                    index = columnName.indexOf(splitExpr[0]);

                }

                cond[j].operand1.symbol = new FldSpec(new RelSpec(0),
                    attrTypes[index].getColumnId());
                cond[j].type1 = new AttrType(AttrType.attrSymbol);
                cond[j].type2 = new AttrType(attrTypes[index].getAttrType());

                if (attrTypes[index].getAttrType() == 1)
                    cond[j].operand2.integer = Integer.parseInt(splitExpr[1]);
                else if (attrTypes[index].getAttrType() == 0)
                    cond[j].operand2.string = splitExpr[1];
                if (j + 1 < temp2.length)
                    cond[j].next = cond[j + 1];

            }
            constParser.add(cond[0]);

        }

        return constParser.toArray(new CondExpr[constParser.size()]);

    }

    public static CondExpr parseJoinConstr(String fileName1, String fileName2,
                                           StringBuffer joinConst)
        throws DiskMgrException, ColumnarFileDoesExistsException,
        ColumnarFilePinPageException, HFException, HFBufMgrException,
        HFDiskMgrException, ColumnarFileUnpinPageException,
        bitmap.PinPageException, AddFileEntryException, UnpinPageException,
        bitmap.ConstructPageException, bitmap.GetFileEntryException,
        IOException, InvalidSlotNumberException {

        int index1 = -1, index2 = -1;
        ColumnarFile columnarFile1 = new ColumnarFile(fileName1);
        ColumnarFile columnarFile2 = new ColumnarFile(fileName2);
        String joinConstr = joinConst.toString();
        AttrType attrTypes1[] = columnarFile1.getColumnarHeader().getColumns();
        AttrType attrTypes2[] = columnarFile2.getColumnarHeader().getColumns();
        List<String> columnNames1 = new ArrayList<String>();
        List<String> columnNames2 = new ArrayList<String>();
        for (int i = 0; i < attrTypes1.length; i++) {
            columnNames1.add(attrTypes1[i].getAttrName());
        }
        for (int i = 0; i < attrTypes2.length; i++) {
            columnNames2.add(attrTypes2[i].getAttrName());
        }
        CondExpr condExpr = new CondExpr();
        condExpr.next = null;
        condExpr.op = parseOperator(joinConstr);
        String op = getOperatorSymbol(condExpr.op);
        String splitExpr[] = joinConstr.split(" " + op + " ");
        if (columnNames1.toString().contains(splitExpr[0])) {
            index1 = columnNames1.indexOf(splitExpr[0]);

        }
        if (columnNames2.toString().contains(splitExpr[1])) {
            index2 = columnNames2.indexOf(splitExpr[1]);

        }
        condExpr.operand1.symbol = new FldSpec(new RelSpec(0),
            attrTypes1[index1].getColumnId());
        condExpr.operand2.symbol = new FldSpec(new RelSpec(0),
            attrTypes2[index2].getColumnId());
        condExpr.type1 = new AttrType(AttrType.attrSymbol);
        condExpr.type2 = new AttrType(AttrType.attrSymbol);

        return condExpr;
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

    private static AttrOperator parseOperator(String expr) {

        if (expr.contains("=="))
            return new AttrOperator(0);
        if (expr.contains("<="))
            return new AttrOperator(4);
        if (expr.contains(">="))
            return new AttrOperator(5);
        if (expr.contains("<"))
            return new AttrOperator(1);
        if (expr.contains(">"))
            return new AttrOperator(2);
        if (expr.contains("!="))
            return new AttrOperator(3);

        else
            return null;
    }

    private static String getOperatorSymbol(AttrOperator operator) {

        if (operator.toString().equals("aopEQ"))
            return "==";
        if (operator.toString().equals("aopLT"))
            return "<";
        if (operator.toString().equals("aopGT"))
            return ">";
        if (operator.toString().equals("aopNE"))
            return "==";
        if (operator.toString().equals("aopLE"))
            return "<=";
        if (operator.toString().equals("aopGE"))
            return ">=";
        else
            return null;
    }

}
