package cmdline;

import java.io.IOException;
import java.util.ArrayList;

import bitmap.AddFileEntryException;
import bitmap.BitMapScanException;
import bitmap.UnpinPageException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import columnar.ByteToTuple;
import columnar.ColumnarFile;
import columnar.ColumnarFileDoesExistsException;
import columnar.ColumnarFilePinPageException;
import columnar.ColumnarFileUnpinPageException;
import diskmgr.DiskMgrException;
import global.AttrOperator;
import global.AttrType;
import global.Convert;
import global.IndexType;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.Tuple;
import iterator.BitmapScan;
import iterator.BtreeScan;
import iterator.ColumnScan;
import iterator.ColumnarFileScan;
import iterator.ColumnarFileScanException;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.IndexException;
import iterator.Iterator;
import iterator.NestedLoopJoins;
import iterator.RelSpec;

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
    private static CondExpr[][] outerCondExpr;
    private static CondExpr[][] innerCondExpr;
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
	System.out.println("argv 3" + argv[3]);
	if (argv[3].equals("[")) {
	    i = 4;
	    while (!argv[i].equals("]")) {
		if (argv[i].equals("(") || argv[i].equals(")"))
		    i++;
		else {
		    outerConst.append(argv[i]);
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
		    innerConst.append(argv[i]);
		    i++;
		}
	    }
	}
	if (argv[i + 1].equals("[")) {
	    i = i + 2;
	    while (!argv[i].equals("]")) {
		    joinConst.append(argv[i]);
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

	 outerCondExpr = parseToCondExpr(outerFile, outerConst);
	 innerCondExpr = parseToCondExpr(innerFile, innerConst);
	
    }

    private static CondExpr[][] parseToCondExpr(String fileName,
	    StringBuffer constr) throws HFBufMgrException,
	    InvalidSlotNumberException, IOException, DiskMgrException,
	    ColumnarFileDoesExistsException, ColumnarFilePinPageException,
	    HFException, HFDiskMgrException, ColumnarFileUnpinPageException,
	    bitmap.PinPageException, AddFileEntryException, UnpinPageException,
	    bitmap.ConstructPageException, bitmap.GetFileEntryException {
	// TODO Auto-generated method stub
	int k = 0, index = 0;
	ColumnarFile columnarFile = new ColumnarFile(fileName);
	AttrType attrTypes[] = columnarFile.getColumnarHeader().getColumns();
	ArrayList<String> columnName = new ArrayList<String>();
	for (int i = 0; i < attrTypes.length; i++) {
	    columnName.add(attrTypes[i].getAttrName());
	}
	in1 = new AttrType[attrTypes.length];
	in2 = new AttrType[attrTypes.length];
	ArrayList<ArrayList<CondExpr>> outerConstParser = new ArrayList<ArrayList<CondExpr>>();
	String[] temp1 = constr.toString().split("AND");
	for (int i = 0; i < temp1.length; i++) {
	    ArrayList<CondExpr> condList = new ArrayList<CondExpr>();
	    String[] temp2 = temp1[i].toString().split("OR");
	    for (int j = 0; j < temp2.length; j++) {
		CondExpr condExpr = new CondExpr();

		condExpr.op = parseOperator(temp2[j]);
		String op = getOperatorSymbol(condExpr.op);
		// System.out.println(op);
		String splitExpr[] = temp2[j].split(op);
		if (fileName.equals(outerFile)) {
		    if (columnName.toString().contains(splitExpr[0])) {
			index = columnName.indexOf(splitExpr[0]);
			in1[k] = attrTypes[index];
			k++;
		    }

		} else {
		    if (columnName.toString().contains(splitExpr[0])) {
			index = columnName.indexOf(splitExpr[0]);
			in2[k] = attrTypes[index];
			k++;
		    }
		}

		condExpr.operand1.symbol = new FldSpec(new RelSpec(0),
			attrTypes[index].getColumnId());
		// System.out.println(condExpr.operand1.symbol);
		condExpr.type1 = new AttrType(AttrType.attrSymbol);
		condExpr.type2 = new AttrType(attrTypes[index].getAttrType());
		condExpr.next = null;

		if (attrTypes[index].getAttrType() == 1)
		    condExpr.operand2.integer = Integer.parseInt(splitExpr[1]);
		else if (attrTypes[index].getAttrType() == 0)
		    condExpr.operand2.string = splitExpr[1];

		condList.add(condExpr);
	    }
	    outerConstParser.add(condList);

	}
	CondExpr c = outerConstParser.get(0).get(0);
	System.out.println(c.op.attrOperator);

	CondExpr[][] condExpression = outerConstParser.stream()
		.map(u -> u.toArray(new CondExpr[0]))
		.toArray(CondExpr[][]::new);

	return condExpression;

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
