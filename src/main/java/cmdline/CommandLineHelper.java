package cmdline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import bitmap.AddFileEntryException;
import bitmap.UnpinPageException;
import columnar.ColumnarFile;
import columnar.ColumnarFileDoesExistsException;
import columnar.ColumnarFilePinPageException;
import columnar.ColumnarFileUnpinPageException;
import diskmgr.DiskMgrException;
import global.AttrOperator;
import global.AttrType;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

public class CommandLineHelper {
    
    public static CondExpr[] parseToCondExpr(String fileName,
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
	condExpr.op = CommandLineHelper.parseOperator(joinConstr);
	String op = CommandLineHelper.getOperatorSymbol(condExpr.op);
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


    public static AttrOperator parseOperator(String expr) {

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

       public static String getOperatorSymbol(AttrOperator operator) {

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
