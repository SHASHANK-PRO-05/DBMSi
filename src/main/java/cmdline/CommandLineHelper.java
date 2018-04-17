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
