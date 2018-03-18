package cmdline;


import java.io.IOException;
import java.util.ArrayList;
import columnar.ColumnarFile;
import columnar.ColumnarFileDoesExistsException;
import columnar.ColumnarFilePinPageException;
import columnar.ColumnarFileUnpinPageException;
import diskmgr.DiskMgrException;
import iterator.ColumnarFileScan;
import iterator.ColumnarFileScanException;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
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

public class Query {
	private static String columnDBName;
	private static String columnarFileName;
	private static ArrayList<String> targetColumnNames;
	private static String operator;
	private static String value;
	private static String conditonalColumn;
	private static int numBuf;
	private static IndexType indexType;
	private ColumnarFile columnarFile;
	private static AttrType[] attrTypes;
	
	
	
	
	public static void main(String argv[]) 
			throws Exception {
		
			initFromArgs(argv);
	}

	private static void initFromArgs(String argv[]) 
			throws Exception {
		int lengthOfArgv = argv.length;
		columnDBName = argv[0];
		columnarFileName = argv[1];
		indexType = getIndexType(argv[lengthOfArgv - 1]);
		int numBuf = Integer.parseInt(argv[lengthOfArgv - 2]);
		value = argv[lengthOfArgv - 3];
		operator = argv[lengthOfArgv - 4];
		conditonalColumn = argv[lengthOfArgv - 5];

		int counter = 0;
		for (int i = lengthOfArgv - 7; i >= 3; i--) {
			targetColumnNames.add(argv[i]);
			counter++;
		}
		
		setUpFileScan();
		
	}
	
	private static void setUpFileScan() 
			throws Exception {
		
		AttrType[] in = {};
		short[] strSizes = {};
		int conditonalColumnId =-1;
		FldSpec[] projList= {};
		AttrType condAttr = new AttrType();
		SystemDefs systemDefs = new SystemDefs(columnDBName, 0, numBuf, "LRU");
		ColumnarFile columnarFile = new ColumnarFile(columnarFileName);
		attrTypes = columnarFile.getColumnarHeader().getColumns();
		int columncount = attrTypes.length;
		int outColumnssize = targetColumnNames.size();
		int counterStr=0, counterIn = 0, counterFld = 0;
	
		for (int i = 0; i < columncount; i++) {
			for (int j = 0; j < outColumnssize; j++) {
				if (attrTypes[i].getAttrName().equals(targetColumnNames.get(j))) {
					in[counterIn] = attrTypes[i];
					projList[counterIn] = new FldSpec(new RelSpec(0), attrTypes[i].getColumnId());
					counterIn++;
					counterFld++;
					if(attrTypes[i].getAttrType()==0) {
						strSizes[counterStr] = attrTypes[i].getSize();
						counterStr++;
					}
						
					break;
					}
				else if(j == outColumnssize-1 ) {
					//throw exception record not found.
					
					
				}
			}
			
			if(attrTypes[i].getAttrName().equals(conditonalColumn)) {
				condAttr = attrTypes[i];
				in[counterIn] = attrTypes[i];
				counterIn++;	
			}
		}
		
		
		CondExpr[] condition = new CondExpr[2];
		condition[0].next = null;
		condition[0].operand1.symbol = new FldSpec(new RelSpec(0),conditonalColumnId);
		condition[0].op = parseOperator(operator);
		if(condAttr.getAttrType() == 0)
			condition[0].operand2.integer = Integer.parseInt(value);
		else
			condition[0].operand2.string = value;
		
		ColumnarFileScan columnarScan = new ColumnarFileScan(columnarFileName, in, strSizes,counterIn, counterFld, projList, condition);
		Tuple tuple = columnarScan.getNext();

        while (tuple != null) {
           
        	
	        System.out.println();
	        tuple = columnarScan.getNext();
        }	
	}
	
	
	private static IndexType getIndexType(String indexName) {
		if(indexName == "FILESCAN")
			return new IndexType(0);
		if(indexName == "COLUMNSCAN")
			return new IndexType(4);
		if(indexName == "BTREE")
			return new IndexType(1);
		if(indexName == "BITMAP");
			return new IndexType(3);
	}
	
	private static AttrOperator parseOperator(String operator) {
		if (operator == "==") 
			return new AttrOperator(0);
		if (operator == "<")
			return new AttrOperator(1);
		if (operator == ">")
			return new AttrOperator(2);
		if (operator == "!=")
			return new AttrOperator(3);
		if (operator == "<=")
			return new AttrOperator(4);
		if (operator == ">=")
			return new AttrOperator(5);
		else 
			return null;	
	}
	
	
}
