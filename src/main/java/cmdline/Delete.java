package cmdline;

import bitmap.BitMapFile;
import columnar.ByteToTuple;
import columnar.ColumnarFile;
import global.*;
import heap.Tuple;
import iterator.*;

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
  }

  private static void initFromArgs(String argv[]) throws Exception {

    int lengthOfArgv = argv.length;
    columnDBName = argv[0];
    columnarFileName = argv[1];
    indexType = getIndexType(argv[lengthOfArgv - 2]);
    numBuf = Integer.parseInt(argv[lengthOfArgv - 3]);
    value = argv[lengthOfArgv - 4];
    operator = argv[lengthOfArgv - 5];
    conditionalColumn = argv[lengthOfArgv - 6];
    purgeDB = Boolean.parseBoolean(argv[lengthOfArgv - 1]);

    SystemDefs systemDefs = new SystemDefs(columnDBName, 0, 4000, "LRU");


    columnarFile = new ColumnarFile(columnarFileName);

    /*attrTypes = columnarFile.getColumnarHeader().getColumns();
    int columnCount = attrTypes.length;
    for (int i = 0; i < columnCount; i++) {
        if (attrTypes[i].getAttrName().equals(columnName)) {
            attrType = attrTypes[i];
            columnId = i;
            break;
        }
    }*/
    for (int i = 0; i < columnarFile.getNumColumns(); i++) {
      String fileNum = Integer.toString(i);
      String columnsFileName = columnarFile.getColumnarHeader() + "." + fileNum;
    }

    setUpFileScan();
  }

  /*
   * Function to get the values of the arguments and then call filescan
   */
  private static void setUpFileScan()
      throws Exception {

    AttrType[] in = new AttrType[columnarFile.getNumColumns() + 1];
    AttrType[] proj = new AttrType[columnarFile.getNumColumns()];

    short[] strSizes = new short[2];
    int conditionalColumnId = -1;
    FldSpec[] projList = new FldSpec[columnarFile.getNumColumns()];
    AttrType condAttr = new AttrType();
    SystemDefs systemDefs = new SystemDefs(columnDBName, 0, numBuf, "LRU");
    ColumnarFile columnarFile = new ColumnarFile(columnarFileName);
    attrTypes = columnarFile.getColumnarHeader().getColumns();
    int columncount = attrTypes.length;
    int outColumnsSize = columnarFile.getNumColumns();
    int counterStr = 0, counterIn = 0, counterFld = 0;

    for (int i = 0; i < columncount; i++) {
      for (int j = 0; j < outColumnsSize; j++) {
        if (attrTypes[i].getAttrName().equals(columnarFile.getHeapFileNames()[j].getType().getAttrName())) {
          in[counterIn] = attrTypes[i];
          proj[counterFld] = attrTypes[i];
          projList[counterFld] = new FldSpec(new RelSpec(0), attrTypes[i].getColumnId());
          counterIn++;
          counterFld++;
          if (attrTypes[i].getAttrType() == 0) {
            //strSizes[counterStr] = attrTypes[i].getSize();
            counterStr++;
          }

          break;
        } else if (i == columncount - 1) {
          //throw exception record not found.


        }
      }

      if (attrTypes[i].getAttrName().equals(conditionalColumn)) {
        condAttr = attrTypes[i];
        in[counterIn] = attrTypes[i];
        counterIn++;
        conditionalColumnId = attrTypes[i].getColumnId();
      }
    }


    CondExpr[] condition = new CondExpr[2];
    condition[1] = null;
    condition[0] = new CondExpr();
    condition[0].next = condition[1];
    condition[0].operand1.symbol = new FldSpec(new RelSpec(0), conditionalColumnId);
    condition[0].op = parseOperator(operator);
    condition[0].type1 = new AttrType(AttrType.attrSymbol);
    condition[0].type2 = new AttrType(condAttr.getAttrType());
    if (condAttr.getAttrType() == 1)
      condition[0].operand2.integer = Integer.parseInt(value);
    else if (condAttr.getAttrType() == 0)
      condition[0].operand2.string = value;

    CondExprEval condExprEval = new CondExprEval(attrTypes, condition);

    ColumnarFileScan columnarScan = new ColumnarFileScan(columnarFileName, in, strSizes, counterIn, counterFld, projList, condition);
    Tuple tuple = columnarScan.getNext();
    ByteToTuple byteToTuple = new ByteToTuple(proj);

    for (AttrType aProject : proj) {
      System.out.print(aProject.getAttrName() + "\t");
    }

    System.out.print("\n");

    int position = 0;

    while (tuple != null) {
      ArrayList<byte[]> tuples = byteToTuple.setTupleBytes(tuple.getTupleByteArray());
      System.out.println("\n");
      tuple = columnarScan.getNext();

      ArrayList<byte[]> arrayList = byteToTuple.setTupleBytes(tuple.getTupleByteArray());
      if (condExprEval.isValid(arrayList)) {
        columnarFile.markTupleDeleted(new TID(columnarFile.getNumColumns(), position));
      }

      position++;
    }

    if (purgeDB) {
      BitMapFile bitMapFile = new BitMapFile(columnarFile.getColumnarHeader().getHdrFile() + ".del");
      columnarFile.purgeAllDeletedTuples(bitMapFile);
    }
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

