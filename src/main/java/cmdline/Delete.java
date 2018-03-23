package cmdline;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import columnar.ByteToTuple;
import columnar.ColumnarFile;
import columnar.ColumnarFileDoesExistsException;
import columnar.ColumnarFilePinPageException;
import columnar.ColumnarFileUnpinPageException;
import diskmgr.DiskMgrException;
import global.IndexType;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Tuple;
import iterator.ColumnarFileScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import global.AttrOperator;
import global.AttrType;
import global.Convert;

public class Delete {
  private static String columnDBName;
  private static String columnarFileName;
  private static ArrayList<String> targetColumnNames;
  private static String operator;
  private static String value;
  private static String conditonalColumn;
  private static int numBuf;
  private static IndexType indexType;
  private static ColumnarFile columnarFile;
  private static boolean purgeDB;
  private static AttrType[] attrTypes;

  public static void main(String argv[]) throws Exception{
    initFromArgs(argv);
  }

  private static void initFromArgs(String argv[]) throws Exception{

    int lengthOfArgv = argv.length;
    columnDBName = argv[0];
    columnarFileName = argv[1];
    //targetColumnNames = (ArrayList<String>) Arrays.asList(argv[2].split(","));
    indexType = getIndexType(argv[lengthOfArgv - 2]);
    numBuf = Integer.parseInt(argv[lengthOfArgv - 3]);
    value = argv[lengthOfArgv - 4];
    operator = argv[lengthOfArgv - 5];
    conditonalColumn = argv[lengthOfArgv - 6];
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
      targetColumnNames.add(columnsFileName);

    }
    setUpFileScan();


  }

  /*
   * Function to get the values of the arguments and then call filescan
   */
  private static void setUpFileScan()
          throws Exception {

    AttrType[] in = new AttrType[targetColumnNames.size()+1];
    AttrType[] proj = new AttrType[targetColumnNames.size()];

    short[] strSizes = new short[2];
    int conditonalColumnId =-1;
    FldSpec[] projList= new FldSpec[targetColumnNames.size()];
    AttrType condAttr = new AttrType();
    SystemDefs systemDefs = new SystemDefs(columnDBName, 0, numBuf, "LRU");
    ColumnarFile columnarFile = new ColumnarFile(columnarFileName);
    attrTypes = columnarFile.getColumnarHeader().getColumns();
    int columncount = attrTypes.length;
    int outColumnsSize = targetColumnNames.size();
    int counterStr=0, counterIn = 0, counterFld = 0;

    for (int i = 0; i < columncount; i++) {
      for (int j = 0; j < outColumnsSize; j++) {
        if (attrTypes[i].getAttrName().equals(targetColumnNames.get(j))) {
          in[counterIn] = attrTypes[i];
          proj[counterFld] = attrTypes[i];
          projList[counterFld] = new FldSpec(new RelSpec(0), attrTypes[i].getColumnId());
          counterIn++;
          counterFld++;
          if(attrTypes[i].getAttrType()==0) {
            //strSizes[counterStr] = attrTypes[i].getSize();
            counterStr++;
          }

          break;
        }
        else if(i == columncount-1 ) {
          //throw exception record not found.


        }
      }

      if(attrTypes[i].getAttrName().equals(conditonalColumn)) {
        condAttr = attrTypes[i];
        in[counterIn] = attrTypes[i];
        counterIn++;
        conditonalColumnId = attrTypes[i].getColumnId();
      }
    }


    CondExpr[] condition = new CondExpr[2];
    condition[1] = null;
    condition[0] = new CondExpr();
    condition[0].next = condition[1];
    condition[0].operand1.symbol = new FldSpec(new RelSpec(0),conditonalColumnId);
    condition[0].op = parseOperator(operator);
    condition[0].type1 = new AttrType(AttrType.attrSymbol);
    condition[0].type2 = new AttrType(condAttr.getAttrType());
    if(condAttr.getAttrType() == 1)
      condition[0].operand2.integer = Integer.parseInt(value);
    else if (condAttr.getAttrType() == 0)
      condition[0].operand2.string = value;

    ColumnarFileScan columnarScan = new ColumnarFileScan(columnarFileName, in, strSizes,counterIn, counterFld, projList, condition);
    Tuple tuple = columnarScan.getNext();
    ByteToTuple byteToTuple = new ByteToTuple(proj);
    for (int i = 0 ; i< proj.length;i++) {
      System.out.print(proj[i].getAttrName()+"\t");
    }
    System.out.print("\n");
    while (tuple != null) {
      ArrayList<byte[]> tuples = byteToTuple.setTupleBytes(tuple.getTupleByteArray());
      int count = 0;
      for(AttrType type: proj) {

        if(type.getAttrType() == 1) {
          System.out.print(Convert.getIntValue(0, tuples.get(count))+ "\t	");
        }
        else if(type.getAttrType()==0) {
          System.out.print(Convert.getStringValue(0, tuples.get(count), tuples.get(count).length)+"\t");
        }

        count++;
      }
      System.out.println("\n");
      tuple = columnarScan.getNext();
      columnarFile.markTupleDeleted(tuple)
      if (purgeDB) {

        columnarFile.purgeAllDeletedTuples(bitMapFile)
      }
    }



  }

  /*
   * Function to parse the Index type
   */
  private static IndexType getIndexType(String indexName) {
    if(indexName.equals("FILESCAN"))
      return new IndexType(0);
    if(indexName.equals("COLUMNSCAN"))
      return new IndexType(4);
    if(indexName.equals("BTREE"))
      return new IndexType(1);
    if(indexName.equals("BITMAP"))
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

