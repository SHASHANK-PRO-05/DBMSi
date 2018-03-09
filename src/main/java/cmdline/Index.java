package cmdline;

import java.io.BufferedReader;
import java.io.File;
import java.util.Scanner;

import columnar.ColumnarFile;
import columnar.TupleScan;
import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.Heapfile;
import heap.Scan;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class Index {
  private static String columnDBName;
  private static String columnarFileName;
  private static int columnId;
  private static String indexType;
  private static String columnName;
  private static ColumnarFile columnarFile;

  public static void main(String argv[]) throws Exception {
    initFromArgs(argv);
  }

  private static void initFromArgs(String argv[])
          throws Exception {
	   columnDBName = argv[0];
	   columnarFileName = argv[1];
	   columnName = argv[2];
	   indexType = argv[3];
	   HFPage hfPage = new HFPage();
	   
	 /*  int pageSizeRequired = (int) (file
               .length() / MINIBASE_PAGESIZE) * 4;

       int bufferSize = pageSizeRequired / 4;
       if (bufferSize < 10) bufferSize = 10;
	   
	   SystemDefs systemDefs = new SystemDefs(columnDBName, pageSizeRequired
               , bufferSize, null);*/
	   
	   if(indexType.equals("BITMAP")) {
		   columnarFile = new ColumnarFile(columnarFileName);
		   for(int i=0;i<columnarFile.getNumColumns();i++) {
		   if(columnarFile.getType()[i].getAttrName().equals(columnName)) {
			   columnId=columnarFile.getType()[i].getColumnId();
			     
		   }
		   }
		   
		   // Leaving this function right now as not sure about sequential scan
		   Scan scan = new Scan(columnarFile,(short)columnId);
		   int count = columnarFile.getTupleCount();
		   RID rid = new RID();
		   rid = scan.getFirstRID();
		   for(int i=0;i<count;i++) {
			  
			   
		   }
		   
		   
	   }
	   }
	   
      
      
     
  }

  

