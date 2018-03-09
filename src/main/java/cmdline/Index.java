package cmdline;

import java.io.BufferedReader;
import java.io.File;
import java.util.Scanner;

import columnar.ColumnarFile;
import global.AttrType;
import global.SystemDefs;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class Index {
  private static String columnDBName;
  private static String columnarFileName;
  private static short numColumns;
  private static String indexType;
  private static String columnName;

  public static void main(String argv[]) throws Exception {
    initFromArgs(argv);
  }

  private static void initFromArgs(String argv[])
          throws Exception {
	   columnDBName = argv[0];
	   columnarFileName = argv[1];
	   columnName = argv[2];
	   indexType = argv[3];
	   
	   if(indexType.equals("BITMAP")) {
		   
	   }
	   
      
      
     
  }

  
}
