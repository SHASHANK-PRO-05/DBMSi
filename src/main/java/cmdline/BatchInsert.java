package cmdline;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class BatchInsert {
  private static String dbFile;
  private static String columnDBName;
  private static String columnarFileName;
  private static short numColumns;

  public static void main(String argv[]) {
    initFromArgs(argv);
  }

  private static void initFromArgs(String argv[]) {
    throw new NotImplementedException();
  }
}
