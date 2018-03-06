package global;

import bufmgr.BufMgr;
import diskmgr.ColumnDB;
import diskmgr.PCounter;

public class SystemDefs {
    public static BufMgr JavabaseBM;
    public static ColumnDB JavabaseDB;
    public static PCounter pCounter;
    public static boolean MINIBASE_RESTART_FLAG = false;

    public SystemDefs(String dbName, int numPages, int bufPoolSize, String replacementPolicy) {
        init(dbName, numPages, bufPoolSize, replacementPolicy);
    }

    public void init(String dbName, int numPages, int bufPoolSize, String replacementPolicy) {
        JavabaseBM = new BufMgr(bufPoolSize, replacementPolicy);
        JavabaseDB = new ColumnDB();
        if (MINIBASE_RESTART_FLAG || numPages == 0)
            try {
                JavabaseDB.openDB(dbName);
            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        else
            try {
                JavabaseDB.openDB(dbName, numPages);
                JavabaseBM.flushAllPages();
            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
    }
}
