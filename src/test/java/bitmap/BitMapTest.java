package bitmap;

import columnar.ColumnarFile;
import global.Convert;
import global.SystemDefs;
import global.ValueClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class BitMapTest {
    @Before
    public void setup() {

    }

  
    @Test
    public void setupBitMapOperation() throws Exception {
        String dbPath = "Minibase.min";
        SystemDefs systemDefs = new SystemDefs(dbPath, 40, 10, null);
//        BitMapFile bitMapFile = new BitMapFile("test", new ColumnarFile(), 0, new IntegerKey());
//        BitMapOperations bitMapOperations = new BitMapOperations();
//        for (int elem : bitMapOperations.getIndexedPostions(bitMapFile)) {
//            System.out.println(elem);
//        }
//        SystemDefs.JavabaseBM.unpinPage(bitMapFile.getHeaderPage().getCurrPage(), true);
//        SystemDefs.JavabaseBM.flushAllPages();
//        File file = new File(dbPath);
        // file.delete();
    }

    @After
    public void cleanUp() {

    }


}
