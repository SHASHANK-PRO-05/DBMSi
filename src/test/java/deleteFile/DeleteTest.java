package deleteFile;

import columnar.ColumnarFile;
import global.SystemDefs;
import heap.Heapfile;
import org.junit.Test;

public class DeleteTest {
    @Test
    public void test() throws Exception {
        SystemDefs systemDefs = new SystemDefs("Minibase.min", 0, 4000, "LRU");
        ColumnarFile columnarFile = new ColumnarFile("Employee1");
        Heapfile heapfile = new Heapfile("Employee1.0");
        System.out.println(heapfile.getRecCnt());
        columnarFile.purgeRecords();
        heapfile = new Heapfile("Employee1.0");
        System.out.println(heapfile.getRecCnt());
        SystemDefs.JavabaseBM.flushAllPages();
    }
}
