package columnar;

import bufmgr.LRU;
import global.AttrType;
import global.SystemDefs;
import org.junit.Test;

public class ColumnarFileTest {
    @Test
    public void creationColumnar() throws Exception {
        String dbPath = "Minibase.min";
        SystemDefs systemDefs = new SystemDefs(dbPath, 3000
                , 400, "LRU");
        AttrType[] attrTypes = new AttrType[20];
        for (int i = 0; i < 20; i++) {
            attrTypes[i] = new AttrType((int) (Math.random() * 4));
            attrTypes[i].setColumnId(i);
            attrTypes[i].setSize(4);
            attrTypes[i].setAttrName("Column" + i);
        }
        ColumnarFile columnarFile = new ColumnarFile("Employee", 20, attrTypes);
        SystemDefs.JavabaseBM.flushAllPages();
        columnarFile = new ColumnarFile("Employee");
        SystemDefs.JavabaseBM.pinPage(columnarFile.getColumnarHeader().getHeaderPageId()
                , columnarFile.getColumnarHeader(), false);
        System.out.println(columnarFile.getColumnarHeader().getNextPage().pid);
        System.out.println(columnarFile.getColumnarHeader().getColumnCount());
        columnarFile.getColumnarHeader().getColumns();
    }
}
