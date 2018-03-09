package columnar;

import bufmgr.LRU;
import global.*;
import org.junit.Test;

public class ColumnarFileTest {
    @Test
    public void creationColumnar() throws Exception {
        String dbPath = "Minibase.min";
        SystemDefs systemDefs = new SystemDefs(dbPath, 3000
                , 400, "LRU");
        AttrType[] attrTypes = new AttrType[20];
        int[] in = new int[20];

        for (int i = 0; i < 20; i++) {
            attrTypes[i] = new AttrType();
            attrTypes[i].setColumnId(i);
            attrTypes[i].setSize(4);
            attrTypes[i].setAttrType(1);
            attrTypes[i].setAttrName("Column" + i);
            in[i] = (int) (Math.random() * 40);
        }
        ColumnarFile columnarFile = new ColumnarFile("Employee", 20, attrTypes);
        SystemDefs.JavabaseBM.flushAllPages();
        columnarFile = new ColumnarFile("Employee");
        SystemDefs.JavabaseBM.pinPage(columnarFile.getColumnarHeader().getHeaderPageId()
                , columnarFile.getColumnarHeader(), false);
        System.out.println(columnarFile.getColumnarHeader().getNextPage().pid);
        System.out.println(columnarFile.getColumnarHeader().getColumnCount());
        AttrType[] attrTypes1 = columnarFile.getColumnarHeader().getColumns();
        for (AttrType attrType : attrTypes1) {
            System.out.println(attrType.getSize());
            System.out.println(attrType.getAttrType());
            System.out.println(attrType.getColumnId());
            System.out.println(attrType.getAttrName());
        }
        columnarFile.insertTuple(Convert.intAtobyteA(in));


//        IndexInfo info = new IndexInfo();
//        info.setColumnNumber(12);
//        info.setFileName("Laveena");
//        info.setIndexType(new IndexType(1));
//        info.setValue(new IntegerValue(4));
//
//
//        columnarFile.getColumnarHeader().setIndex(info);
//        IndexInfo info1 = columnarFile.getColumnarHeader().getIndex(12, new IndexType(1));
//        System.out.println(info1.getColumnNumber());
//        System.out.println(info1.getFileName());


        SystemDefs.JavabaseBM.unpinPage(columnarFile.getColumnarHeader().getHeaderPageId(), false);
        SystemDefs.JavabaseBM.flushAllPages();

        columnarFile.deleteColumnarFile();
        SystemDefs.JavabaseBM.flushAllPages();
        System.out.println(SystemDefs.JavabaseDB.getFileEntry(columnarFile.getColumnarHeader().getHdrFile()));


    }
}
