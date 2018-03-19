package columnar;

import bufmgr.LRU;
import global.*;
import heap.Scan;
import heap.Tuple;
import org.junit.Test;

import java.util.Arrays;

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
            attrTypes[i].setAttrType(0);
            attrTypes[i].setAttrName("Column" + i);
            in[i] = (int) (Math.random() * 40);
        }

        ColumnarFile columnarFile = new ColumnarFile("Employee", 20, attrTypes);
        SystemDefs.JavabaseBM.flushAllPages();
//        columnarFile = new ColumnarFile("Employee");
        SystemDefs.JavabaseBM.flushAllPages();
        //SystemDefs.JavabaseBM.pinPage(columnarFile.getColumnarHeader().getHeaderPageId()
        //       , columnarFile.getColumnarHeader(), false);
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


        //Index insertion Info testing
        IndexInfo info = new IndexInfo();
        info.setColumnNumber(12);
        info.setFileName("Laveena");
        info.setIndexType(new IndexType(1));
        info.setValue(new IntegerValue(4));

        

        System.out.println("--------------------");
        System.out.println(columnarFile.getHeapFileNames()[1].getRecCnt());
        //SystemDefs.JavabaseBM.pinPage(columnarFile.getColumnarHeader().getHeaderPageId(), columnarFile.getColumnarHeader(), false);
        System.out.println(columnarFile.getHeapFileNames()[0].getRecCnt());
        Scan scan = new Scan(columnarFile, (short) 0);
        System.out.println("First RID:" + scan.getFirstRID());
        RID rid = new RID();
        Tuple tuple = scan.getNext(rid);
        System.out.println(Arrays.toString(tuple.getTupleByteArray()) + " " + in[0]);
        System.out.println(tuple.getLength());
        scan.closeScan();
        //System.out.println(scan.getNext(scan.getFirstRID()).getLength());
//        IndexInfo info = new IndexInfo();
//        info.setColumnNumber(12);
//        info.setFileName("Laveena");
//        info.setIndexType(new IndexType(1));
//        info.setValue(new IntegerValue(4));
//
//

//        columnarFile.getColumnarHeader().setIndex(info);
//        IndexInfo info1 = columnarFile.getColumnarHeader().getIndex(12, new IntegerValue(4), new IndexType(1) );
//        System.out.println(info1.getColumnNumber());
//        System.out.println(info1.getFileName());
//        System.out.println(info1.getValue().getValue());
        
        
        
        IndexInfo info2 = new IndexInfo();
        info2.setColumnNumber(6);
        info2.setFileName("Laveena");
        info2.setIndexType(new IndexType(1));
        info2.setValue(new StringValue("bachani"));

        
        columnarFile.getColumnarHeader().setIndex(info2);
        IndexInfo info3 = columnarFile.getColumnarHeader().getIndex(6, new StringValue("bachani"), new IndexType(1) );
        System.out.println(info3.getColumnNumber());
        System.out.println(info3.getFileName());
        System.out.println(info3.getValue().getValue());


        //SystemDefs.JavabaseBM.unpinPage(columnarFile.getColumnarHeader().getHeaderPageId(), false);
        //SystemDefs.JavabaseBM.flushAllPages();

        //columnarFile.deleteColumnarFile();
        //SystemDefs.JavabaseBM.flushAllPages();
        System.out.println(SystemDefs.JavabaseDB.getFileEntry(columnarFile.getColumnarHeader().getHdrFile()));


    }
}
