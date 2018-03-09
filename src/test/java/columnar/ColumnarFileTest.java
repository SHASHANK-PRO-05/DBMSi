package columnar;

import bufmgr.LRU;
import global.*;
import heap.Tuple;

import org.junit.Test;

public class ColumnarFileTest {
    @Test
    public void creationColumnar() throws Exception {
        String dbPath = "Minibase.min";
        SystemDefs systemDefs = new SystemDefs(dbPath, 3000
                , 400, "LRU");
        AttrType[] attrTypes = new AttrType[20];
        int[] in = new int[20];
        int [] b = new int[20];

        for (int i = 0; i < 20; i++) {
            attrTypes[i] = new AttrType();
            attrTypes[i].setColumnId(i);
            attrTypes[i].setSize(4);
            attrTypes[i].setAttrType(1);
            attrTypes[i].setAttrName("Column" + i);
            in[i] = (int) (Math.random() * 40);
            b[i] = 20;
        }
        System.out.println("Random value "+in[3]);
        ColumnarFile columnarFile = new ColumnarFile("Employee", 20, attrTypes);
        SystemDefs.JavabaseBM.flushAllPages();
        columnarFile = new ColumnarFile("Employee");
        SystemDefs.JavabaseBM.pinPage(columnarFile.getColumnarHeader().getHeaderPageId()
                , columnarFile.getColumnarHeader(), false);
        System.out.println(columnarFile.getColumnarHeader().getNextPage().pid);
        System.out.println(columnarFile.getColumnarHeader().getColumnCount());
        columnarFile.getColumnarHeader().getColumns();
        TID tid = columnarFile.insertTuple(Convert.intAtobyteA(in));
        //TID tid = columnarFile.insertTuple(Convert.intAtobyteA(b));
        Tuple newTuple = new Tuple(Convert.intAtobyteA(b),0,Convert.intAtobyteA(b).length);
        boolean value= columnarFile.updateTuple(tid, newTuple);
        System.out.println(value);

     /*   IndexInfo info = new IndexInfo();
        info.setColumnNumber(12);
        info.setFileName("Laveena");
        info.setIndexType(new IndexType(1));
        info.setValue(new IntegerValue(4));


        columnarFile.getColumnarHeader().setIndex(info);
        IndexInfo info1 = columnarFile.getColumnarHeader().getIndex(12, new IndexType(1));
        System.out.println(info1.getColumnNumber());
        System.out.println(info1.getFileName());


        SystemDefs.JavabaseBM.unpinPage(columnarFile.getColumnarHeader().getHeaderPageId(), false);
        SystemDefs.JavabaseBM.flushAllPages();
*/
        columnarFile.deleteColumnarFile();
     //   SystemDefs.JavabaseBM.flushAllPages();
      //  System.out.println(SystemDefs.JavabaseDB.getFileEntry(columnarFile.getColumnarHeader().getHdrFile()));




        
    }
    
    @Test
    public void getTupleTest() throws Exception {
    	
    }
    
}
