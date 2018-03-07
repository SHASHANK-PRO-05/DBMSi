package bitmap;

import java.io.IOException;


import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.ColumnarFile;
import diskmgr.Page;
import global.*;


public class BitMapFile {


    private BitMapHeaderPage headerPage;
    private PageId headerPageId;
    private String fileName;
    private ColumnarFile columnarFile;


    /**
     * Access method to data member.
     *
     * @return Return a BitMapHeaderPage object that is the header page
     * of this bitmap file.
     */
    public BitMapHeaderPage getHeaderPage() {
        return headerPage;
    }


    private PageId getFileEntry(String fileName) throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.getFileEntry(fileName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    private Page pinPage(PageId pageId) throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageId, page, false/*Rdisk*/);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }


    private void addFileEntry(String fileName, PageId pageId)
            throws AddFileEntryException {
        try {
            SystemDefs.JavabaseDB.addFileEntry(fileName, pageId);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AddFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageId) throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageId, false);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    private void freePage(PageId pageId) throws FreePageException {
        try {
            SystemDefs.JavabaseBM.freePage(pageId);
        } catch (Exception e) {
            e.printStackTrace();
            throw new FreePageException(e, "");
        }

    }

    private void deleteFileEntry(String fileName)
            throws DeleteFileEntryException {
        try {
            SystemDefs.JavabaseDB.deleteFileEntry(fileName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DeleteFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageId, boolean dirty)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageId, dirty);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }


    public BitMapFile(String fileName) throws GetFileEntryException,
            PinPageException, ConstructPageException {
        headerPageId = getFileEntry(fileName);
        headerPage = new BitMapHeaderPage(headerPageId);
        this.fileName = fileName;
    }


    public BitMapFile(String fileName, ColumnarFile columnarFile, int columnNo, ValueClass value)
            throws GetFileEntryException,
            ConstructPageException, IOException, AddFileEntryException {

        headerPageId = getFileEntry(fileName);
        if (headerPageId == null) // file not exist
        {
            headerPage = new BitMapHeaderPage();
            headerPageId = headerPage.getCurrPage();
            addFileEntry(fileName, headerPageId);
            headerPage.setColumnIndex((short)columnNo);
            headerPage.setValueType((short)value.getValueType());
            this.columnarFile = columnarFile;
        } else {
            headerPage = new BitMapHeaderPage(headerPageId);
        }


        init(value);
        this.fileName = fileName;

    }


    /*
     * Initialize our bitMap
     */
    private boolean compareValues(int a, int b){
        if (a==b)
        return true;
        else
        return false;
    }
    private boolean compareValues(float a, float b){
        if (a==b)
            return true;
        else
            return false;
    }
    private boolean compareValues(String a, String b){
        if (a.equals(b))
            return true;
        else
            return false;
    }
    private boolean compareValues(int c,int a, int b){
        //a must be smaller than b
        if (c>= a && c<=b)
            return true;
        else
            return false;
    }
    private boolean compareValues(float c,float a, float b){
        //a must be smaller than b
        if (c>= a && c<=b)
            return true;
        else
            return false;
    }
    private boolean compareValues(String c,String a, String b){
        //a must be smaller than b
        if (c.compareTo(a)>=0 && c.compareTo(b)>=0)
            return true;
        else
            return false;
    }
    private void init(ValueClass value) {
        //initialize a sequential scan on the

        //Scan scan = columnarFile.openColumnScan(1);
        //check if scan object is not valid

        int val_type=value.getValueType();
        boolean flag=false;
        int position=0;


         /*check if the tuple match the value*/

         switch(val_type)
         {
             case 1:/*IntegerValue value_new1=scan.getnext();
                    IntegerValue value_given1=(IntegerValue) value;

                    do {
                        flag = compareValues(value_new1.getIntValue(), value_given1.getIntValue());
                        if (flag) {
                            //set bit at the given position as 1
                            insert(position);
                        } else {
                            position = position + 1;
                            // access next column value
                            value_new1 = scan.getnext();

                        }
                    }while(value_new1!=NULL); *///how the scan will end

             case 2:/*StringValue value_new2=scan.getnext();
                    StringValue value_given2=(StringValue) value;
                    flag=compareValues(value_new2.getStringValue(),value_given2.getStringValue());*/
             case 3:/*FloatValue value_new3=scan.getnext();
                    FloatValue value_given3=(FloatValue)value;
                    flag=compareValues(value_new3.getFloatValue(),value_given3.getFloatValue());*/
             /*
             Issue : How to know that value_new object is range as it will be a single value
             case 4:RangeIntValue value_new4=scan.getnext();
                    flag=compareValues(value_new4.getIntValue(),value.getIntValue1(),value.getIntValue2());
             case 5:RangeStringValue value_new5=scan.getnext();
                    flag=compareValues(value_new5.getStringValue(),value.getStringValue1(),value.getStringValue2());
             case 6:RangeFloatValue value_new6=scan.getnext();
                    flag=compareValues(value_new6.getFloatValue(),value.getFloatValue1(),value.getFloatValue2());
            */
         }



    }


    public void close() throws PageUnpinnedException,
            InvalidFrameNumberException, HashEntryNotFoundException,
            ReplacerException {
        if (headerPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
            headerPage = null;
        }
    }

    public void destroyBitMapFile() {

    }

    public boolean Delete(int position) {
        return false;
    }

    public boolean Insert(int position) {
        return false;

    }


}