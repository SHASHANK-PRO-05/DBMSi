package columnar;

import java.io.IOException;

import diskmgr.FileNameTooLongException;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;

public class ColumnarFile implements GlobalConst {

    public ColumnarHeader columnarHeader;
    public Heapfile heapFileNames[];
    static int numColumns;
    AttrType[] type;

    public ColumnarFile(String fileName, int numColumns, AttrType[] type)
            throws ColumnClassCreationException, HFDiskMgrException,
            IOException {

        try {
            columnarHeader = new ColumnarHeader(fileName, numColumns, type);
            for (int i = 0; i < numColumns; i++) {
                String fileNum = Integer.toString(i);
                String columnsFileName = fileName + "." + fileNum;
                heapFileNames[i] = new Heapfile(columnsFileName);
            }
        } catch (Exception e) {
            for (int i = 0; i < numColumns; i++) {
                String fileNum = Integer.toString(i);
                String columnsFileName = fileName + "." + fileNum;
                deleteFileEntry(columnsFileName);
            }
            PageId pageId = columnarHeader.getCurPage();
            PageId nextPage = columnarHeader.getNextPage();

            while (pageId.pid != INVALID_PAGE) {
                deallocatePage(pageId);
                pageId.pid = nextPage.pid;
            }
            throw new ColumnClassCreationException(e
                    , "ColumnarFile: not able to create a file");
        }


    }

    public void deleteColumnarFile()
            throws InvalidSlotNumberException,
            FileAlreadyDeletedException,
            InvalidTupleSizeException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {

        deleteFileEntry(columnarHeader.hdrFile);
        for (int i = 0; i < numColumns; i++) {
            heapFileNames[i].deleteFile();
            deleteFileEntry(heapFileNames[i].toString());
        }

        deleteFileEntry(columnarHeader.hdrFile);

    }

    private void deallocatePage(PageId pageId) throws HFDiskMgrException {
        try {
            SystemDefs.JavabaseDB.deallocatePage(pageId, 1);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: deallocatePage failed");
        }
    }

    private void deleteFileEntry(String filename)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.deleteFileEntry(filename);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: delete_file_entry() failed");
        }

    }
}