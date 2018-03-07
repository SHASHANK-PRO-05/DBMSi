package diskmgr;

import global.GlobalConst;
import global.PageId;
import global.SystemDefs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ColumnDB implements GlobalConst {
    private static final int bitsPerPage = MINIBASE_PAGESIZE * 8;
    private RandomAccessFile filePointer;
    private int numPages;
    private String fName;
    private int numOfMapPages;

    public ColumnDB() {

    }

    public void openDB(String fName)
            throws IOException, DiskMgrException {
        this.fName = fName;
        filePointer = new RandomAccessFile(this.fName, "rw");
        PageId pageId = new PageId();
        pageId.pid = 0;
        Page page = new Page();
        numPages = 1;
        pinPage(pageId, page, false);
        DBFirstPage dbFirstPage = new DBFirstPage();
        dbFirstPage.openPage(page);
        numPages = dbFirstPage.getNumDBPages();
        unpinPage(pageId, false);
    }

    public void openDB(String fName, int numPages) throws IOException
            , DiskMgrException, InvalidPageNumberException {
        this.fName = fName;
        this.numPages = numPages > 2 ? numPages : 2;
        this.numOfMapPages = (numPages + BITS_PER_PAGE - 1) / BITS_PER_PAGE;
        SystemDefs.pCounter.initialize();
        //Just making sure the file is deleted
        File dbFile = new File(this.fName);
        dbFile.delete();


        this.filePointer = new RandomAccessFile(this.fName, "rw");
        this.filePointer.seek((long) this.numPages * MINIBASE_PAGESIZE - 1);
        this.filePointer.writeByte(0);
        SystemDefs.pCounter.writeIncrement();
        Page page = new Page();
        PageId pageId = new PageId();
        pageId.pid = 0;
        pinPage(pageId, page, true);
        DBFirstPage firstPage = new DBFirstPage(page);

        DBFirstPage dbFirstPage = new DBFirstPage(page);
        firstPage.setNumDBPages(numPages);

        unpinPage(pageId, true);

        int numMapPages = (numPages + bitsPerPage - 1) / bitsPerPage;

        setBits(pageId, 1 + numMapPages, 1);
    }

    public void closeDB() throws IOException {
        filePointer.close();
    }

    public void DBDestroy()
            throws IOException {

        filePointer.close();
        File DBfile = new File(fName);
        DBfile.delete();
    }

    public void readPage(PageId pageId, Page apage)
            throws InvalidPageNumberException,
            FileIOException,
            IOException {

        if ((pageId.pid < 0) || (pageId.pid >= numPages))
            throw new InvalidPageNumberException(null, "BAD_PAGE_NUMBER");

        filePointer.seek((long) (pageId.pid * MINIBASE_PAGESIZE));

        byte[] buffer = apage.getPage();
        try {
            filePointer.read(buffer);
        } catch (IOException e) {
            throw new FileIOException(e, "DB file I/O error");
        }
        SystemDefs.pCounter.readIncrement();
    }


    public void writePage(PageId pageNo, Page page) throws
            InvalidPageNumberException, IOException
            , FileIOException {
        if ((pageNo.pid < 0) || (pageNo.pid >= numPages))
            throw new InvalidPageNumberException(null, "INVALID_PAGE_NUMBER");
        filePointer.seek((long) pageNo.pid * MINIBASE_PAGESIZE);

        try {
            filePointer.write(page.getPage());
        } catch (IOException e) {
            throw new FileIOException(e, "DB file I/O error");
        }
        SystemDefs.pCounter.writeIncrement();
    }

    public void allocatePage(PageId startPageNum, int runSize)
            throws InvalidRunSizeException, DiskMgrException
            , OutOfSpaceException, InvalidPageNumberException {
        if (runSize < 0) throw new InvalidRunSizeException(null, "Negative run_size");
        int currentRunStart = 0;
        int currentRunLength = 0;
        PageId pageId = new PageId();
        byte[] pageBuffer;
        int bytePtr;
        for (int i = 0; i < numOfMapPages; ++i) {
            pageId.pid = i + 1;
            Page page = new Page();
            pinPage(pageId, page, false);
            pageBuffer = page.getPage();
            bytePtr = 0;
            int numBitsThisPage = numPages - i * bitsPerPage;
            if (numBitsThisPage > bitsPerPage) numBitsThisPage = bitsPerPage;
            for (; numBitsThisPage > 0 && currentRunLength < runSize; ++bytePtr) {
                Byte mask = (new Integer(1)).byteValue();
                byte byteMask = mask.byteValue();

                while (mask.intValue() != 0 && numBitsThisPage > 0
                        && currentRunLength < runSize) {
                    if ((pageBuffer[bytePtr] & byteMask) != 0) {
                        currentRunStart += currentRunLength + 1;
                        currentRunLength = 0;
                    } else {
                        currentRunLength++;
                    }
                    byteMask <<= 1;
                    mask = byteMask;
                    --numBitsThisPage;
                }
            }
            unpinPage(pageId, false);
        }
        if (currentRunLength >= runSize) {
            startPageNum.pid = currentRunStart;
            setBits(startPageNum, runSize, 1);
            return;
        }

        throw new OutOfSpaceException(null, "No space left");
    }

    private void setBits(PageId startPage, int runSize, int bit)
            throws InvalidPageNumberException, DiskMgrException {
        if (startPage.pid < 0 || startPage.pid + runSize > numPages) {
            throw new InvalidPageNumberException(null, "Bad page number");
        }
        int firstMapPage = startPage.pid / bitsPerPage + 1;
        int lastMapPage = (startPage.pid + runSize - 1) / bitsPerPage + 1;
        int firstBitNo = startPage.pid % bitsPerPage;
        for (PageId pageId = new PageId(firstMapPage);
             pageId.pid <= lastMapPage;
             pageId.pid++, firstBitNo = 0) {
            Page page = new Page();
            pinPage(pageId, page, false);
            byte[] pageBuffer = page.getPage();
            int firstByteNo = firstBitNo / 8;
            int firstBitOffset = firstBitNo % 8;
            int lastBitNo = firstBitNo + runSize - 1;
            if (lastBitNo >= bitsPerPage) lastBitNo = bitsPerPage - 1;

            int lastByteNo = lastBitNo / 8;
            int curPosition = firstByteNo;
            for (; curPosition <= lastByteNo; ++curPosition, firstBitOffset = 0) {
                int maxBitsThisByte = 8 - firstBitOffset;
                int numBitsThisByte = (runSize > maxBitsThisByte ?
                        maxBitsThisByte : runSize);
                int integerMask = 1;
                int temp;
                integerMask = ((integerMask << numBitsThisByte) - 1) << firstBitOffset;
                byte byteMask = new Integer(integerMask).byteValue();
                if (bit == 1) {
                    temp = (pageBuffer[curPosition] | byteMask);
                    integerMask = new Integer(temp);
                    pageBuffer[curPosition] = new Integer(integerMask).byteValue();
                } else {
                    temp = pageBuffer[curPosition] & (255 ^ byteMask);
                    integerMask = temp;
                    pageBuffer[curPosition] = new Integer(integerMask).byteValue();
                }
                runSize -= numBitsThisByte;
            }
            unpinPage(pageId, true);
        }
    }

    public void addFileEntry(String fName, PageId pageId)
            throws FileNameTooLongException, InvalidPageNumberException
            , DuplicateEntryException, IOException
            , DiskMgrException {
        if (fName.length() >= MAX_NAME)
            throw new FileNameTooLongException(null, "DB filename too long");
        if ((pageId.pid < 0) || (pageId.pid >= numPages))
            throw new InvalidPageNumberException(null, " DB bad page number");

        if (getFileEntry(fName) != null)
            throw new DuplicateEntryException(null, "DB fileentry already exists");

        Page page = new Page();
        boolean found = false;
        int freeSlot = 0;
        PageId headerPageId = new PageId();
        PageId nextHeaderPageId = new PageId(0);
        DBHeaderPage dbHeaderPage;
        do {
            headerPageId.pid = nextHeaderPageId.pid;
            pinPage(headerPageId, page, false);
            if (headerPageId.pid == 0)
                dbHeaderPage = new DBFirstPage();
            else
                dbHeaderPage = new DBDirectoryPage();
            dbHeaderPage.openPage(page);

            nextHeaderPageId = dbHeaderPage.getNextPage();

            int entry = 0;
            PageId tempPageId = new PageId();

            int sizeOfEntries = dbHeaderPage.getNumOfEntries();
            while (entry < dbHeaderPage.getNumOfEntries()) {
                dbHeaderPage.getFileEntry(tempPageId, entry);
                if (tempPageId.pid == INVALID_PAGE) break;
                entry++;
            }

            if (entry < sizeOfEntries) {
                freeSlot = entry;
                found = true;
            } else if (nextHeaderPageId.pid != INVALID_PAGE) {
                unpinPage(headerPageId, false);
            }
        } while ((nextHeaderPageId.pid != INVALID_PAGE) && (!found));

        if (!found) {
            try {
                allocatePage(nextHeaderPageId, 1);
            } catch (Exception e) {
                unpinPage(headerPageId, false);
                e.printStackTrace();
            }
            dbHeaderPage.setNextPage(nextHeaderPageId);
            unpinPage(headerPageId, true);
            headerPageId.pid = nextHeaderPageId.pid;
            pinPage(headerPageId, page, true/*no diskIO*/);
            dbHeaderPage = new DBDirectoryPage(page);
            freeSlot = 0;
        }
        dbHeaderPage.setFileEntry(pageId, fName, freeSlot);

        unpinPage(headerPageId, true);
    }

    public void deleteFileEntry(String fName) throws DiskMgrException
            , IOException, FileEntryNotFoundException {
        Page page = new Page();
        boolean found = false;
        int slot = 0;
        PageId headerPageId = new PageId();
        PageId nextHeaderPageId = new PageId(0);
        PageId tempPageId = new PageId();
        DBHeaderPage dbHeaderPage;
        do {
            headerPageId.pid = nextHeaderPageId.pid;
            pinPage(headerPageId, page, false);
            if (headerPageId.pid == 0)
                dbHeaderPage = new DBFirstPage();
            else
                dbHeaderPage = new DBDirectoryPage();
            dbHeaderPage.openPage(page);
            nextHeaderPageId = dbHeaderPage.getNextPage();
            int entry = 0;
            String tempName;
            int sizeOfEntries = dbHeaderPage.getNumOfEntries();
            while (entry < sizeOfEntries) {
                tempName = dbHeaderPage.getFileEntry(tempPageId, entry);
                if ((tempPageId.pid != INVALID_PAGE) &&
                        (tempName.compareTo(fName) == 0)) break;
                entry++;
            }
            if (entry < sizeOfEntries) {
                slot = entry;
                found = true;
            } else {
                unpinPage(headerPageId, false /*undirty*/);
            }
        } while (nextHeaderPageId.pid != INVALID_PAGE && !found);
        if (!found)  // Entry not found - nothing deleted
            throw new FileEntryNotFoundException(null, "DB file not found");
        tempPageId.pid = INVALID_PAGE;
        dbHeaderPage.setFileEntry(tempPageId, "\0", slot);
        unpinPage(headerPageId, true);
    }

    public PageId getFileEntry(String fName)
            throws IOException, DiskMgrException {
        Page page = new Page();
        boolean found = false;
        int slot = 0;
        PageId headerPid = new PageId();
        PageId nextHeaderPageId = new PageId(0);
        DBHeaderPage dbHeaderPage;

        do {
            headerPid.pid = nextHeaderPageId.pid;
            pinPage(headerPid, page, false);
            if (headerPid.pid == 0) {
                dbHeaderPage = new DBFirstPage();
                dbHeaderPage.openPage(page);
            } else {
                dbHeaderPage = new DBDirectoryPage();
                dbHeaderPage.openPage(page);
            }
            nextHeaderPageId = dbHeaderPage.getNextPage();
            int entry = 0;
            PageId tempId = new PageId();
            String tempFileName;
            int sizeOfEntry = dbHeaderPage.getNumOfEntries();
            while (entry < sizeOfEntry) {
                tempFileName = dbHeaderPage.getFileEntry(tempId, entry);
                if (tempId.pid != INVALID_PAGE && tempFileName.compareTo(fName) == 0) break;
                entry++;
            }
            if (entry < sizeOfEntry) {
                slot = entry;
                found = true;
            }
            unpinPage(headerPid, false);
        } while (nextHeaderPageId.pid != INVALID_PAGE && (!found));

        if (!found) return null;

        PageId startPid = new PageId();
        dbHeaderPage.getFileEntry(startPid, slot);
        return startPid;
    }


    public void deallocatePage(PageId startPageNum, int runSize)
            throws InvalidRunSizeException,
            InvalidPageNumberException,
            DiskMgrException {

        if (runSize < 0) throw new InvalidRunSizeException(null, "Negative run_size");

        setBits(startPageNum, runSize, 0);
    }

    private void unpinPage(PageId pageId, boolean dirty)
            throws DiskMgrException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageId, dirty);
        } catch (Exception e) {
            throw new DiskMgrException(e, "DB.java: unpinPage() failed");
        }
    }

    private void pinPage(PageId pageNo, Page page, boolean emptyPage)
            throws DiskMgrException {
        try {
            SystemDefs.JavabaseBM.pinPage(pageNo, page, emptyPage);
        } catch (Exception e) {
            throw new DiskMgrException(e, "DB.java: pinPage() failed");
        }
    }
}
