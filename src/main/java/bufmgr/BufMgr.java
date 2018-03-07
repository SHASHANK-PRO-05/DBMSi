package bufmgr;


import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;


class BufHTEntry {
    public BufHTEntry next;
    public PageId pageId = new PageId();
    public int frameNo;
}

class BufHashTbl implements GlobalConst {
    private static final int HTSIZE = HASH_TABLE_SIZE;

    private BufHTEntry[] hashTable = new BufHTEntry[HTSIZE];

    private int hash(PageId pageId) {
        return pageId.pid % HTSIZE;
    }

    public BufHashTbl() {

    }

    public boolean insert(PageId pageId, int frameNo) {
        BufHTEntry ent = new BufHTEntry();
        int index = hash(pageId);

        ent.pageId.pid = pageId.pid;
        ent.frameNo = frameNo;

        ent.next = hashTable[index];
        hashTable[index] = ent;
        return true;
    }

    public int lookup(PageId pageId) {

        if (pageId.pid == INVALID_PAGE) return INVALID_PAGE;

        for (BufHTEntry ent = hashTable[hash(pageId)]; ent != null; ent = ent.next) {
            if (ent.pageId.pid == pageId.pid) return ent.frameNo;
        }
        return INVALID_PAGE;
    }

    public boolean remove(PageId pageId) {

        if (pageId.pid == INVALID_PAGE) return true;
        BufHTEntry cur, prev = null;

        int index = hash(pageId);

        for (cur = hashTable[index]; cur != null; cur = cur.next) {
            if (cur.pageId.pid == pageId.pid) break;
            prev = cur;
        }
        if (cur != null) {
            if (prev != null) {
                prev.next = cur.next;
                cur.next = null;
            } else {
                hashTable[index] = cur.next;
                cur.next = null;
            }
        } else {
            System.err.println("ERROR: Page " + pageId.pid
                    + " was not found in hashtable.\n");
            return false;
        }
        return true;
    }

    public void display() {
        BufHTEntry cur;

        System.out.println("HASH Table contents :FrameNo[PageNo]");

        for (int i = 0; i < HTSIZE; i++) {
            if (hashTable[i] != null) {
                for (cur = hashTable[i]; cur != null; cur = cur.next) {
                    System.out.println(cur.frameNo + "[" + cur.pageId.pid + "]-");
                }
                System.out.println("\t\t");
            } else {
                System.out.println("NONE\t");
            }
        }
        System.out.println("");
    }
}

class FrameDesc implements GlobalConst {
    public PageId pageId;
    public boolean dirty;
    private int pinCount;

    public FrameDesc() {
        pageId = new PageId();
        pageId.pid = INVALID_PAGE;
        dirty = false;
        pinCount = 0;
    }

    public int getPinCount() {
        return pinCount;
    }

    public int pin() {
        return ++pinCount;
    }

    public int unpin() {
        pinCount = (pinCount <= 0) ? 0 : pinCount - 1;
        return pinCount;
    }
}

public class BufMgr implements GlobalConst {

    private BufHashTbl hashTbl = new BufHashTbl();

    private int numBuffers;

    private FrameDesc[] frameTable;

    private byte[][] bufPool;

    private Replacer replacer;

    private void writePage(PageId pageno, Page page)
            throws BufMgrException {
        try {
            SystemDefs.JavabaseDB.writePage(pageno, page);
        } catch (Exception e) {
            throw new BufMgrException(e, "BufMgr.java: write_page() failed");
        }
    }

    private void readPage(PageId pageId, Page page)
            throws BufMgrException {
        try {
            SystemDefs.JavabaseDB.readPage(pageId, page);
        } catch (Exception e) {
            throw new BufMgrException(e, "BufMgr.java: read_page() failed");
        }

    }

    private void privFlushPages(PageId pageId, int allPages) throws
            PageNotFoundException, BufMgrException,
            PagePinnedException, HashOperationException {
        int i;
        int unpinned = 0;

        for (i = 0; i < numBuffers; i++) {
            if (allPages != 0 || (frameTable[i].pageId.pid == pageId.pid)) {
                if (frameTable[i].getPinCount() != 0)
                    unpinned++;

                if (frameTable[i].dirty) {
                    if (frameTable[i].pageId.pid == INVALID_PAGE)
                        throw new PageNotFoundException(null, "BUFMGR: INVALID_PAGE_NO");

                    pageId.pid = frameTable[i].pageId.pid;

                    Page page = new Page(bufPool[i]);
                    writePage(pageId, page);
                    try {
                        hashTbl.remove(pageId);
                    } catch (Exception e) {
                        throw new HashOperationException(e, "BUFMGR: HASH_TBL_ERROR.");
                    }
                    frameTable[i].pageId.pid = INVALID_PAGE;
                    frameTable[i].dirty = false;
                }

                if (allPages == 0) {
                    if (unpinned != 0)
                        throw new PagePinnedException(null, "BUFMGR: PAGE_PINNED.");
                }
            }
        }
        if (allPages != 0) {
            if (unpinned != 0)
                throw new PagePinnedException(null, "BUFMGR: PAGE_PINNED.");
        }
    }


    public BufMgr(int numBuffers, String replacerAlgorithm) {
        this.numBuffers = numBuffers;
        frameTable = new FrameDesc[numBuffers];
        bufPool = new byte[numBuffers][MINIBASE_PAGESIZE];
        frameTable = new FrameDesc[numBuffers];

        for (int i = 0; i < numBuffers; i++)
            frameTable[i] = new FrameDesc();

        if (replacerAlgorithm == null) {
            replacer = new Clock(this);
        } else {
            if (replacerAlgorithm.equals("Clock")) {
                replacer = new Clock(this);
                System.out.println("Replacer: Clock\n");
            } else if (replacerAlgorithm.equals("LRU")) {
                replacer = new LRU(this);
                System.out.println("Replacer: LRU");
            } else if (replacerAlgorithm.equals("MRU")) {
                replacer = new MRU(this);
                System.out.println("Replacer: MRU");
            } else {
                replacer = new Clock(this);
                System.out.println("Replacer:Unknown, Use Clock\n");
            }
        }

        replacer.setBufferManager(this);
    }

    private void bmhashdisplay() {
        hashTbl.display();
    }


    public void pinPage(PageId pageId, Page page, boolean emptyPage) throws
            InvalidFrameNumberException, PagePinnedException
            , BufferPoolExceededException, ReplacerException
            , HashOperationException, BufMgrException
            , PageUnpinnedException, PageNotReadException {
        int frameNo = hashTbl.lookup(pageId);
        boolean hashTableRemove, hashTableInsert;
        PageId oldPageId = new PageId(-1);
        boolean needWrite = false;
        if (frameNo < 0) {
            frameNo = replacer.pickVictim();
            if (frameNo < 0) {
                page = null;
                throw new ReplacerException(null, "BUFMGR: REPLACER_ERROR.");
            }
            if (frameTable[frameNo].pageId.pid != INVALID_PAGE
                    && frameTable[frameNo].dirty) {
                needWrite = true;
                oldPageId.pid = frameTable[frameNo].pageId.pid;
            }

            hashTableRemove = hashTbl.remove(frameTable[frameNo].pageId);
            if (!hashTableRemove) {
                throw new HashOperationException(null, "BUFMGR: HASH_TABLE_ERROR.");
            }

            frameTable[frameNo].pageId.pid = INVALID_PAGE;
            frameTable[frameNo].dirty = false;

            hashTableInsert = hashTbl.insert(pageId, frameNo);

            (frameTable[frameNo].pageId).pid = pageId.pid;
            frameTable[frameNo].dirty = false;

            if (!hashTableInsert)
                throw new HashOperationException(null, "BUFMGR: HASH_TABLE_ERROR.");

            //This is created for writing down in disk
            Page tempPage = new Page(bufPool[frameNo]);

            if (needWrite) writePage(oldPageId, tempPage);

            if (!emptyPage) {
                try {
                    readPage(pageId, tempPage);
                } catch (Exception e) {
                    hashTableRemove = hashTbl.remove(frameTable[frameNo].pageId);
                    if (!hashTableRemove) {
                        throw new HashOperationException(e, "BUFMGR: HASH_TABLE_ERROR.");
                    }

                    frameTable[frameNo].pageId.pid = INVALID_PAGE;
                    frameTable[frameNo].dirty = false;

                    hashTableRemove = replacer.unpin(frameNo);
                    if (hashTableRemove != true)
                        throw new ReplacerException(e, "BUFMGR: REPLACER_ERROR.");
                    throw new PageNotReadException(e, "BUFMGR: DB_READ_PAGE_ERROR.");
                }
            } else {
                bufPool[frameNo] = new byte[MINIBASE_PAGESIZE];
            }
            page.setPage(bufPool[frameNo]);

        } else {
            page.setPage(bufPool[frameNo]);
            replacer.pin(frameNo);
        }

    }

    public PageId newPage(Page firstPage, int howMany)
            throws BufMgrException {

        PageId firstPageId = new PageId();
        allocatePage(firstPageId, howMany);
        try {
            pinPage(firstPageId, firstPage, true);
        } catch (Exception e) {
            deallocatePage(firstPageId, howMany);
            return null;
        }
        return firstPageId;
    }

    public void freePage(PageId pageId)
            throws BufMgrException, InvalidBufferException,
            ReplacerException, HashOperationException {
        int frameNo = hashTbl.lookup(pageId);
        if (frameNo < 0) {
            deallocatePage(pageId, 1);
            return;
        }
        if (frameNo >= (int) numBuffers) {
            throw new InvalidBufferException(null, "BUFMGR, BAD_BUFFER");
        }
        try {
            replacer.free(frameNo);
        } catch (Exception e) {
            throw new ReplacerException(e, "BUFMGR, REPLACER_ERROR");
        }

        try {
            hashTbl.remove(frameTable[frameNo].pageId);
        } catch (Exception e) {
            throw new HashOperationException(e, "BUFMGR, HASH_TABLE_ERROR");
        }
        frameTable[frameNo].pageId.pid = INVALID_PAGE;
        frameTable[frameNo].dirty = false;

        deallocatePage(pageId, 1);
    }

    public void flushPage(PageId pageid)
            throws HashOperationException,
            PagePinnedException,
            PageNotFoundException,
            BufMgrException {
        privFlushPages(pageid, 0);
    }

    public void flushAllPages()
            throws HashOperationException,
            PagePinnedException,
            PageNotFoundException,
            BufMgrException {
        PageId pageId = new PageId(INVALID_PAGE);
        privFlushPages(pageId, 1);
    }

    private void allocatePage(PageId pageId, int num)
            throws BufMgrException {
        try {
            SystemDefs.JavabaseDB.allocatePage(pageId, num);
        } catch (Exception e) {
            throw new BufMgrException(e, "BufMgr.java: allocate_page() failed");
        }

    }

    public void unpinPage(PageId pageId, boolean dirty)
            throws HashEntryNotFoundException, InvalidFrameNumberException
            , PageUnpinnedException, ReplacerException {
        int frameNo = hashTbl.lookup(pageId);
        if (frameNo < 0) {
            throw new HashEntryNotFoundException(null, "BUFMGR: HASH_NOT_FOUND.");
        }
        if (frameTable[frameNo].pageId.pid == INVALID_PAGE) {
            throw new InvalidFrameNumberException(null, "BUFMGR: BAD_FRAMENO.");
        }
        if (!replacer.unpin(frameNo)) {
            throw new ReplacerException(null, "BUFMGR: REPLACER_ERROR.");
        }

        if (dirty)
            frameTable[frameNo].dirty = true;
    }


    private void deallocatePage(PageId pageno, int runSize)
            throws BufMgrException {
        try {
            SystemDefs.JavabaseDB.deallocatePage(pageno, runSize);
        } catch (Exception e) {
            throw new BufMgrException(e, "BufMgr.java: deallocate_page() failed");
        }
    }

    public FrameDesc[] getFrameTable() {
        return frameTable;
    }

    public int getNumBuffers() {
        return numBuffers;
    }
}
