package bufmgr;

import diskmgr.Page;
import global.GlobalConst;
import global.PageId;

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

            }
        }


    }

    public FrameDesc[] getFrameTable() {
        return frameTable;
    }

    public int getNumBuffers() {
        return numBuffers;
    }
}
