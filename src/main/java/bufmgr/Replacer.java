package bufmgr;

import global.GlobalConst;

class STATE {
    int state;
}

abstract class Replacer implements GlobalConst {
    protected BufMgr mgr;


    protected int head;
    protected STATE states[];

    public static final int Available = 12;
    public static final int Referenced = 13;
    public static final int Pinned = 14;

    public void pin(int frameNo) throws InvalidFrameNumberException {
        if (frameNo < 0 || frameNo >= mgr.getNumBuffers())
            throw new InvalidFrameNumberException(null, "BUFMGR: BAD_BUFFRAMENO.");

        mgr.getFrameTable()[frameNo].pin();
        states[frameNo].state = Pinned;
    }

    public boolean unpin(int frameNo) throws
            InvalidFrameNumberException, PageUnpinnedException {
        if (frameNo < 0 || frameNo >= mgr.getNumBuffers()) {
            throw new InvalidFrameNumberException(null, "BUFMGR: BAD_BUFFRAMENO.");
        }
        if (mgr.getFrameTable()[frameNo].getPinCount() == 0)
            throw new PageUnpinnedException(null, "BUFMGR: PAGE_NOT_PINNED.");
        mgr.getFrameTable()[frameNo].unpin();
        if (mgr.getFrameTable()[frameNo].getPinCount() == 0)
            states[frameNo].state = Referenced;
        return true;
    }

    public void free(int frameNo) throws PagePinnedException {
        if (mgr.getFrameTable()[frameNo].getPinCount() > 1)
            throw new PagePinnedException(null, "BUFMGR: PAGE_PINNED.");
        mgr.getFrameTable()[frameNo].unpin();
        states[frameNo].state = Available;
    }

    public abstract int pickVictim() throws BufferPoolExceededException, PagePinnedException;

    public abstract String name();

    public void info() {
        System.out.println("\nInfo:\nstate_bits:(R)eferenced | (A)vailable | (P)inned");
        int numBuffers = mgr.getNumBuffers();
        for (int i = 0; i < numBuffers; i++) {
            if ((i + 1) % 9 == 0)
                System.out.println("\n");
            System.out.println("(" + i + ")");
            switch (states[i].state) {
                case Referenced:
                    System.out.println("R\t");
                    break;
                case Available:
                    System.out.println("A\t");
                    break;
                case Pinned:
                    System.out.println("P\t");
                    break;
                default:
                    System.err.println("ERROR from Replacer.info()");
                    break;
            }
        }
        System.out.println("\n\n");
    }

    public int getNumUnpinnedBuffers() {
        int numBuffers = mgr.getNumBuffers();
        int answer = 0;
        for (int index = 0; index < numBuffers; ++index)
            if ((mgr.getFrameTable())[index].getPinCount() == 0)
                ++answer;
        return answer;
    }

    protected Replacer(BufMgr mgr) {
        this.mgr = mgr;
        int numBufers = mgr.getNumBuffers();
        states = new STATE[numBufers];
        for (int i = 0; i < numBufers; i++)
            states[i] = new STATE();
        head = -1;
    }

    protected void setBufferManager(BufMgr mgr) {
        this.mgr = mgr;
        int numBuffers = mgr.getNumBuffers();
        for (int index = 0; index < numBuffers; ++index) {
            states[index].state = Available;
        }
        head = -1;
    }


}
