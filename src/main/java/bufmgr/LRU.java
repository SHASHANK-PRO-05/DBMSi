package bufmgr;

public class LRU extends Replacer {
    private int frames[];
    private int numberFramesUsed;

    private void update(int frameNo) {
        int index;
        for (index = 0; index < numberFramesUsed; index++)
            if (frames[index] == frameNo)
                break;
        while (++index < numberFramesUsed) {
            frames[index - 1] = frames[index];
        }
        frames[numberFramesUsed - 1] = frameNo;
    }

    public void setBufferManager(BufMgr mgr) {
        super.setBufferManager(mgr);
        frames = new int[mgr.getNumBuffers()];
        numberFramesUsed = 0;
    }

    public LRU(BufMgr mgr) {
        super(mgr);
        frames = null;
    }

    public void pin(int frameNo) throws InvalidFrameNumberException {
        super.pin(frameNo);
        update(frameNo);
    }

    public int pickVictim() {
        int numBuffers = mgr.getNumBuffers();
        int frame;

        if (numberFramesUsed < numBuffers) {
            frame = numberFramesUsed++;
            frames[frame] = frame;
            states[frame].state = Pinned;
            mgr.getFrameTable()[frame].pin();
            return frame;
        }

        for (int i = 0; i < numBuffers; i++) {
            frame = frames[i];
            if (states[frame].state != Pinned) {
                states[frame].state = Pinned;
                mgr.getFrameTable()[frame].pin();
                update(frame);
                return frame;
            }
        }
        return INVALID_PAGE;
    }

    public String name() {
        return "LRU";
    }

    public void info() {
        super.info();

        System.out.print("LRU REPLACEMENT");

        for (int i = 0; i < numberFramesUsed; i++) {
            if (i % 5 == 0)
                System.out.println();
            System.out.print("\t" + frames[i]);

        }
        System.out.println();
    }
}
