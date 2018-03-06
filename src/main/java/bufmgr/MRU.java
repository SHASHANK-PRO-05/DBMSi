package bufmgr;

public class MRU extends Replacer {
    private int frames[];

    public void setBufferManager(BufMgr mgr) {
        super.setBufferManager(mgr);
        int numBuffers = mgr.getNumBuffers();
        frames = new int[numBuffers];

        for (int index = 0; index < numBuffers; index++) {
            frames[index] = -index;
        }
        frames[0] = -numBuffers;
    }

    public void pin(int frameNo) throws InvalidFrameNumberException {
        super.pin(frameNo);
        update(frameNo);
    }

    public MRU(BufMgr mgr) {
        super(mgr);
        frames = null;
    }

    private void update(int frameNo) {
        int index;
        int numBuffers = mgr.getNumBuffers();

        for (index = 0; index < numBuffers; ++index)
            if (frames[index] < 0 || frames[index] == frameNo) break;

        if (frames[index] < 0) frames[index] = frameNo;

        int frame = frames[index];

        while (index-- != 0) {
            frames[index + 1] = frames[index];
        }
        frames[0] = frame;
    }

    @Override
    public int pickVictim() {
        int numBuffers = mgr.getNumBuffers();
        int i, frame;
        for (i = 0; i < numBuffers; i++) {
            if (frames[i] < 0) {
                if (i == 0) {
                    frames[i] = 0;
                } else {
                    frames[i] *= -1;
                }
                frame = frames[i];
                states[frame].state = Pinned;
                mgr.getFrameTable()[frame].pin();
                update(frame);
                return frame;
            }
        }
        for (i = 0; i < numBuffers; i++) {
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

    @Override
    public String name() {
        return "MRU";
    }

    public void info() {
        super.info();

        System.out.print("MRU REPLACEMENT");

        for (int i = 0; i < mgr.getNumBuffers(); i++) {
            if (i % 5 == 0)
                System.out.println();
            System.out.print("\t" + frames[i]);

        }
        System.out.println();
    }
}
