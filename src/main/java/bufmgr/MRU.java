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
    public MRU
}
