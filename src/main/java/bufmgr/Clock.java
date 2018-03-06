package bufmgr;

public class Clock extends Replacer {
    public Clock(BufMgr mgr) {
        super(mgr);
    }

    @Override
    public int pickVictim() throws BufferPoolExceededException, PagePinnedException {
        int num = 0;
        int numBuffers = mgr.getNumBuffers();
        head = (head + 1) % numBuffers;
        while (states[head].state != Available) {
            if (states[head].state == Referenced)
                states[head].state = Available;
            if (num == 2 * numBuffers) {
                throw new BufferPoolExceededException(null, "BUFMGR: BUFFER_EXCEEDED.");
            }
            ++num;
            head = (head + 1) % numBuffers;
        }
        states[head].state = Pinned;
        mgr.getFrameTable()[head].pin();
        return head;
    }

    public final String name() {
        return "Clock";
    }

    public void info() {
        super.info();
        System.out.println("Clock hand:\t" + head);
        System.out.println("\n\n");
    }
}
