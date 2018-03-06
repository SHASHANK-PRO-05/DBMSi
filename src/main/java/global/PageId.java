package global;

public class PageId {
    public int pid;

    public PageId() {

    }

    public PageId(int pageNo) {
        pid = pageNo;
    }

    public void copyPageId(PageId pageNo) {
        this.pid = pageNo.pid;
    }

    @Override
    public String toString() {
        return String.valueOf(this.pid);
    }
}

