package diskmgr;

import global.GlobalConst;

public class Page implements GlobalConst {
    protected byte[] data;

    public Page() {
        data = new byte[MINIBASE_PAGESIZE];
    }

    public Page(byte[] data) {
        this.data = data;
    }

    public byte[] getPage() {
        return data;
    }

    public void setPage(byte[] data) {
        this.data = data;
    }
}
