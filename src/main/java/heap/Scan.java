package heap;
import columnar.ColumnarFile;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class Scan implements GlobalConst {
  private ColumnarFile cf;

  public Scan(ColumnarFile cf) {
    init(cf);
  }

  public Tuple getNext() {
    throw new NotImplementedException();
  }

  public boolean position(RID rid) {
    throw new NotImplementedException();
  }

  private void init(ColumnarFile cf) {
    this.cf = cf;
    firstDataPage();
  }

  private void closeScan() {
    throw new NotImplementedException();
  }

  private void reset() {
    throw new NotImplementedException();
  }

  private boolean firstDataPage() {
    throw new NotImplementedException();
  }

  private boolean nextDataPage() {
    throw new NotImplementedException();
  }

  private boolean peekNext(RID rid) {
    throw new NotImplementedException();
  }

  private boolean mvNext(RID rid) {
    throw new NotImplementedException();
  }

  /**
   * Shortcut method to pin page
   * @param pgNo      Page number in our column store
   * @param page      The pointer to the page
   * @param emptyPage try (empty page); false (non-empty page)
   * @throws HFBufMgrException General Exception
   */
  private void pinPage(PageId pgNo, Page page, boolean emptyPage) throws HFBufMgrException {
    try {
      SystemDefs.JavabaseBM.pinPage(pgNo, page, emptyPage);
    } catch (Exception e) {
      throw new HFBufMgrException(e, "Scan.java: pinPage() failed");
    }
  }

  /**
   * Shortcut method to unpin page
   * @param pgNo  Page ID
   * @param dirty The dirty bit of the frame
   * @throws HFBufMgrException General exception
   */
  private void unpinPage(PageId pgNo, boolean dirty) throws HFBufMgrException {
    try {
      SystemDefs.JavabaseBM.unpinPage(pgNo, dirty);
    } catch (Exception e) {
      throw new HFBufMgrException(e, "Scan.java: unpinPage() failed");
    }
  }
}
