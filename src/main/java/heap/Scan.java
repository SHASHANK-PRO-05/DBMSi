package heap;

import columnar.ColumnarFile;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

public class Scan implements GlobalConst {
  private ColumnarFile cf;
  private PageId dirPageId = new PageId();
  private HFPage dirPage = new HFPage();
  private RID dataPageRID = new RID();
  private PageId dataPageId = new PageId();
  private HFPage dataPage = new HFPage();
  private RID userRID = new RID();
  /**
   * Column Index no in the Columnar File starting from 0
   */
  private short columnNo;
  private boolean nextUserStatus;

  public Scan(ColumnarFile cf, short columnNo) {
    init(cf, columnNo);
  }

  public Tuple getNext(RID rid) {
    Tuple recptrTuple = null;

    if (!nextUserStatus) {
      nextDataPage();
    }

    if (dataPage == null)
      return null;

    rid.pageNo.pid = userRID.pageNo.pid;
    rid.slotNo = userRID.slotNo;

    try {
      recptrTuple = dataPage.getRecord(rid);
      userRID = dataPage.nextRecord(rid);
      nextUserStatus = userRID != null;
    } catch (Exception e) {
      //    System.err.println("SCAN: Error in Scan" + e);
      e.printStackTrace();
    }

    return recptrTuple;
  }

  public boolean position(RID rid) {
    RID nextRID = new RID();
    boolean bst;

    if (nextRID.equals(rid)) {
      return true;
    }

    // This is kind lame, but otherwise it will take all day.
    PageId pgId = new PageId();
    pgId.pid = rid.pageNo.pid;

    if (!dataPageId.equals(pgId)) {
      reset();

      bst = firstDataPage();

      if (!bst) {
        return false;
      }

      while (!dataPageId.equals(pgId)) {
        bst = nextDataPage();
        if (!bst)
          return bst;
      }
    }

    // Now we are on the correct page.

    try {
      userRID = dataPage.firstRecord();
    } catch (Exception e) {
      e.printStackTrace();
    }

    if (userRID == null) {
      return false;
    }

    bst = peekNext(nextRID);

    while ((bst) && (nextRID != rid)) {
      bst = mvNext(nextRID);
    }

    return bst;
  }

  private void init(ColumnarFile cf, short columnNo) {
    this.cf = cf;
    this.columnNo = columnNo;
    if (!firstDataPage()) {
      System.err.println("Error in Scan class object's init method.");
    }
  }

  /**
   * Closes the scan object
   */
  public void closeScan() {
    reset();
  }

  /**
   * Reset data & Unpin all pages
   */
  private void reset() {
    try {
      if (dirPage != null) {
        unpinPage(dirPageId, false);
      }

      if (dataPage != null) {
        unpinPage(dataPageId, false);
      }
    } catch (HFBufMgrException e) {
      System.err.println("SCAN: Error in reset() " + e);
      e.printStackTrace();
    }

    dataPageId.pid = 0;
    dataPage = null;

    dirPage = null;
    nextUserStatus = true;
  }

  private boolean firstDataPage() {
    DataPageInfo dpInfo;
    Tuple recTuple = null;
    Boolean bst;

    dirPageId.pid = cf.heapFileNames[columnNo]._firstDirPageId.pid;
    nextUserStatus = true;

    try {
      dirPage = new HFPage();
      pinPage(dirPageId, dirPage, false);
    } catch (Exception e) {
      System.err.println("SCAN Error, try pinpage: " + e);
      e.printStackTrace();
    }

    try {
      /** now try to get a pointer to the first dataPage */
      dataPageRID = dirPage.firstRecord();
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (dataPageRID != null) {
      /** there is a dataPage record on the first directory page: */
      try {
        recTuple = dirPage.getRecord(dataPageRID);
      } catch (Exception e) {
        //	System.err.println("SCAN: Chain Error in Scan: " + e);
        e.printStackTrace();
      }

      try {
        dpInfo = new DataPageInfo(recTuple);
        dataPageId.pid = dpInfo.pageId.pid;
      } catch (InvalidTupleSizeException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

    } else {

      /** the first directory page is the only one which can possibly remain
       * empty: therefore try to get the next directory page and
       * check it. The next one has to contain a dataPage record, unless
       * the heapfile is empty:
       */
      PageId nextDirPageId = new PageId();

      try {
        nextDirPageId = dirPage.getNextPage();
      } catch (IOException e) {
        e.printStackTrace();
      }

      if (nextDirPageId.pid != INVALID_PAGE) {

        try {
          unpinPage(dirPageId, false);
          dirPage = null;
        } catch (Exception e) {
          //	System.err.println("SCAN: Error in 1stdataPage 1 " + e);
          e.printStackTrace();
        }

        try {

          dirPage = new HFPage();
          pinPage(nextDirPageId, (Page) dirPage, false);

        } catch (Exception e) {
          //  System.err.println("SCAN: Error in 1stdataPage 2 " + e);
          e.printStackTrace();
        }

        /** now try again to read a data record: */

        try {
          dataPageRID = dirPage.firstRecord();
        } catch (Exception e) {
          //  System.err.println("SCAN: Error in 1stdatapg 3 " + e);
          e.printStackTrace();
          dataPageId.pid = INVALID_PAGE;
        }

        if (dataPageRID != null) {

          try {

            recTuple = dirPage.getRecord(dataPageRID);
          } catch (Exception e) {
            //    System.err.println("SCAN: Error getRecord 4: " + e);
            e.printStackTrace();
          }

          if (recTuple.getLength() != DataPageInfo.size)
            return false;

          try {
            dpInfo = new DataPageInfo(recTuple);
            dataPageId.pid = dpInfo.pageId.pid;
          } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          }
        } else {
          // heapfile empty
          dataPageId.pid = INVALID_PAGE;
        }
      }//end if01
      else {// heapfile empty
        dataPageId.pid = INVALID_PAGE;
      }
    }

    dataPage = null;

    try {
      nextDataPage();
    } catch (Exception e) {
      //  System.err.println("SCAN Error: 1st_next 0: " + e);
      e.printStackTrace();
    }

    return true;

    /** ASSERTIONS:
     * - first directory page pinned
     * - this->dirPageId has Id of first directory page
     * - this->dirPage valid
     * - if heapfile empty:
     *    - this->dataPage == NULL, this->dataPageId==INVALID_PAGE
     * - if heapfile nonempty:
     *    - this->dataPage == NULL, this->dataPageId, this->dataPageRID valid
     *    - first dataPage is not yet pinned
     */
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
   *
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
   *
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
