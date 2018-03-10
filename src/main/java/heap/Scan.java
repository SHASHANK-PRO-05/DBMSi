package heap;

import columnar.ColumnarFile;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

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

  public Scan(ColumnarFile cf, short columnNo) throws IOException, InvalidTupleSizeException {
    init(cf, columnNo);
  }

public RID getFirstRID() throws IOException {
    return dirPage.firstRecord();
  }

  public Tuple getNext(RID rid) throws InvalidTupleSizeException, IOException {
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

  public boolean position(RID rid) throws InvalidTupleSizeException, IOException {
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

  private void init(ColumnarFile cf, short columnNo) throws IOException, InvalidTupleSizeException {
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

  private boolean firstDataPage() throws IOException, InvalidTupleSizeException {
    DataPageInfo dpInfo;
    Tuple recTuple = null;
    Boolean bst;

    dirPageId.pid = cf.getHeapFileNames()[columnNo]._firstDirPageId.pid;
    nextUserStatus = true;

    try {
      dirPage = new HFPage();
      pinPage(dirPageId, dirPage, false);
    } catch (Exception e) {
      System.err.println("SCAN Error, try pinpage: " + e);
      e.printStackTrace();
    }

    dataPageRID = dirPage.firstRecord();

    if (dataPageRID != null) {
      /** there is a dataPage record on the first directory page: */
      try {
        recTuple = dirPage.getRecord(dataPageRID);
      } catch (Exception e) {
        //	System.err.println("SCAN: Chain Error in Scan: " + e);
        e.printStackTrace();
      }

      dpInfo = new DataPageInfo(recTuple);
      dataPageId.pid = dpInfo.pageId.pid;

    } else {

      /** the first directory page is the only one which can possibly remain
       * empty: therefore try to get the next directory page and
       * check it. The next one has to contain a dataPage record, unless
       * the heapFile is empty:
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
          // heapFile empty
          dataPageId.pid = INVALID_PAGE;
        }
      }//end if01
      else {// heapFile empty
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
     * - if heapFile empty:
     *    - this->dataPage == NULL, this->dataPageId==INVALID_PAGE
     * - if heapFile nonempty:
     *    - this->dataPage == NULL, this->dataPageId, this->dataPageRID valid
     *    - first dataPage is not yet pinned
     */
  }

  private boolean nextDataPage()
      throws InvalidTupleSizeException,
      IOException {

    DataPageInfo dpinfo;

    boolean nextDataPageStatus;
    PageId nextDirPageId = new PageId();
    Tuple rectuple = null;

    // ASSERTIONS:
    // - this->dirPageId has Id of current directory page
    // - this->dirPage is valid and pinned
    // (1) if heapFile empty:
    //    - this->dataPage==NULL; this->dataPageId == INVALID_PAGE
    // (2) if overall first record in heapFile:
    //    - this->dataPage==NULL, but this->dataPageId valid
    //    - this->dataPageRID valid
    //    - current data page unpinned !!!
    // (3) if somewhere in heapFile
    //    - this->dataPageId, this->dataPage, this->dataPageRID valid
    //    - current data page pinned
    // (4)- if the scan had already been done,
    //        dirPage = NULL;  dataPageId = INVALID_PAGE

    if ((dirPage == null) && (dataPageId.pid == INVALID_PAGE))
      return false;

    if (dataPage == null) {
      if (dataPageId.pid == INVALID_PAGE) {
        // heapFile is empty to begin with

        try {
          unpinPage(dirPageId, false);
          dirPage = null;
        } catch (Exception e) {
          //  System.err.println("Scan: Chain Error: " + e);
          e.printStackTrace();
        }

      } else {

        // pin first data page
        try {
          dataPage = new HFPage();
          pinPage(dataPageId, (Page) dataPage, false);
        } catch (Exception e) {
          e.printStackTrace();
        }

        try {
          userRID = dataPage.firstRecord();
        } catch (Exception e) {
          e.printStackTrace();
        }

        return true;
      }
    }

    // ASSERTIONS:
    // - this->dataPage, this->dataPageId, this->dataPageRID valid
    // - current dataPage pinned

    // unpin the current dataPage
    try {
      unpinPage(dataPageId, false /* no dirty */);
      dataPage = null;
    } catch (Exception e) {
      e.printStackTrace();
    }

    // read next dataPagerecord from current directory page
    // dirPage is set to NULL at the end of scan. Hence

    if (dirPage == null) {
      return false;
    }

    dataPageRID = dirPage.nextRecord(dataPageRID);

    if (dataPageRID == null) {
      nextDataPageStatus = false;
      // we have read all dataPage records on the current directory page

      // get next directory page
      nextDirPageId = dirPage.getNextPage();

      // unpin the current directory page
      try {
        unpinPage(dirPageId, false /* not dirty */);
        dirPage = null;

        dataPageId.pid = INVALID_PAGE;
      } catch (Exception e) {
        e.printStackTrace();
      }

      if (nextDirPageId.pid == INVALID_PAGE)
        return false;
      else {
        // ASSERTION:
        // - nextDirPageId has correct id of the page which is to get

        dirPageId = nextDirPageId;

        try {
          dirPage = new HFPage();
          pinPage(dirPageId, (Page) dirPage, false);
        } catch (Exception e) {
          e.printStackTrace();
        }

        if (dirPage == null)
          return false;

        try {
          dataPageRID = dirPage.firstRecord();
          nextDataPageStatus = true;
        } catch (Exception e) {
          nextDataPageStatus = false;
          return false;
        }
      }
    }

    // ASSERTION:
    // - this->dirPageId, this->dirPage valid
    // - this->dirPage pinned
    // - the new dataPage to be read is on dirPage
    // - this->dataPageRID has the Rid of the next dataPage to be read
    // - this->dataPage, this->dataPageId invalid

    // data page is not yet loaded: read its record from the directory page
    try {
      rectuple = dirPage.getRecord(dataPageRID);
    } catch (Exception e) {
      System.err.println("HeapFile: Error in Scan" + e);
    }

    if (rectuple.getLength() != DataPageInfo.size)
      return false;

    dpinfo = new DataPageInfo(rectuple);
    dataPageId.pid = dpinfo.pageId.pid;

    try {
      dataPage = new HFPage();
      pinPage(dpinfo.pageId, dataPage, false);
    } catch (Exception e) {
      System.err.println("HeapFile: Error in Scan" + e);
    }


    // - directory page is pinned
    // - dataPage is pinned
    // - this->dirPageId, this->dirPage correct
    // - this->dataPageId, this->dataPage, this->dataPageRID correct

    userRID = dataPage.firstRecord();

    if (userRID == null) {
      nextUserStatus = false;
      return false;
    }

    return true;
  }

  private boolean peekNext(RID rid) {
    rid.pageNo.pid = userRID.pageNo.pid;
    rid.slotNo = userRID.slotNo;
    return true;
  }

  private boolean mvNext(RID rid) throws IOException, InvalidTupleSizeException {
    RID nextRID;
    boolean status;

    if (dataPage == null)
      return false;

    nextRID = dataPage.nextRecord(rid);

    if (nextRID != null) {
      userRID.pageNo.pid = nextRID.pageNo.pid;
      userRID.slotNo = nextRID.slotNo;
      return true;
    } else {
      status = nextDataPage();

      if (status) {
        rid.pageNo.pid = userRID.pageNo.pid;
        rid.slotNo = userRID.slotNo;
      }

    }

    return true;
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
