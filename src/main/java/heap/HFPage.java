/* File HFPage.java */

package heap;

import diskmgr.InvalidPageNumberException;
import diskmgr.Page;
import global.*;
import heap.exceptions.NotImplementedException;

import java.io.IOException;

/**
 * Class heap file page.
 * The design assumes that records are kept compacted when
 * deletions are performed.
 */

public class HFPage extends Page implements GlobalConst {

  /**
   * Length of fixed data on page
   */
  private final int DPFIXED = (4 * 2) + (3 * 4);
  /**
   * Position where type of records stored in this page is stored
   */
  private final int TYPE = 0;
  /**
   * Position where record length is stored
   */
  private final int RECORD_LENGTH = 2;
  /**
   * Position where free space count is stored
   */
  private final int FREE_SPACE = 4;
  /**
   * Position where the pointer to the previous page is stored
   */
  private final int PREV_PAGE = 6;
  /**
   * Position where the pointer to the next page is stored
   */
  private final int NEXT_PAGE = 10;
  /**
   * Position where the current page id is stored
   */
  private final int CUR_PAGE = 14;
  /**
   * Position where record count is stored
   */
  private final int RECORD_COUNT = 18;

  /* Warning:
   These items must all pack tight, (no padding) for
   the current implementation to work properly.
   Be careful when modifying this class.
  */

  /**
   * number of slots in use
   */
  private short recordCount;
  /**
   * size of the record to be stored in the page. It is going to be fixed.
   */
  private short recordLength;
  /**
   * number of bytes free in data[]
   */
  private short freeSpace;
  /**
   * an arbitrary value used by subclasses as neede
   */
  private short type;
  /**
   * backward pointer to data page
   */
  private PageId prevPage = new PageId();
  /**
   * forward pointer to data page
   */
  private PageId nextPage = new PageId();
  /**
   * page number of this page
   */
  private PageId curPage = new PageId();

  /*** Default constructor*/
  public HFPage() {
  }

  /**
   * Constructor of class HFPage
   * open a HFPage and make this HFPage point to the given page
   *
   * @param page the given page in Page type
   */
  public HFPage(Page page) {
    data = page.getPage();
  }

  /**
   * Constructor of class HFPage
   * open a existed HFPage
   *
   * @param apage a page in buffer pool
   */
  public void openHFPage(Page apage) {
    data = apage.getPage();
  }

  /**
   * Constructor of class HFPage
   * initialize a new page
   *
   * @param pageNo the page number of a new page to be initialized
   * @param aPage  the Page to be initialized
   * @throws IOException I/O errors
   * @see Page
   */
  public void init(PageId pageNo, Page aPage, AttrType attrType) throws IOException {
    data = aPage.getPage();

    curPage.pid = pageNo.pid;
    Convert.setIntValue(curPage.pid, CUR_PAGE, data);

    nextPage.pid = prevPage.pid = INVALID_PAGE;
    Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);

    freeSpace = (short) (MINIBASE_PAGESIZE - DPFIXED); // amount of space available
    Convert.setShortValue(freeSpace, FREE_SPACE, data);

    this.type = (short) attrType.getAttrType();
    Convert.setShortValue(this.type, TYPE, data);

    recordLength = (short) attrType.getSize();
    Convert.setShortValue(recordLength, RECORD_LENGTH, data);
  }

  public void readValuesFromData() throws IOException {
    recordCount = Convert.getShortValue(RECORD_COUNT, data);
    recordLength = Convert.getShortValue(RECORD_LENGTH, data);
    type = Convert.getShortValue(TYPE, data);
    freeSpace = Convert.getShortValue(FREE_SPACE, data);
    nextPage = new PageId(Convert.getIntValue(NEXT_PAGE, data));
    prevPage = new PageId(Convert.getIntValue(PREV_PAGE, data));
    curPage = new PageId(Convert.getIntValue(CUR_PAGE, data));
  }

  /**
   * @return byte array
   */

  public byte[] getHFPageArray() {
    return data;
  }

  /**
   * Dump contents of a page
   *
   * @throws IOException I/O errors
   */
  public void dumpPage() throws IOException {
    int i, n;

    curPage.pid = Convert.getIntValue(CUR_PAGE, data);
    nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
    freeSpace = Convert.getShortValue(FREE_SPACE, data);
    recordCount = Convert.getShortValue(RECORD_COUNT, data);

    System.out.println("dumpPage");
    System.out.println("curPage= " + curPage.pid);
    System.out.println("nextPage= " + nextPage.pid);
    System.out.println("freeSpace= " + freeSpace);
    System.out.println("recordCnt= " + recordCount);

    for (i = 0, n = DPFIXED; i < recordCount; n += recordLength, i++) {
      System.out.println("RecordNo = " + i + " | offset = " + (i * recordLength));
    }
  }

  /**
   * @return PageId of previous page
   * @throws IOException I/O errors
   */
  public PageId getPrevPage() throws IOException {
    prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
    return prevPage;
  }

  /**
   * sets value of prevPage to pageNo
   *
   * @param pageNo page number for previous page
   * @throws IOException I/O errors
   */
  public void setPrevPage(PageId pageNo) throws IOException {
    prevPage.pid = pageNo.pid;
    Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
  }

  /**
   * @return page number of next page
   * @throws IOException I/O errors
   */
  public PageId getNextPage() throws IOException {
    nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
    return nextPage;
  }

  /**
   * sets value of nextPage to pageNo
   *
   * @param pageNo page number for next page
   * @throws IOException I/O errors
   */
  public void setNextPage(PageId pageNo) throws IOException {
    nextPage.pid = pageNo.pid;
    Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
  }

  /**
   * @return page number of current page
   * @throws IOException I/O errors
   */
  public PageId getCurPage() throws IOException {
    curPage.pid = Convert.getIntValue(CUR_PAGE, data);
    return curPage;
  }

  /**
   * sets value of curPage to pageNo
   *
   * @param pageNo page number for current page
   * @throws IOException I/O errors
   */
  public void setCurPage(PageId pageNo) throws IOException {
    curPage.pid = pageNo.pid;
    Convert.setIntValue(curPage.pid, CUR_PAGE, data);
  }

  /**
   * @return the ype
   * @throws IOException I/O errors
   */
  public short getType() throws IOException {
    type = Convert.getShortValue(TYPE, data);
    return type;
  }

  /**
   * sets value of type
   *
   * @param valtype an arbitrary value
   * @throws IOException I/O errors
   */
  public void setType(short valtype) throws IOException {
    type = valtype;
    Convert.setShortValue(type, TYPE, data);
  }

  /**
   * @return slotCnt used in this page
   * @throws IOException I/O errors
   */
  public short getRecordCount() throws IOException {
    recordCount = Convert.getShortValue(RECORD_COUNT, data);
    return recordCount;
  }

  /**
   * Get the size of records that are stored in this page
   *
   * @return size of record
   * @throws IOException
   */
  public short getRecordLength() throws IOException {
    recordLength = Convert.getShortValue(RECORD_LENGTH, data);
    return recordLength;
  }

  /**
   * inserts a new record onto the page, returns RID of this record
   *
   * @param record a record to be inserted
   * @return RID of record, null if sufficient space does not exist
   * @throws IOException I/O errors
   *                     in C++ Status insertRecord(char *recPtr, int recLen, RID& rid)
   */
  public RID insertRecord(byte[] record) throws IOException {
    RID rid = new RID();

    recordLength = Convert.getShortValue(RECORD_LENGTH, data);

//    if (record.length != recordLength) {
//      throw new IOException("record.length should be equal to recordLength");
//    }

    // Start by checking if sufficient space exists.
    // This is an upper bound check. May not actually need a slot
    // if we can find an empty one.

    freeSpace = Convert.getShortValue(FREE_SPACE, data);
    if (record.length > freeSpace) {
      return null;
    } else {
      // look for an empty slot
      recordCount = Convert.getShortValue(RECORD_COUNT, data);

      freeSpace -= recordLength;
      Convert.setShortValue(freeSpace, FREE_SPACE, data);

      recordCount++;
      Convert.setShortValue(recordCount, RECORD_COUNT, data);

      // insert data onto the data page
      System.arraycopy(record, 0, data, recordCount * record.length, record.length);
      curPage.pid = Convert.getIntValue(CUR_PAGE, data);
      rid.pageNo.pid = curPage.pid;
      rid.slotNo = recordCount - 1;
      return rid;
    }
  }

  /**
   * delete the record with the specified rid
   *
   * @param rid the record ID
   * @throws IOException                I/O errors
   *                                    in C++ Status deleteRecord(const RID& rid)
   * @throws InvalidSlotNumberException Invalid slot number
   */
  public void deleteRecord(RID rid) throws IOException, InvalidSlotNumberException {
    throw new NotImplementedException();
  }

  /**
   * @return RID of first record on page, null if page contains no records.
   * @throws IOException I/O errors
   *                     in C++ Status firstRecord(RID& firstRid)
   */
  public RID firstRecord() throws IOException {
    RID rid = new RID();

    recordCount = Convert.getShortValue(RECORD_COUNT, data);

    if (recordCount == 0) {
      return null;
    }

    // TODO: What to do if the first record is deleted

    rid.slotNo = 0;
    readCurrentPagePID();
    rid.pageNo.pid = curPage.pid;

    return rid;
  }

  /**
   * @param curRid current record ID
   * @return RID of next record on the page, null if no more
   * records exist on the page
   * @throws IOException I/O errors
   *                     in C++ Status nextRecord (RID curRid, RID& nextRid)
   */
  public RID nextRecord(RID curRid) throws IOException {
    RID rid = new RID();
    recordCount = Convert.getShortValue(RECORD_COUNT, data);

    if (curRid.slotNo >= recordCount) {
      return null;
    }

    // TODO: What to do if the record is deleted.

    rid.slotNo = recordCount - 1;
    readCurrentPagePID();
    rid.pageNo.pid = curPage.pid;
    return rid;
  }

  /**
   * copies out record with RID rid into record pointer.
   * <br>
   * Status getRecord(RID rid, char *recPtr, int& recLen)
   *
   * @param rid the record ID
   * @return a tuple contains the record
   * @throws InvalidSlotNumberException Invalid slot number
   * @throws IOException                I/O errors
   * @see Tuple
   */
  public Tuple getRecord(RID rid) throws IOException, InvalidSlotNumberException, InvalidPageNumberException {
    readCurrentPagePID();
    verifyRID(rid);

    // TODO: What to do if the record is deleted

    byte[] record = new byte[recordLength];
    System.arraycopy(data, rid.slotNo * recordCount, record, 0, recordLength);

    return new Tuple(record, 0, recordLength);
  }

  /**
   * returns a tuple in a byte array[pageSize] with given RID rid.
   * <br>
   * in C++	Status returnRecord(RID rid, char*& recPtr, int& recLen)
   *
   * @param rid the record ID
   * @return a tuple  with its length and offset in the byte array
   * @throws InvalidSlotNumberException Invalid slot number
   * @throws IOException                I/O errors
   * @see Tuple
   */
  public Tuple returnRecord(RID rid) throws IOException, InvalidSlotNumberException, InvalidPageNumberException {

    readCurrentPagePID();
    verifyRID(rid);

    return new Tuple(data, rid.slotNo * recordLength, recordLength);
  }

  public byte[] getDataAtSlot(RID rid) throws IOException, InvalidSlotNumberException, InvalidPageNumberException {
    readCurrentPagePID();
    verifyRID(rid);

    byte[] ans = new byte[recordLength];
    System.arraycopy(data, rid.slotNo * recordLength, ans, 0, recordLength);

    return ans;
  }

  /**
   * returns the amount of available space on the page.
   *
   * @return the amount of available space on the page
   * @throws IOException I/O errors
   */
  public int available_space() throws IOException {
    return Convert.getShortValue(FREE_SPACE, data);
  }

  /**
   * Determining if the page is empty
   *
   * @return true if the HFPage is has no records in it, false otherwise
   * @throws IOException I/O errors
   */
  public boolean empty() throws IOException {
    recordCount = Convert.getShortValue(RECORD_COUNT, data);
    return recordCount == 0;
  }

  /**
   * Compacts the slot directory on an HFPage.
   * WARNING -- this will probably lead to a change in the RIDs of
   * records on the page.  You CAN'T DO THIS on most kinds of pages.
   *
   * @throws IOException I/O errors
   */
  private void compact_slot_dir() throws IOException {
    throw new NotImplementedException();
  }

  private void readCurrentPagePID() throws IOException {
    if (curPage == null) {
      curPage = new PageId();
    }

    if (curPage.pid == 0) {
      curPage.pid = Convert.getIntValue(CUR_PAGE, data);
    }
  }

  private void verifyRID(RID rid) throws InvalidPageNumberException, InvalidSlotNumberException, IOException {
    if (recordLength == 0) {
      recordLength = Convert.getShortValue(RECORD_LENGTH, data);
    }

    if (rid.pageNo.pid != curPage.pid) {
      throw new InvalidPageNumberException(null, "HFPage: Invalid PID");
    }

    if (rid.slotNo >= (MINIBASE_PAGESIZE - DPFIXED) / recordLength) {
      throw new InvalidSlotNumberException(null, "HFPage: Invalid Slot No");
    }
  }

  /**
   * Make sure that the current page is the start page for this
   *
   * @param position Tuple Position
   * @throws HFBufMgrException
   * @throws IOException
   */
  public void setCurPage_forGivenPosition(int position) throws HFBufMgrException, IOException {
    if (position < recordCount) {
      return;
    }

    long totalRecordsRead = recordCount;

    PageId currentPageID = new PageId(curPage.pid);
    PageId nextPageID = new PageId(nextPage.pid);
    HFPage nextHFPage = null;

    while (position >= totalRecordsRead) {
      // Assuming the position a valid position
      nextHFPage = new HFPage();

      pinPage(nextPageID, nextHFPage);
      totalRecordsRead += nextHFPage.getRecordCount();
      nextPageID = new PageId(nextHFPage.getNextPage().pid);
      currentPageID = new PageId(nextHFPage.getCurPage().pid);
      unpinPage(nextHFPage.getCurPage(), false);
    }

    if (nextHFPage != null) {
      pinPage(currentPageID, nextHFPage);
      data = nextHFPage.getHFPageArray();
      readValuesFromData();
    }
  }

  private void pinPage(PageId pageno, Page page)
      throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.pinPage(pageno, page, false);
    } catch (Exception e) {
      throw new HFBufMgrException(e, "Heapfile.java: pinPage() failed");
    }

  } // end of pinPage

  /**
   * short cut to access the unpinPage function in bufmgr package.
   */
  private void unpinPage(PageId pageno, boolean dirty)
      throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
    } catch (Exception e) {
      throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
    }

  }
}
