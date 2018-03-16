package btree;

import java.io.IOException;

import diskmgr.Page;
import global.PageId;
import global.SystemDefs;
import global.TID;
import heap.HFPage;

public class BTSortedPage extends HFPage {
	int keyType;

	public BTSortedPage(PageId pageno, int keyType) throws ConstructPageException {
		super();
		try {
			// super();
			SystemDefs.JavabaseBM.pinPage(pageno, this, false/* Rdisk */);
			this.keyType = keyType;
		} catch (Exception e) {
			throw new ConstructPageException(e, "construct sorted page failed");
		}
	}

	public BTSortedPage(Page page, int keyType) {

		super(page);
		this.keyType = keyType;
	}

	public BTSortedPage(int keyType) throws ConstructPageException {
		super();
		try {
			Page apage = new Page();
			PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
			if (pageId == null)
				throw new ConstructPageException(null, "construct new page failed");
			this.init(pageId, apage);
			this.keyType = keyType;
		} catch (Exception e) {
			e.printStackTrace();
			throw new ConstructPageException(e, "construct sorted page failed");
		}
	}

	protected TID insertRecord(KeyDataEntry entry) {
		return null;

	}

	public boolean deleteSortedRecord(TID rid) {
		return false;
	}

	protected int numberOfRecords() throws IOException {
		return getSlotCnt();
	}
	
	

}
