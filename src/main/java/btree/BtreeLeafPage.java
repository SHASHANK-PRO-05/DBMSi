package btree;

import java.io.IOException;

import diskmgr.Page;
import global.PageId;
import global.RID;
import global.TID;

public class BtreeLeafPage extends BTSortedPage {

	public BtreeLeafPage(int keyType) throws IOException, ConstructPageException {
		super(keyType);
		setType(NodeType.LEAF);
	}

	public BtreeLeafPage(Page page, int keyType) throws IOException, ConstructPageException {
		super(page, keyType);
		setType(NodeType.LEAF);
	}

	public BtreeLeafPage(PageId pageno, int keyType) throws IOException, ConstructPageException {
		super(pageno, keyType);
		setType(NodeType.LEAF);
	}

	public TID insertRecord(KeyClass key, TID tid) throws LeafInsertRecException {
		{
			KeyDataEntry entry;

			try {
				entry = new KeyDataEntry(key, tid);

				return insertRecord(entry);
			} catch (Exception e) {
				throw new LeafInsertRecException(e, "insert record failed");
			}
		}
	}

	public KeyDataEntry getFirst(TID tid) {
		return null;
	}

	public KeyDataEntry getCurrent(TID tid) {
		return null;

	}

	public boolean delEntry(KeyDataEntry dEntry) {

		return false;

	}

	boolean redistribute(BtreeLeafPage leafPage, BTIndexPage parentIndexPage, int direction, KeyClass deletedKey)
			throws LeafRedistributeException {
		return false;
	}

}
