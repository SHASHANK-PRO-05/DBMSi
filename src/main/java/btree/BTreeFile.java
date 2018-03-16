package btree;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import bitmap.AddFileEntryException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import global.TID;

public class BTreeFile extends IndexFile implements GlobalConst {

	private final static int MAGIC0 = 1989;

	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	private BTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	public static void traceFilename(String filename) throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}

	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.getFileEntry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno) throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.addFileEntry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}

	private void delete_file_entry(String filename) throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.deleteFileEntry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	public BTreeFile(String filename, int keytype, int keysize, int delete_fashion)
			throws GetFileEntryException, ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
			headerPage.setType(NodeType.BTHEAD);
		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}

		dbname = new String(filename);

	}

	public void close()
			throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	public void destroyFile() throws IOException, IteratorException, UnpinPageException, FreePageException,
			DeleteFileEntryException, ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException, IteratorException, PinPageException,
			ConstructPageException, UnpinPageException, FreePageException {

		BTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new BTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page, headerPage.get_keyType());
			TID tid = new TID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(tid); entry != null; entry = indexPage.getNext(tid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // BTLeafPage

			unpinPage(pageno);
			freePage(pageno);
		}

	}

	private void updateHeader(PageId newRoot) throws IOException, PinPageException, UnpinPageException {

		BTreeHeaderPage header;
		PageId old_data;

		header = new BTreeHeaderPage(pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId(newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */ );

		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	BtreeLeafPage findRunStart(KeyClass lo_key, RID startrid) throws IOException, IteratorException,
			KeyNotMatchException, ConstructPageException, PinPageException, UnpinPageException {
		return null;

	}

	private KeyDataEntry _insert(KeyClass key, TID rid, PageId currentPageId)
			throws PinPageException, IOException, ConstructPageException, LeafDeleteException, ConstructPageException,
			DeleteRecException, IndexSearchException, UnpinPageException, LeafInsertRecException, ConvertException,
			IteratorException, IndexInsertRecException, KeyNotMatchException, NodeNotMatchException, InsertException

	{
		return null;

	}

	private boolean NaiveDelete(KeyClass key, TID tid)
			throws LeafDeleteException, KeyNotMatchException, PinPageException, ConstructPageException, IOException,
			UnpinPageException, PinPageException, IndexSearchException, IteratorException {
		BtreeLeafPage leafPage;
		RID curRid = new RID(); // iterator
		KeyClass curkey;
		RID dummyRid;
		PageId nextpage;
		boolean deleted;
		KeyDataEntry entry;

		if (trace != null) {
			trace.writeBytes("DELETE " + tid.getNumRIDs() + " " + tid.getPosition() + " " + key + lineSep);
			trace.writeBytes("DO" + lineSep);
			trace.writeBytes("SEARCH" + lineSep);
			trace.flush();
		}

		leafPage = findRunStart(key, curRid); // find first page,rid of key
		if (leafPage == null)
			return false;

		entry = leafPage.getCurrent(curRid);

		while (true) {

			while (entry == null) { // have to go right
				nextpage = leafPage.getNextPage();
				unpinPage(leafPage.getCurPage());
				if (nextpage.pid == INVALID_PAGE) {
					return false;
				}

				leafPage = new BtreeLeafPage(pinPage(nextpage), headerPage.get_keyType());
				entry = leafPage.getFirst(new RID());
			}

			if (BT.keyCompare(key, entry.key) > 0)
				break;

			if (leafPage.delEntry(new KeyDataEntry(key, tid)) == true) {

				// successfully found <key, rid> on this page and deleted it.
				// unpin dirty page and return OK.
				unpinPage(leafPage.getCurPage(), true /* = DIRTY */);

				if (trace != null) {
					trace.writeBytes("TAKEFROM node " + leafPage.getCurPage() + lineSep);
					trace.writeBytes("DONE" + lineSep);
					trace.flush();
				}

				return true;
			}
			nextpage = leafPage.getNextPage();
			unpinPage(leafPage.getCurPage());

			leafPage = new BtreeLeafPage(pinPage(nextpage), headerPage.get_keyType());

			entry = leafPage.getFirst(curRid);
		}

		/*
		 * We reached a page with first key > `key', so return an error. We should have
		 * got true back from delUserRid above. Apparently the specified <key,rid> data
		 * entry does not exist.
		 */

		unpinPage(leafPage.getCurPage());
		return false;
	}

	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key) throws IOException, KeyNotMatchException,
			IteratorException, ConstructPageException, PinPageException, UnpinPageException

	{
		BTFileScan scan = new BTFileScan();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			scan.leafPage = null;
			return scan;
		}

		scan.treeFilename = dbname;
		scan.endkey = hi_key;
		scan.didfirst = false;
		scan.deletedcurrent = false;
		scan.curRid = new RID();
		scan.keyType = headerPage.get_keyType();
		scan.maxKeysize = headerPage.get_maxKeySize();
		scan.bfile = this;

		// this sets up scan at the starting position, ready for iteration
		scan.leafPage = findRunStart(lo_key, scan.curRid);
		return scan;
	}

	private boolean FullDelete(KeyClass key, RID rid)
			throws IndexInsertRecException, RedistributeException, IndexSearchException, RecordNotFoundException,
			DeleteRecException, InsertRecException, LeafRedistributeException, IndexFullDeleteException,
			FreePageException, LeafDeleteException, KeyNotMatchException, ConstructPageException, IOException,
			IteratorException, PinPageException, UnpinPageException, IteratorException {

		if (trace != null) {
			trace.writeBytes("DELETE " + rid.pageNo + " " + rid.slotNo + " " + key + lineSep);
			trace.writeBytes("DO" + lineSep);
			trace.writeBytes("SEARCH" + lineSep);
			trace.flush();
		}

		_Delete(key, rid, headerPage.get_rootId(), null);

		if (trace != null) {
			trace.writeBytes("DONE" + lineSep);
			trace.flush();
		}

		return true;

	}

	void trace_children(PageId id) {

	}

	private KeyClass _Delete(KeyClass key, RID rid, PageId currentPageId, PageId parentPageId) {
		return null;
	}

	@Override
	public void insert(KeyClass data, TID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
			NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
			LeafDeleteException, InsertException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean Delete(KeyClass data, TID tid)
			throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
			KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException, IOException {
		// TODO Auto-generated method stub
		return false;
	}
	
	

}
