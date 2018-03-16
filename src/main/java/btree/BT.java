package btree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.TID;

public class BT implements GlobalConst {

	public final static int keyCompare(KeyClass key1, KeyClass key2) throws KeyNotMatchException {
		if ((key1 instanceof IntegerKey) && (key2 instanceof IntegerKey)) {

			return (((IntegerKey) key1).getKey()).intValue() - (((IntegerKey) key2).getKey()).intValue();
		} else if ((key1 instanceof StringKey) && (key2 instanceof StringKey)) {
			return ((StringKey) key1).getKey().compareTo(((StringKey) key2).getKey());
		}

		else {
			throw new KeyNotMatchException(null, "key types do not match");
		}
	}

	protected final static int getKeyLength(KeyClass key) throws KeyNotMatchException, IOException {
		if (key instanceof StringKey) {

			OutputStream out = new ByteArrayOutputStream();
			DataOutputStream outstr = new DataOutputStream(out);
			outstr.writeUTF(((StringKey) key).getKey());
			return outstr.size();
		} else if (key instanceof IntegerKey)
			return 4;
		else
			throw new KeyNotMatchException(null, "key types do not match");
	}

	protected final static int getDataLength(short pageType) throws NodeNotMatchException {
		if (pageType == NodeType.LEAF)
			return 8;
		else if (pageType == NodeType.INDEX)
			return 4;
		else
			throw new NodeNotMatchException(null, "key types do not match");
	}

	protected final static int getKeyDataLength(KeyClass key, short pageType)
			throws KeyNotMatchException, NodeNotMatchException, IOException {
		return getKeyLength(key) + getDataLength(pageType);
	}

	protected final static KeyDataEntry getEntryFromBytes(byte[] from, int offset, int length, int keyType,
			short nodeType) {
		return null;

	}

	protected final static byte[] getBytesFromEntry(KeyDataEntry entry) {
		return null;

	}

	// need not to implement now
	public static void printPage(PageId pageno, int keyType) {

	}

	// need not implement now
	public static void printBTree(BTreeHeaderPage header) throws IOException, ConstructPageException, IteratorException,
			HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {

	}

	// need not implement now
	private static void _printTree(PageId currentPageId, String prefix, int i, int keyType) {

	}

	public static void printAllLeafPages(BTreeHeaderPage header)
			throws IOException, ConstructPageException, IteratorException, HashEntryNotFoundException,
			InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
		if (header.get_rootId().pid == INVALID_PAGE) {
			System.out.println("The Tree is Empty!!!");
			return;
		}

		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("---------------The B+ Tree Leaf Pages---------------");

		_printAllLeafPages(header.get_rootId(), header.get_keyType());

		System.out.println("");
		System.out.println("");
		System.out.println("------------- All Leaf Pages Have Been Printed --------");
		System.out.println("");
		System.out.println("");
	}

	private static void _printAllLeafPages(PageId currentPageId, int keyType)
			throws IOException, ConstructPageException, IteratorException, InvalidFrameNumberException,
			HashEntryNotFoundException, PageUnpinnedException, ReplacerException {

		BTSortedPage sortedPage = new BTSortedPage(currentPageId, keyType);

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage((Page) sortedPage, keyType);

			_printAllLeafPages(indexPage.getPrevPage(), keyType);

			TID rid = new TID();
			for (KeyDataEntry entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				_printAllLeafPages(((IndexData) entry.data).getData(), keyType);
			}
		}

		if (sortedPage.getType() == NodeType.LEAF) {
			printPage(currentPageId, keyType);
		}
	}

}
