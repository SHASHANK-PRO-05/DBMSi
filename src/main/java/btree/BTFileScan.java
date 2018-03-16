package btree;

import java.io.IOException;

import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import global.TID;

public class BTFileScan extends IndexFileScan implements GlobalConst {

	BTreeFile bfile;
	String treeFilename; // B+ tree we're scanning
	BtreeLeafPage leafPage; // leaf page containing current record
	RID curRid; // position in current leaf; note: this is
				// the RID of the key/RID pair within the
				// leaf page.
	boolean didfirst; // false only before getNext is called
	boolean deletedcurrent; // true after deleteCurrent is called (read
							// by get_next, written by deleteCurrent).

	KeyClass endkey; // if NULL, then go all the way right
						// else, stop when current record > this value.
						// (that is, implement an inclusive range
						// scan -- the only way to do a search for
						// a single value).
	int keyType;
	int maxKeysize;

	@Override
	public KeyDataEntry get_next() throws ScanIteratorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete_current() throws ScanDeleteException {
		// TODO Auto-generated method stub

	}

	@Override
	public int keysize() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void DestroyBTreeFileScan() throws IOException, bufmgr.InvalidFrameNumberException, bufmgr.ReplacerException,
			bufmgr.PageUnpinnedException, bufmgr.HashEntryNotFoundException {
		if (leafPage != null) {
			SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
		}
		leafPage = null;
	}
}
