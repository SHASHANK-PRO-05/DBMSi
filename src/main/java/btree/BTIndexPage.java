package btree;

import java.io.IOException;

import diskmgr.Page;
import global.PageId;
import global.RID;
import global.TID;

public class BTIndexPage extends BTSortedPage {

	public BTIndexPage(Page page, int keyType) throws IOException, ConstructPageException {
		super(page, keyType);
		setType(NodeType.INDEX);
	}

	public BTIndexPage(PageId pageno, int keyType) throws IOException, ConstructPageException {
		super(pageno, keyType);
		setType(NodeType.INDEX);
	}

	public BTIndexPage(int keyType) throws IOException, ConstructPageException {
		super(keyType);
		setType(NodeType.INDEX);
	}
	
	public RID insertKey(KeyClass key, PageId pageNo) {
		return null;
		
	}
	
	TID deleteKey(KeyClass key) {
		return null;	
	}
	
	
	PageId getPageNoByKey(KeyClass key) {
		
		
		
		return null;
	}
	
	public KeyDataEntry getFirst(RID rid) {
		return null;
		
		
		
	}
	
	public KeyDataEntry getNext (RID rid) {
		return null;
		
	}
	
	
	protected PageId getLeftLink() {
		return null;
		
		
	}
	
	protected void setLeftLink(PageId left) {
		
		
		
	}
	
	int  getSibling(KeyClass key, PageId pageNo) {
		return 0;
		
	}
	
	boolean adjustKey(KeyClass newKey, KeyClass oldKey) {
		return false;
	}
	
	KeyDataEntry findKeyData(KeyClass key) {
		return null;	
	}
	
	KeyClass findKey(KeyClass key) {
		return null;
		
	}
	
	boolean redistribute(BTIndexPage indexPage, BTIndexPage parentIndexPage,
		       int direction, KeyClass deletedKey) {
				return false;
		
		
	}
}
