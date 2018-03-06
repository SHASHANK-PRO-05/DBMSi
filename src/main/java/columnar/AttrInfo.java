package columnar;

import global.PageId;

public class AttrInfo {

	private int columnId;
	private PageId pageId;
	private int attrType;
	private int attrSize;
	public int getColumnId() {
		return columnId;
	}
	public void setColumnId(int columnId) {
		this.columnId = columnId;
	}
	public PageId getPageId() {
		return pageId;
	}
	public void setPageId(PageId pageId) {
		this.pageId = pageId;
	}
	public int getAttrType() {
		return attrType;
	}
	public void setAttrType(int attrType) {
		this.attrType = attrType;
	}
	public int getAttrSize() {
		return attrSize;
	}
	public void setAttrSize(int attrSize) {
		this.attrSize = attrSize;
	}
	
	
	
}
