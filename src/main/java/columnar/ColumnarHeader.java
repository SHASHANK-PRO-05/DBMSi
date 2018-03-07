package columnar;

import java.io.IOException;

import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.HFPage;
import heap.Heapfile;


public class ColumnarHeader extends HFPage {

	public ColumnarHeader(String name, int numColumns, AttrType [] type) 
			throws HFDiskMgrException, 
			HFBufMgrException, 
			HFException, 
			IOException {
		// checking name entry in DB file
		//On failing as file is not present yet so created HFPage and inserted the name and inserted back
		// to DB file
		
		String hdrFile = name + ".hdr";
		PageId hdrPageNo = getFileEntry(hdrFile);
		if(hdrPageNo== null) {
			Page aPage = new Page();
			hdrPageNo = newPage(aPage,1);

			if(hdrPageNo == null)
				throw new HFException(null, "can't allocate new page");

			addFileEntry(hdrFile, hdrPageNo);
			HFPage hdrFileEntry = new HFPage();
			hdrFileEntry.init(hdrPageNo, aPage);
			PageId pageId = new PageId(INVALID_PAGE);

			hdrFileEntry.setNextPage(pageId);
			hdrFileEntry.setPrevPage(pageId);
			hdrFileEntry.setCurPage(hdrPageNo);
			hdrFileEntry.setType((short)numColumns);
			
			//unpinPage(hdrPageNo, true /*dirty*/ );
			for (int i = 0; i < numColumns; i++) {
				String fileNum = Integer.toString(i);
				String fileName = name + "." + fileNum;
				
				AttrInfo attrInfo = new AttrInfo();
				attrInfo.setColumnId(i);
				attrInfo.setfileName(fileName);
				attrInfo.setAttrType(Integer.parseInt(type[i].toString()));
				attrInfo.setAttrSize(1);
					
				hdrFileEntry.insertRecord(attrInfo.toString().getBytes());
				
				// TO DO - set constraint on number of fields.. should not exceed 1024 bytes 510 fields max
			}
		}
	}
	
	private PageId getFileEntry(String filename) throws HFDiskMgrException {

		PageId tmpId = new PageId();

		try {
			tmpId = SystemDefs.JavabaseDB.getFileEntry(filename);
		} catch (Exception e) {
			throw new HFDiskMgrException(e, "Heapfile.java: get_file_entry() failed");
		}

		return tmpId;

	} // end of get_file_entry

	private void addFileEntry(String filename, PageId pageno) throws HFDiskMgrException {

		try {
			SystemDefs.JavabaseDB.addFileEntry(filename, pageno);
		} catch (Exception e) {
			throw new HFDiskMgrException(e, "Heapfile.java: add_file_entry() failed");
		}

	} // end of add_file_entry
	
	private PageId newPage(Page page, int num)
		    throws HFBufMgrException {

		    PageId tmpId = new PageId();

		    try {
		      tmpId = SystemDefs.JavabaseBM.newPage(page,num);
		    }
		    catch (Exception e) {
		      throw new HFBufMgrException(e,"Heapfile.java: newPage() failed");
		    }

		    return tmpId;

		  }
	
	private void unpinPage(PageId pageno, boolean dirty) throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
		}

	} // end of unpinPage


}
