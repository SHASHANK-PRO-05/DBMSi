package bitMap;

import java.io.*;


import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.*;
import diskmgr.*;
import heap.*;

public class BitMapFile {
	
	
	  private BitMapHeaderPage headerPage;
	  private  PageId  headerPageId;
	  private String  filename;  
	  private Columnarfile columnarfile;
	  
	  
	  
	  /**
	   * Access method to data member.
	   * @return  Return a BitMapHeaderPage object that is the header page
	   *          of this bitmap file.
	   */
	  public BitMapHeaderPage getHeaderPage() {
	    return headerPage;
	  }
	  
	  
	  private PageId get_file_entry(String filename) throws GetFileEntryException {
		  try {
			  return SystemDefs.JavabaseDB.get_file_entry(filename);
		  }
	      catch (Exception e) {
	    	  e.printStackTrace();
	    	  throw new GetFileEntryException(e,"");
    	  }
	  }
	  
	  private Page pinPage(PageId pageno) throws PinPageException {
		  try {
			  Page page=new Page();
			  SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
			  return page;
		  }
		  catch (Exception e) {
			  e.printStackTrace();
			  throw new PinPageException(e,"");
		  }
	  }
	  
	  
	  
	  private void add_file_entry(String fileName, PageId pageno)throws AddFileEntryException {
		  try {
			  SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		  }
		  catch (Exception e) {
			  e.printStackTrace();
			  throw new AddFileEntryException(e,"");
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

	private void delete_file_entry(String filename)
			throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty)
			throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}
	
	
	
	/**
	 * BitMap class an index file with given filename should already exist;
	 * this opens it.
	 * 
	 * @param filename
	 *            the B+ tree file name. Input parameter.
	 * @exception GetFileEntryException
	 *                can not ger the file from DB
	 * @exception PinPageException
	 *                failed when pin a page
	 * @exception ConstructPageException
	 *                BT page constructor failed
	 */
	public BitMapFile(String filename) throws GetFileEntryException,
			PinPageException, ConstructPageException {

		headerPageId = get_file_entry(filename);

		headerPage = new BitMapHeaderPage(headerPageId);
		filename = new String(filename);
		/*
		 * 
		 * - headerPageId is the PageId of this BitMapFile's header page; -
		 * headerPage, headerPageId valid and pinned - dbname contains a copy of
		 * the name of the database
		 */
	}
	
	 /**
	   *  if index file exists, open it; else create it.
	   *  This creates a bitmap file from Index
	   *@param filename file name. Input parameter.
	   *@param keytype the type of key. Input parameter.
	   *@param keysize the maximum size of a key. Input parameter.
	   *@param delete_fashion full delete or naive delete. Input parameter.
	   *           It is either DeleteFashion.NAIVE_DELETE or 
	   *           DeleteFashion.FULL_DELETE.
	   *@exception GetFileEntryException  can not get file
	   *@exception ConstructPageException page constructor failed
	   *@exception IOException error from lower layer
	   *@exception AddFileEntryException can not add file into DB
	   */
	public BitMapFile(String filename, Columnarfile columnfile, int columnNo,ValueClass value) throws GetFileEntryException,
			ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new BitMapHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.setColumnIndex(columnNo);
			headerPage.setValue(value);
			columnfile = columnfile;
		} else {
			headerPage = new BitMapHeaderPage(headerPageId);
		}
		//initialize a bitmap
		init();
		filename = new String(filename);

	}
	
	
	/*
	 * Initialize our bitMap 
	 */
	private void init() {
		//initialize a sequential scan on the 
		Scan scan = columnarfile.openColumnScan(1);
		//ValueClass value = headerpage.getValue()
		/*
		 * get value of 1st tuple
		 * ValueClass value = scan.getnext();
		 * 
		 * check if the tuple match the value
		 * 
		 * if yes then insert(1);
		 * if no then position ++;		 */
		/*
		 * 
		 */
		
		/*
		 * while(scan.getNext()){
		 * scan through the tuples
		 * 
		 * 
		 * }
		 */	
		
	}


	/**
	 * Close the B+ tree file. Unpin header page.
	 * 
	 * @exception PageUnpinnedException
	 *                error from the lower layer
	 * @exception InvalidFrameNumberException
	 *                error from the lower layer
	 * @exception HashEntryNotFoundException
	 *                error from the lower layer
	 * @exception ReplacerException
	 *                error from the lower layer
	 */
	public void close() throws PageUnpinnedException,
			InvalidFrameNumberException, HashEntryNotFoundException,
			ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}
	
	/** Destroy entire BitMaptree file.
	   *@exception IOException  error from the lower layer
	   *@exception IteratorException iterator error
	   *@exception UnpinPageException error  when unpin a page
	   *@exception FreePageException error when free a page
	   *@exception DeleteFileEntryException failed when delete a file from DM
	   *@exception ConstructPageException error in BT page constructor 
	   *@exception PinPageException failed when pin a page
	   */
	
	public void destroyBitMapFile() {
		
	}
	
	public boolean Delete(int position){
		return false;
		
	}
	
	public boolean Insert (int position){
		return false;
		
	}
	

}
