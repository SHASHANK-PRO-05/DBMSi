/* File HFPage.java */

package heap;

import java.io.*;
import java.lang.*;

import global.*;
import diskmgr.*;

/**
 * Define constant values for INVALID_SLOT and EMPTY_SLOT
 */

interface ConstSlot {
	int INVALID_SLOT = -1;
	int EMPTY_SLOT = -1;
}

/**
 * Class heap file page. The design assumes that records are kept compacted when
 * deletions are performed.
 */

public class HFPage extends Page {

}