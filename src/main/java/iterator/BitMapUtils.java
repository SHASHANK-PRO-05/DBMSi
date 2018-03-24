package iterator;

import bitmap.*;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;

import java.io.IOException;
import java.util.ArrayList;

public class BitMapUtils implements GlobalConst {
    BMPage bmPage[] = null;
    PageId[] pageIds = null;
    //Inside byte array where
    int bytePostion;
    //Insided byte which pointer
    int bytePointer;
    int numBMPages = 0;

    public BitMapUtils(ArrayList<BitMapFile> arrayList)
            throws PinPageException, IOException,
            UnpinPageException {
        bmPage = new BMPage[arrayList.size()];
        pageIds = new PageId[arrayList.size()];
        bytePostion = 0;
        for (int i = 0; i < arrayList.size(); i++) {
            bmPage[i] = new BMPage();
            BitMapHeaderPage bitMapHeaderPage = arrayList.get(i).getBitMapHeaderPage();
            PageId headerPageId = arrayList.get(i).getHeaderPageId();
            pinPage(headerPageId, bitMapHeaderPage);
            pageIds[i] = bitMapHeaderPage.getNextPage();
            unpinPage(headerPageId, false);
            pinPage(pageIds[i], bmPage[i]);
        }
    }

    public void closeUtils() throws UnpinPageException {
        for (int i = 0; i < bmPage.length; i++) {
            if (pageIds[i].pid != INVALID_PAGE)
                unpinPage(pageIds[i], false);
        }
    }

    public int getNextOrPosition()
            throws IOException, UnpinPageException
            , PinPageException {
        if (bmPage.length == 0) return -1;
        while (pageIds[0].pid != INVALID_PAGE) {
            int ansToSend = -1;
            while (bytePostion < BMPage.availableMap) {
                int orAnswer = 0;
                for (int i = 0; i < bmPage.length; i++) {
                    //System.out.println(bmPage[i].getPage()[BMPage.DPFIXED + bytePostion]);

                    int valToTraverse = ((bmPage[i].getPage()[BMPage.DPFIXED + bytePostion] + 256) % 256) >> bytePointer;
                    orAnswer = (orAnswer | (valToTraverse & 1));
                    if (orAnswer != 0) break;
                }
                if (orAnswer != 0) {
                    ansToSend = 8 * bytePostion + bytePointer + (numBMPages * (BMPage.availableMap * 8));
                }
                bytePointer++;
                if (bytePointer == 8) {
                    bytePointer = 0;
                    bytePostion++;
                }
                if (ansToSend != -1) {
                    break;
                }
            }

            if (bytePostion == BMPage.availableMap) {
                for (int i = 0; i < bmPage.length; i++) {
                    PageId tempPageId = bmPage[i].getNextPage();
                    unpinPage(pageIds[i], false);
                    pageIds[i].pid = tempPageId.pid;
                    if (pageIds[i].pid != INVALID_PAGE)
                        pinPage(pageIds[i], bmPage[i]);
                }
                numBMPages++;
                bytePostion = 0;
                bytePointer = 0;
            }
            if (ansToSend != -1)
                return ansToSend;

        }
        return -1;
    }

    public ArrayList<Integer> getAllOrPositions() throws UnpinPageException, IOException, PinPageException {
        ArrayList<Integer> positions = new ArrayList<Integer>();
        int position = getNextOrPosition();

        while (position != -1) {
            positions.add(position);
            position = getNextOrPosition();
        }

        return positions;
    }

    public void unpinPage(PageId pageId, boolean dirty) throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageId, dirty);
        } catch (Exception e) {
            throw new UnpinPageException(e, "Not able to unpin the page");
        }
    }

    private Page pinPage(PageId pageId, Page page) throws PinPageException {
        try {
            SystemDefs.JavabaseBM.pinPage(pageId, page, false/* Rdisk */);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "Not able to pin the page");
        }
    }
}
