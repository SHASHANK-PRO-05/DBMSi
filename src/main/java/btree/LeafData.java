package btree;
import global.*;

/**  IndexData: It extends the DataClass.
 *   It defines the data "rid" for leaf node in B++ tree.
 */
public class LeafData extends DataClass {
  private TID myTid;

  public String toString() {
     String s;
     s="[ "+ (new Integer(myTid.getNumRIDs())).toString() +" "
              + (new Integer(myTid.getPosition())).toString() + " ]";
     //to-do it for the rids
     return s;
  }

  /** Class constructor
   *  @param    rid  the data rid
   */
  LeafData(TID tid) {myTid= new TID(tid.getNumRIDs(),tid.getPosition(),tid.getRecordIDs());};  

  /** get a copy of the rid
  *  @return the reference of the copy 
  */
  public TID getData() {return new TID(myTid.getNumRIDs(), myTid.getPosition(),myTid.getRecordIDs() );};

  /** set the rid
   */ 
  public void setData(TID tid) { myTid= new TID(tid.getNumRIDs(),tid.getPosition(),tid.getRecordIDs());};
}   
