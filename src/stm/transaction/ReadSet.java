package stm.transaction;

import java.util.HashMap;
import java.util.Map;


public class ReadSet {
    public Map<String, AbstractObject> readset;
    
    public ReadSet() {
    	readset = new HashMap<String, AbstractObject>();
    }
    
    public void addToReadSet(String objId, AbstractObject object) {
    	readset.put(objId, object);
    }
    
    public Map<String, AbstractObject> getReadSet() {
    	return readset;
    }

}
