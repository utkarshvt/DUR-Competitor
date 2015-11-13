package stm.impl;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import stm.impl.objectstructure.SharedObject;
import stm.transaction.AbstractObject;


public class SharedObjectRegistry {
	// This class is used for global access to shared objects. All objects are 
	// registered to this registry after they are created. This also serves as
	// the stable copy of objects.
	
	private ConcurrentMap<String, SharedObject> registry;
	private volatile int snapshot;
	
	public SharedObjectRegistry(int capacity) {
		registry = new ConcurrentHashMap<String, SharedObject>(capacity);
		snapshot = 0;
	}
	
	public void registerObjects(String Id, AbstractObject object) {
		registry.put(Id, new SharedObject(object));
	}
	
	public AbstractObject getObject(String Id, String mode, int transactionSnapshot, boolean retry) {

		AbstractObject object = null;
		if(mode == "rw") {
			// Object requested for write operation
			// It can either be in non-committed state from some previous non-committed transaction
			// or it can be in committed state 
		
			SharedObject shObj = registry.get(Id);
			shObj.ObjectLock(Id);
			//System.out.println("Locking Object " + Id);
			object = getLatestCommittedObject(Id);
			//System.out.println("Locked Object " + Id);
			return object;	
		} else {
			// Object requested for read operation
			return registry.get(Id).getLatestCommittedObject(transactionSnapshot);
		}
	}
	
	public AbstractObject getLatestCommittedObject(String Id) {
		return registry.get(Id).getLatestCommittedObject();
	}
	
	public int getSnapshot() {
		return snapshot;
	}
	
	public int getNextSnapshot() {
		snapshot++;
		return snapshot;
	}	
	
	public int getCapacity() {
		return registry.size();
	}
	
	public void updateCompletedObject(String Id, AbstractObject object) {	
		registry.get(Id).updateCompletedObject(object);
	}
	
	public void updateObject(String Id, AbstractObject object, int timeStamp) {	
		registry.get(Id).updateCommittedObject(object, timeStamp);
	}

	public SharedObject getSharedObject(String Id)
	{
		return registry.get(Id);
	}

}
