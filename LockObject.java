package stm.impl;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import stm.impl.objectstructure.SharedObject;
import stm.transaction.AbstractObject;

public class LockObject
{
	private final ReentrantLock lock;
	
	public LockObject()
	{
		lock  = new ReentrantLock();
	}
	
	public boolean lock(String Id)
	{
		//if(lock.isLocked())
		//	System.out.println("Lock is locked already fr Object " + Id);

		while(lock.isLocked())
		{
			
			//System.out.println("Trying to lock " + Id);
			try 
			{
                       		Thread.sleep(100);
                   	} 
			catch (InterruptedException ex) 
			{
                        	ex.printStackTrace();
			}
		}
		/*At present it is Ok not to check for the lock being locked,since there is only one lock */
		return lock.tryLock();
	}

	public void unlock(String Id)
	{
		lock.unlock();
	}
}
		
