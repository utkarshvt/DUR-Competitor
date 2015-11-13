package stm.impl.objectstructure;

import stm.transaction.AbstractObject;

public class CompletedObject { 

	AbstractObject currentObject; // version is part of object
	
	public void setCurrentObject(AbstractObject object) {
		currentObject = object;
	}
	
	public AbstractObject getCurrentObject() {
		return currentObject;
	}
}
