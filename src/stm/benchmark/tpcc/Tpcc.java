package stm.benchmark.tpcc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import lsr.common.ClientRequest;
import lsr.common.ProcessDescriptor;
import lsr.common.Request;
import lsr.common.RequestId;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.replica.Replica;
import lsr.paxos.replica.SnapshotListener;
import lsr.service.STMService;
import stm.impl.PaxosSTM;
import stm.impl.SharedObjectRegistry;
import stm.transaction.AbstractObject;
import stm.transaction.TransactionContext;
import java.util.concurrent.atomic.AtomicInteger;



public class Tpcc extends STMService {

	protected static final int RETRY_COUNT = 2; // 100
	public static final byte TX_ORDER_STATUS = 0;
	public static final byte TX_DELIVERY = 1;
	public static final byte TX_STOCKLEVEL = 2;
	public static final byte TX_NEWORDER = 3;
	public static final byte TX_PAYMENT = 4;

	public final int DEFAULT_LENGTH = 6;

	private Random random = new Random();

	SharedObjectRegistry sharedObjectRegistry;
	PaxosSTM stmInstance;
	Replica replica;
	TpccMultiClient client;

	private SingleThreadDispatcher tpccSTMDispatcher;

	private String id;
	// Constants
	public int NUM_ITEMS = 50; // Correct overall # of items: 100,000
	public int NUM_WAREHOUSES = 5;
	public final int NUM_DISTRICTS = 10; // 4;
	public final int NUM_CUSTOMERS_PER_D = 30; // 30;
	public final int NUM_ORDERS_PER_D = 30; // 30;
	public final int MAX_CUSTOMER_NAMES = 1000; // 10;

	/** data collector variables **/
	static long startRead;
	static long startWrite;
	static long endRead;
	static long endWrite;
	private long lastReadCount = 0;
	private long lastWriteCount = 0;
	private long lastAbortCount = 0;
	private long lastCompletedCount = 0;
	private long lastSubmitCount = 0;
	private long lastearlyAbort = 0;	

	static long readCount = 0;
	static long writeCount = 0;
	static boolean startedSampling = false;
	static boolean endedSampling = false;


	private volatile long completedCount = 0;
	private volatile long committedCount = 0;
	private volatile long abortedCount = 0;
	private volatile long earlyAbort = 0;

	private final Map<RequestId, byte[]> requestIdValueMap = new HashMap<RequestId, byte[]>();

	private int localId;
	private int min;
	private int max;
	private int numReplicas;
	private int accessibleObjects;

	private int minW;
	private int maxW;
	private int accessibleWarehouses;

	private AtomicInteger TransactionId;


	MonitorThread monitorTh = new MonitorThread();

	/*************************************************************************
	 * This class is only for taking the readings from the experiment. The
	 * sampling thread is triggered when read/write count reaches a particular
	 * limit, it goes to sleep for 20 seconds and then it samples the reading.
	 * 
	 * @author sachin
	 * 
	 ************************************************************************/
	class MonitorThread extends Thread {
		public void run() {
			long start;
			long count = 0;
			long localReadCount = 0;
			long localWriteCount = 0;
			long localAbortCount = 0;
			long localCompletedCount = 0;
			long localSubmitCount = 0;
			long localearlyAbort = 0;

			System.out
					.println("Read-Throughput/S  Write Throughput/S  CompletedCount  Latency Aborts LocalAborts  Time");
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			System.out.println(" ");
			// Sample time is 20 seconds
			while (count < 10) {
				startRead = System.currentTimeMillis();

				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				localReadCount = readCount;
				localWriteCount = committedCount;
				localAbortCount = abortedCount;
				localCompletedCount = completedCount;
				localSubmitCount = client.getSubmitCount();
				localearlyAbort = earlyAbort;
				endRead = System.currentTimeMillis();
				
				client.collectLatencyData();

				System.out.format("%6d  %6d  %6d  %5.3f %6d  %6d  %6d\n",
						((localReadCount - lastReadCount) * 1000)
								/ (endRead - startRead),
						((localWriteCount - lastWriteCount) * 1000)
								/ (endRead - startRead),
						((localCompletedCount - lastCompletedCount) * 1000)
								/ (endRead - startRead),
						client.getWriteLatency(),
						((localAbortCount - lastAbortCount) * 1000)
								/ (endRead - startRead),
						((localearlyAbort - lastearlyAbort) * 1000)
								/ (endRead - startRead),
						(endRead - startRead));

				lastReadCount = localReadCount;
				lastWriteCount = localWriteCount;
				lastAbortCount = localAbortCount;
				lastCompletedCount = localCompletedCount;
				lastSubmitCount = localSubmitCount;
				lastearlyAbort = localearlyAbort;
				count++;
			}
		
			try 
			{
				Thread.sleep(10);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			/*
			System.out.println("Getting the owners of each warehouse");
			for (int id = 0; id < NUM_WAREHOUSES; id++) 
			{
				final String myid = "w_" + Integer.toString(id);
				int owner = sharedObjectRegistry.getOwner(myid);
				System.out.println("Owner of warehouse " + myid + " is" + owner);
			}	
			*/
			
			System.exit(0);
		}
	}

	public void TpccInit(SharedObjectRegistry sharedObjectRegistry,
			PaxosSTM stminstance, int warehouseCount, int itemCount) {

		this.NUM_ITEMS = itemCount;
		this.NUM_WAREHOUSES = warehouseCount;

		this.sharedObjectRegistry = sharedObjectRegistry;
		this.stmInstance = stminstance;

		for (int id = 0; id < NUM_ITEMS; id++) {
			final String myid = "i_" + Integer.toString(id);
			TpccItem item = new TpccItem(myid);
			this.sharedObjectRegistry.registerObjects(myid, item);
		}
		// System.out.println("Size of shared Object Memory = " +
		// this.sharedObjectRegistry.getCapacity());

		for (int id = 0; id < NUM_WAREHOUSES; id++) {
			final String myid = "w_" + Integer.toString(id);
			TpccWarehouse warehouse = new TpccWarehouse(myid);
			this.sharedObjectRegistry.registerObjects(myid, warehouse);

			for (int s_id = 0; s_id < NUM_ITEMS; s_id++) {
				final String smyid = myid + "_s_" + Integer.toString(s_id);
				TpccStock stock = new TpccStock(smyid);
				this.sharedObjectRegistry.registerObjects(smyid, stock);
			}
			for (int d_id = 0; d_id < NUM_DISTRICTS; d_id++) {
				String dmyid = myid + "_" + Integer.toString(d_id);
				TpccDistrict district = new TpccDistrict(dmyid);
				this.sharedObjectRegistry.registerObjects(dmyid, district);

				for (int c_id = 0; c_id < NUM_CUSTOMERS_PER_D; c_id++) {
					String cmyid = myid + "_c_" + Integer.toString(c_id);
					TpccCustomer customer = new TpccCustomer(cmyid);
					this.sharedObjectRegistry.registerObjects(cmyid, customer);

					String hmyid = myid + "_h_" + Integer.toString(c_id);
					TpccHistory history = new TpccHistory(hmyid, c_id, d_id);
					this.sharedObjectRegistry.registerObjects(hmyid, history);

				}
			}
			for (int o_id = 0; o_id < NUM_ORDERS_PER_D; o_id++) {
				String omyid = myid + "_o_" + Integer.toString(o_id);
				TpccOrder order = new TpccOrder(omyid);
				this.sharedObjectRegistry.registerObjects(omyid, order);

				String olmyid = myid + "_ol_" + Integer.toString(o_id);
				TpccOrderline orderLine = new TpccOrderline(olmyid);
				this.sharedObjectRegistry.registerObjects(olmyid, orderLine);
			}
		}

		//System.out.println("Size of shared Object Registry = "
		//		+ this.sharedObjectRegistry.getCapacity());

		this.tpccSTMDispatcher = new SingleThreadDispatcher("TpccSTM");
		// this.tpccSTMDispatcher.start();
		monitorTh.start();
	}

	public void initRequests() {
		this.localId = ProcessDescriptor.getInstance().localId;
		this.numReplicas = ProcessDescriptor.getInstance().numReplicas;
		this.TransactionId = new AtomicInteger(this.localId + 1);
		
	
		/* Have a shared portion of items */
                this.accessibleObjects = this.NUM_ITEMS / (numReplicas + 1);
                this.min = this.accessibleObjects * (this.localId + 1);
                this.max = (this.accessibleObjects * (this.localId + 2));
                //this.item_count = (max - min)/MaxSpec;

		/*
		this.accessibleObjects = this.NUM_ITEMS / numReplicas;
		this.min = this.accessibleObjects * this.localId;
		this.max = (this.accessibleObjects * (this.localId + 1));*/
		//System.out.println("O:" + this.accessibleObjects + "M:" + this.max +
		//"m:" + this.min);
		/*
		this.accessibleWarehouses = this.NUM_WAREHOUSES / numReplicas;
		this.minW = this.accessibleWarehouses * this.localId;
		this.maxW = (this.accessibleWarehouses * (this.localId + 1));*/

		/* Have a shared portion of warehouses */
                this.accessibleWarehouses = this.NUM_WAREHOUSES / (numReplicas + 1);
                this.minW = this.accessibleWarehouses * (this.localId + 1);
                this.maxW = (this.accessibleWarehouses * (this.localId + 2));
                //this.w_count = (maxW - minW)/MaxSpec;


		//System.out.println("O:" + this.accessibleWarehouses + "M:" +
		//this.maxW +
		//"m:" + this.minW);
	}

	public void initClient(TpccMultiClient client) {
		this.client = client;
	}

	protected void orderStatus(ClientRequest cRequest, int count, boolean retry, int Tid) {
		int success = 0;
		RequestId requestId = cRequest.getRequestId();
		String myid = "w_"
				+ Integer.toString(random.nextInt(maxW - minW) + minW);
		String cmyid = myid + "_c_"
				+ Integer.toString(random.nextInt(NUM_CUSTOMERS_PER_D));

		TpccWarehouse warehouse = ((TpccWarehouse) stmInstance.open(myid,
				this.stmInstance.TX_READ_MODE, requestId,
				this.stmInstance.OBJECT_READ_MODE, retry, Tid));

		TpccCustomer customer = ((TpccCustomer) stmInstance.open(cmyid,
				this.stmInstance.TX_READ_MODE, requestId,
				this.stmInstance.OBJECT_READ_MODE, retry, Tid));

		final String omyid = myid + "_o_"
				+ Integer.toString(random.nextInt(NUM_ORDERS_PER_D));
		TpccOrder order = ((TpccOrder) stmInstance.open(omyid,
				this.stmInstance.TX_READ_MODE, requestId,
				this.stmInstance.OBJECT_READ_MODE, retry, Tid));

		float olsum = (float) 0;
		int i = 1;
		while (i < order.O_OL_CNT) {
			final String olmyid = myid + "_ol_" + Integer.toString(i);
			TpccOrderline orderline = ((TpccOrderline) stmInstance.open(olmyid,
					this.stmInstance.TX_READ_MODE, requestId,
					this.stmInstance.OBJECT_READ_MODE, retry, Tid));

			if (orderline != null) {
				olsum += orderline.OL_AMOUNT;
				i += 1;
			}
		}
		sendReply(ByteBuffer.allocate(4).putInt(success).array(), cRequest);

	}

	protected void delivery(ClientRequest cRequest, int count, boolean retry ,int Tid) {
		int success = 0;
		RequestId requestId = cRequest.getRequestId();
		
		boolean xretry = true;
		
		final String myid = "w_"
					+ Integer.toString(random.nextInt(maxW - minW) + minW);

			// System.out.println("delivery: " + myid);

			
		while(xretry == true)
		{
			xretry = false;
			TpccWarehouse warehouse = ((TpccWarehouse) stmInstance.open(myid,
					this.stmInstance.TX_READ_WRITE_MODE, requestId,
					this.stmInstance.OBJECT_WRITE_MODE, retry, Tid));

			for (int d_id = 0; d_id < NUM_DISTRICTS; d_id++) {

				final String omyid = myid + "_o_"
						+ Integer.toString(random.nextInt(NUM_ORDERS_PER_D));
				final String cmyid = myid + "_c_"
						+ Integer.toString(random.nextInt(NUM_CUSTOMERS_PER_D));

				TpccOrder order = ((TpccOrder) stmInstance.open(omyid,
						this.stmInstance.TX_READ_WRITE_MODE, requestId,
						this.stmInstance.OBJECT_READ_MODE, retry, Tid));

				float olsum = (float) 0;
				String crtdate = new java.util.Date().toString();
				int i = 1;
				while (i < order.O_OL_CNT) {
					if (i < NUM_ORDERS_PER_D) {
						final String olmyid = myid + "_ol_" + Integer.toString(i);
						TpccOrderline orderline = ((TpccOrderline) stmInstance
								.open(olmyid, this.stmInstance.TX_READ_WRITE_MODE,
										requestId,
										this.stmInstance.OBJECT_READ_MODE, retry, Tid));

						if (orderline != null) {
							olsum += orderline.OL_AMOUNT;
							i += 1;
						}
					}
				}
				TpccCustomer customer = ((TpccCustomer) stmInstance.open(cmyid,
						this.stmInstance.TX_READ_WRITE_MODE, requestId,
						this.stmInstance.OBJECT_WRITE_MODE, retry, Tid));

				customer.C_BALANCE += olsum;
				customer.C_DELIVERY_CNT += 1;
			}

			// update shared copy completed-but-not-committed copy with the write
			// set
			//stmInstance.updateUnCommittedSharedCopy(requestId);
		
			TransactionContext ctx = stmInstance.getTransactionContext(requestId);
			if(!stmInstance.validateAndLockReadset(ctx, Tid))
			{
				xretry = true;
				stmInstance.unlockReadSet(ctx, requestId, Tid);
				earlyAbort++;
				try
                     		{
                        		Thread.sleep(1);
                      		}
                    		catch (InterruptedException ex)
                  		{
                        		ex.printStackTrace();
                 		}
			}
		}
	
		completedCount++;
		byte[] result = ByteBuffer.allocate(4).putInt(success).array();
		stmInstance.storeResultToContext(requestId, result);
		//System.out.println("Delivery Transaction " + Tid + " locally committed");
	}

	protected void stockLevel(ClientRequest cRequest, int count, boolean retry, int Tid) {
		int success = 0;
		int i = 0;
		RequestId requestId = cRequest.getRequestId();
		final String myid = "w_"
				+ Integer.toString(random.nextInt(maxW - minW) + minW);
		while (i < 20) {

			/*************** Transaction start ***************/
			final String omyid = myid + "_o_"
					+ Integer.toString(random.nextInt(NUM_ORDERS_PER_D));

			TpccWarehouse warehouse = ((TpccWarehouse) stmInstance.open(myid,
					this.stmInstance.TX_READ_MODE, requestId,
					this.stmInstance.OBJECT_READ_MODE, retry, Tid));

			TpccOrder order = ((TpccOrder) stmInstance.open(omyid,
					this.stmInstance.TX_READ_MODE, requestId,
					this.stmInstance.OBJECT_READ_MODE, retry, Tid));

			if (order != null) {
				int j = 1;
				while (j < order.O_OL_CNT) {
					if (j < NUM_ORDERS_PER_D) {
						final String olmyid = myid + "_ol_"
								+ Integer.toString(j);
						TpccOrderline orderline = ((TpccOrderline) stmInstance
								.open(olmyid, this.stmInstance.TX_READ_MODE,
										requestId,
										this.stmInstance.OBJECT_READ_MODE,
										retry, Tid));
					}
					j += 1;
				}
			}

			/*************** Transaction end ***************/

			i += 1;
		}

		int k = 1;
		while (k <= 10) {
			String wid = "w_"
					+ Integer.toString(random.nextInt(maxW - minW) + minW);
			if (k < NUM_ITEMS) {
				String smyid = wid + "_s_" + Integer.toString(k);
				TpccStock stock = ((TpccStock) stmInstance.open(smyid,
						this.stmInstance.TX_READ_MODE, requestId,
						this.stmInstance.OBJECT_READ_MODE, retry, Tid));

				// HyFlow.getLocator().open(smyid, "r");
				k += 1;
			} else
				k += 1;
		}
		sendReply(ByteBuffer.allocate(4).putInt(success).array(), cRequest);
	}

	protected void newOrder(ClientRequest cRequest, int count, boolean retry, int Tid) {

		int success = 0;
		RequestId requestId = cRequest.getRequestId();
		
		boolean xretry = true;

		Random randomGenerator = new Random();
                int randomInt = randomGenerator.nextInt(100);

		int tw_id = 0;
		if(randomInt < 0)
                {
                        tw_id = random.nextInt(this.NUM_WAREHOUSES/(this.numReplicas));
                }
		else
		{
			tw_id = random.nextInt(maxW - minW) + minW;
		}
		
		final int w_id = tw_id;
		final String myid = "w_" + Integer.toString(w_id);

		// System.out.println("order: " + myid);
		while(xretry == true)	
		{
			xretry = false;
			TpccWarehouse warehouse = ((TpccWarehouse) stmInstance.open(myid,
					this.stmInstance.TX_READ_WRITE_MODE, requestId,
					this.stmInstance.OBJECT_WRITE_MODE, retry, Tid));

			final int d_id = random.nextInt(NUM_DISTRICTS);
			final String dmyid = myid + "_" + Integer.toString(d_id);
			TpccDistrict district = ((TpccDistrict) stmInstance.open(dmyid,
					this.stmInstance.TX_READ_WRITE_MODE, requestId,
					this.stmInstance.OBJECT_WRITE_MODE, retry, Tid));

			double D_TAX = district.D_TAX;
			int o_id = district.D_NEXT_O_ID;
			district.D_NEXT_O_ID = o_id + 1;
			final int c_id = random.nextInt(NUM_CUSTOMERS_PER_D);
			final String cmyid = myid + "_c_" + Integer.toString(c_id);
			TpccCustomer customer = ((TpccCustomer) stmInstance.open(cmyid,
					this.stmInstance.TX_READ_WRITE_MODE, requestId,
					this.stmInstance.OBJECT_WRITE_MODE, retry, Tid));

			double C_DISCOUNT = customer.C_DISCOUNT;
			String C_LAST = customer.C_LAST;
			String C_CREDIT = customer.C_CREDIT;

			// Create entries in ORDER and NEW-ORDER
			final String omyid = myid + "_o_"
					+ Integer.toString(random.nextInt(NUM_ORDERS_PER_D));

			TpccOrder order = new TpccOrder(omyid);
			order.O_C_ID = c_id;
			order.O_CARRIER_ID = Integer.toString(random.nextInt(15)); // Check the
																	// specification
																	// for this
																	// value
			order.O_ALL_LOCAL = true;
			int i = 1;
			while (i <= order.O_CARRIER_ID.length()) {
				final int i_id = random.nextInt((max - min)) + min;
				
				String item_id = "i_" + Integer.toString(i_id);
				TpccItem item = ((TpccItem) stmInstance.open(item_id,
						this.stmInstance.TX_READ_MODE, requestId,
						this.stmInstance.OBJECT_WRITE_MODE, retry, Tid));
			
				if (item == null) {
					System.out.println("Item is null >>>");
					System.exit(-1);
					// return null;
				}

				
				float I_PRICE = item.I_PRICE;
				String I_NAME = item.I_NAME;
				String I_DATA = item.I_DATA;

				String olmyid = myid + "_ol_"
						+ Integer.toString(random.nextInt(1000) + NUM_ORDERS_PER_D);
				TpccOrderline orderLine = new TpccOrderline(olmyid);
				// TODO How to add the new object to shared object registry.
				// This should also be supported in STM framework
				orderLine.OL_QUANTITY = random.nextInt(1000);
				orderLine.OL_I_ID = i_id;
				orderLine.OL_SUPPLY_W_ID = w_id;
				orderLine.OL_AMOUNT = (int) (orderLine.OL_QUANTITY * I_PRICE);
				orderLine.OL_DELIVERY_D = null;
				orderLine.OL_DIST_INFO = Integer.toString(d_id);
				i += 1;
			}

			// update shared copy completed-but-not-committed copy with the write
			// set
			//stmInstance.updateUnCommittedSharedCopy(requestId);
			TransactionContext ctx = stmInstance.getTransactionContext(requestId);
			if(!stmInstance.validateAndLockReadset(ctx, Tid))
			{
				xretry = true;
				stmInstance.unlockReadSet(ctx, requestId, Tid);
				earlyAbort++;
				try
                     		{
                        		Thread.sleep(1);
                      		}
                    		catch (InterruptedException ex)
                  		{
                        		ex.printStackTrace();
                 		}
			}
		}
		completedCount++;
		byte[] result = ByteBuffer.allocate(4).putInt(success).array();
		stmInstance.storeResultToContext(requestId, result);
		//System.out.println("NewOrder Transaction " + Tid + " locally committed");
	}

	protected void payment(ClientRequest cRequest, int count, boolean retry, int Tid) {
		int success = 0;
		RequestId requestId = cRequest.getRequestId();	
		boolean xretry = true;
	


		Random randomGenerator = new Random();
                int randomInt = randomGenerator.nextInt(100);

		final float h_amount = (float) (random.nextInt(500000) * 0.01);
		int tw_id = 0;
		if(randomInt < 0)
                {
                        tw_id = random.nextInt(this.NUM_WAREHOUSES/(this.numReplicas));
                }
		else
		{
			tw_id = random.nextInt(maxW - minW) + minW;
		}
		//final int w_id = random.nextInt(maxW - minW) + minW;
		final int w_id = tw_id;
	
		while(xretry == true)
		{
			xretry = false;	
			final String myid = "w_" + Integer.toString(w_id);
			final int c_id = random.nextInt(NUM_CUSTOMERS_PER_D);
			final String cmyid = myid + "_c_" + Integer.toString(c_id);

			// System.out.println("payment: " + myid);

			// Open Wairehouse Table
			TpccWarehouse warehouse = ((TpccWarehouse) stmInstance.open(myid,
					this.stmInstance.TX_READ_WRITE_MODE, requestId,
					this.stmInstance.OBJECT_WRITE_MODE, retry, Tid));
			warehouse.W_YTD += h_amount;

			// In DISTRICT table
			final int d_id = random.nextInt(NUM_DISTRICTS);
			final String dmyid = myid + "_" + Integer.toString(d_id);
			TpccDistrict district = ((TpccDistrict) stmInstance.open(dmyid,
					this.stmInstance.TX_READ_WRITE_MODE, requestId,
					this.stmInstance.OBJECT_WRITE_MODE, retry, Tid));
			district.D_YTD += h_amount;

			TpccCustomer customer = ((TpccCustomer) stmInstance.open(cmyid,
					this.stmInstance.TX_READ_WRITE_MODE, requestId,
					this.stmInstance.OBJECT_WRITE_MODE, retry, Tid));

			customer.C_BALANCE -= h_amount;
			customer.C_YTD_PAYMENT += h_amount;
			customer.C_PAYMENT_CNT += 1;
			
			TransactionContext ctx = stmInstance.getTransactionContext(requestId);
			if(!stmInstance.validateAndLockReadset(ctx, Tid))
			{
				xretry = true;
				stmInstance.unlockReadSet(ctx, requestId, Tid);
				earlyAbort++;
				try
                     		{
                        		Thread.sleep(1);
                      		}
                    		catch (InterruptedException ex)
                  		{
                        		ex.printStackTrace();
                 		}
			}
			

		
		}
		// update shared copy completed-but-not-committed copy with the write
		// set
		//stmInstance.updateUnCommittedSharedCopy(requestId);
		completedCount++;
		byte[] result = ByteBuffer.allocate(4).putInt(success).array();
		stmInstance.storeResultToContext(requestId, result);
		//System.out.println("Payment Transaction " + Tid + " locally committed");
	}

	/**
	 * Pass reference to replice for sending a reply to client after read
	 * request is executed or write request is committed.
	 */
	public void setReplica(Replica replica) {
		this.replica = replica;
	}

	/**
	 * Used to execute read requests from clients locally.
	 */
	@Override
	public void executeReadRequest(final ClientRequest cRequest) {
		// TODO Auto-generated method stub
		tpccSTMDispatcher.submit(new Runnable() {
			public void run() {
				executeRequest(cRequest, false);
			}
		});
	}

	/**
	 * This method is used for three purposes. 1. For read transaction 2. For
	 * write transaction which is executed speculatively 3. For write
	 * transaction which is retied after commit failed
	 * 
	 * Retried method is executed by writeExecutor itself therefore there is no
	 * need to specifically execute the retry on writeExecutor
	 * 
	 * @param request
	 * @param retry
	 */
	public void executeRequest(final ClientRequest request, final boolean retry) {
		byte[] value = request.getValue();
		ByteBuffer buffer = ByteBuffer.wrap(value);
		final RequestId requestId = request.getRequestId();

		byte transactionType = buffer.get();
		byte command = buffer.get();
		final int count = buffer.getInt();
		final int Tid = TransactionId.getAndAdd(this.numReplicas);
		
		//System.out.println("Starting Request ClientId " + requestId.getClientId() + " SeqNumber " + requestId.getSeqNumber() + " Td = " + Tid);
		//System.out.println("executeRequest start for Tx " + Tid);
		if (transactionType == READ_ONLY_TX) {
			switch (command) {
			case TX_ORDER_STATUS:
				stmInstance.executeReadRequest(new Runnable() {
					public void run() {
						orderStatus(request, count, retry, Tid);
						readCount++;
					}
				});
				break;

			case TX_STOCKLEVEL:
				stmInstance.executeReadRequest(new Runnable() {
					public void run() {
						stockLevel(request, count, retry, Tid);
						readCount++;
					}
				});
				break;

			default:
				System.out.println("Wrong RD command " + command
						+ " transaction type " + transactionType);
				break;
			}
		} else {
			switch (command) {
			case TX_DELIVERY: {
				if (retry == false) {
					requestIdValueMap.put(requestId, value);
					stmInstance.executeWriteRequest(new Runnable() {
						public void run() {
							delivery(request, count, retry, Tid);
							stmInstance.onExecuteComplete(request);
						}
					});
				} else {
					// delivery(request, count, retry);
				}
				break;
			}
			case TX_NEWORDER:
				if (retry == false) {
					requestIdValueMap.put(requestId, value);
					stmInstance.executeWriteRequest(new Runnable() {
						public void run() {
							newOrder(request, count, retry, Tid);
							stmInstance.onExecuteComplete(request);
						}
					});
				} else {
					// newOrder(request, count, retry);
				}
				break;
			case TX_PAYMENT:
				if (retry == false) {
					requestIdValueMap.put(requestId, value);
					stmInstance.executeWriteRequest(new Runnable() {
						public void run() {
							payment(request, count, retry, Tid);
							stmInstance.onExecuteComplete(request);
						}
					});
				} else {
					// payment(request, count, retry);
				}
				break;
			default:
				System.out.println("Wrong WR command " + command
						+ " transaction type " + transactionType);
				break;

			}
		}
		//System.out.println("Execute End");
	}

	/**
	 * A common interface to send the reply to client request back to client
	 * through replica
	 * 
	 * @param result
	 * @param cRequest
	 */
	public void sendReply(byte[] result, ClientRequest cRequest) {
		// System.out.println("Sending reply to " +
		// cRequest.getRequestId().toString());
		// replica.replyToClient(result, cRequest);
	}

	@Override
	public void notifyCommitManager(Request request) {
		// System.out.print("!");
		stmInstance.notifyCommitManager(request);
	}

	/**
	 * Called by network layer to commit a previous speculatively executed
	 * batch.
	 */
	@Override
	public void commitBatchOnDecision(final RequestId rId,
			final TransactionContext ctx) {
		// TODO Auto-generated method stub
		// Validate sequence
		// If validated - commit -- Delete RequestId from
		// LocalTransactionManager.requestDirtycopyMap
		// else abort and retry

		stmInstance.executeCommitRequest(new Runnable() {
			public void run() {
				// System.out.println("Comm");
				onCommit(rId, ctx);

				writeCount++;
			}
		});
	}

	/*************************************************************************
	 * 
	 * @param requestId
	 * @param commandType
	 * @return
	 * 
	 *         This method commits the given transaction. It first validates the
	 *         readset and then after validating shadowcopy too, updates the
	 *         objects in SharedObjectRegistry with the shadowcopy. Finally it
	 *         removes all the data for optimistically executed transaction
	 *         (cleanup).
	 ************************************************************************/
	public void onCommit(RequestId requestId, TransactionContext ctx) {

		// Validate the transaction object versions or Decided InstanceIds and
		// sequence numbers
		// Check the version of object in stable copy and see if the current
		// shadowcopy version is +1
		// and InstanceId of shadowcopy matches with the stable copy.
		// Object object = null;
		// boolean retry = true;
		// RequestId requestId = cRequest.getRequestId();

		// Validate read set first
		int Tid = 0;
		TransactionContext old_ctx = stmInstance.getTransactionContext(requestId);
		if(old_ctx != null)
		{
			Tid = old_ctx.getTransactionId();

		}	
		//if(Tid != 0)
		//	System.out.println("Executing commit for Request ClientId " + requestId.getClientId() + " SeqNumber " + requestId.getSeqNumber() + " Td = " + Tid);
		
		if (stmInstance.validateReadset(ctx)) {
			stmInstance.updateSharedObject(ctx, requestId, Tid);
			committedCount++;
		} else {
			// Isn;t it needed to remove the previous content
			//stmInstance.emptyWriteSet(ctx,false);
                        //System.out.println("Abort during final commit");
			stmInstance.unlockReadSet(ctx, requestId, Tid);
			stmInstance.removeTransactionContext(requestId);

			byte[] value = requestIdValueMap.get(requestId);
                        if(value != null)
                        {
                                ClientRequest cRequest = new ClientRequest(requestId , value);

                                executeRequest(cRequest, false);
                        }

			// executeRequest(cRequest, retry);
			// stmInstance.updateSharedObject(requestId);
			abortedCount++;
			return;
		}
		// committedCount++;
		// remove the entries for this transaction LTM (TransactionContext,
		// lastModifier)
		// object = stmInstance.getResultFromContext(requestId);
		// sendReply(stmInstance.getResultFromContext(requestId), cRequest);

		client.replyToClient(requestId);

		stmInstance.removeTransactionContext(requestId);
		requestIdValueMap.remove(requestId);
		// Object object = null;
		// boolean retry = false;
		// RequestId requestId = cRequest.getRequestId();
		// // Normally retries will be limited to only one retry, still kept
		// this loop
		// for(int i =0; i < RETRY_COUNT; i++) {
		//
		// if(retry == false) {
		// boolean valid = stmInstance.validateReadset(requestId);
		// if(valid) {
		// stmInstance.updateSharedObject(requestId);
		// break;
		// } else {
		// retry = true;
		// // Isn;t it needed to remove the previous content
		// executeRequest(cRequest, retry);
		// }
		// } else {
		// stmInstance.updateSharedObject(requestId);
		// }
		//
		// // // Validate read set first
		// // if(stmInstance.validateReadset(requestId)) {
		// // stmInstance.updateSharedObject(requestId);
		// // break;
		// // } else {
		// // retry = true;
		// // // Isn;t it needed to remove the previous content
		// // //stmInstance.removeTransactionContext(requestId);
		// // executeRequest(cRequest, retry);
		// // }
		// }
		//
		// // remove the entries for this transaction LTM (TransactionContext,
		// lastModifier)
		// object = stmInstance.getResultFromContext(requestId);
		// // byte command = getCommandName(requestIdValueMap.get(requestId));
		// sendReply(stmInstance.getResultFromContext(requestId), cRequest);
		//
		// stmInstance.removeTransactionContext(requestId);
		// requestIdValueMap.remove(requestId);
	}

	/**
	 * Read the command name from the client request byte array.
	 * 
	 * @param value
	 * @return
	 */
	public byte getCommandName(byte[] value) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		byte transactionType = buffer.get();
		byte command = buffer.get();
		buffer.flip();
		return command;

	}

	/************************************************************************
	 * 
	 * @param requestId
	 * 
	 *            This method rollsback all the changes performed on shadowcopy
	 *            of the optimistically executed transaction.
	 */
	public void rollback(RequestId requestId) {
		stmInstance.removeTransactionContext(requestId);
	}

	public void sanityCheck() {
		// TODO
	}

	@Override
	public byte[] serializeTransactionContext(TransactionContext ctx)
			throws IOException {
		Map<String, AbstractObject> readset = ctx.getReadSet();
		Map<String, AbstractObject> writeset = ctx.getWriteSet();

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteBuffer bb;

		/*int Tid = ctx.getTransactionId();
		
		/*System.out.println("Serializing " + Tid);
		/* Add Tid to ctx*/
		//bb = ByteBuffer.allocate(4);
		//bb.putInt(Tid);
		//bb.flip();
		//out.write(bb.array());

		bb = ByteBuffer.allocate(4);
		bb.putInt(readset.size());
		bb.flip();

		out.write(bb.array());
		for (Map.Entry<String, AbstractObject> entry : readset.entrySet()) {
			String id = entry.getKey();
			byte[] idBytes = id.getBytes(Charset.forName("UTF-8"));

			bb = ByteBuffer.allocate(idBytes.length + 4 + 8);

			bb.putInt(idBytes.length);
			bb.put(idBytes);
			bb.putLong(entry.getValue().getVersion());

			bb.flip();
			out.write(bb.array());
		}

		bb = ByteBuffer.allocate(4);
		bb.putInt(writeset.size());
		bb.flip();

		// System.out.println("SW:" + writeset.size());

		out.write(bb.array());
		for (Map.Entry<String, AbstractObject> entry : writeset.entrySet()) {
			String id = entry.getKey();
			byte[] idBytes = id.getBytes(Charset.forName("UTF-8"));

			ByteBuffer bb1 = ByteBuffer.allocate(idBytes.length + 4);

			bb1.putInt(idBytes.length);
			bb1.put(idBytes);

			bb1.flip();
			out.write(bb1.array());

			Object object = entry.getValue();
			if (object instanceof TpccWarehouse) {
				bb = ByteBuffer.allocate(2 + 8);

				bb.putShort((short) 0);

				TpccWarehouse warehouse = (TpccWarehouse) object;

				bb.putLong(warehouse.getVersion());

				bb.flip();
				out.write(bb.array());

			} else if (object instanceof TpccCustomer) {
				bb = ByteBuffer.allocate(2 + 8 + 8 + 4 + 4 + 8);

				bb.putShort((short) 1);

				TpccCustomer customer = (TpccCustomer) object;

				bb.putDouble(customer.C_BALANCE);
				bb.putDouble(customer.C_YTD_PAYMENT);
				bb.putInt(customer.C_DELIVERY_CNT);
				bb.putInt(customer.C_PAYMENT_CNT);

				bb.putLong(customer.getVersion());

				bb.flip();
				out.write(bb.array());

			} else if (object instanceof TpccDistrict) {
				bb = ByteBuffer.allocate(2 + 4 + 8 + 8);

				bb.putShort((short) 2);

				TpccDistrict district = (TpccDistrict) object;

				bb.putInt(district.D_NEXT_O_ID);
				bb.putDouble(district.D_YTD);

				bb.putLong(district.getVersion());

				bb.flip();
				out.write(bb.array());

			} else if (object instanceof TpccItem) {
				bb = ByteBuffer.allocate(2 + 8);

				bb.putShort((short) 3);

				TpccItem item = (TpccItem) object;

				bb.putLong(item.getVersion());

				bb.flip();
				out.write(bb.array());

			} else if (object instanceof TpccOrder) {
				TpccOrder order = (TpccOrder) object;

				String str = order.O_CARRIER_ID;
				byte[] strBytes = str.getBytes();

				bb = ByteBuffer.allocate(2 + 4 + 4 + strBytes.length + 1 + 8);

				bb.putShort((short) 4);

				bb.putInt(order.O_C_ID);

				bb.putInt(strBytes.length);
				bb.put(strBytes);

				bb.put(new byte[] { (byte) (order.O_ALL_LOCAL ? 1 : 0) });

				bb.putLong(order.getVersion());

				bb.flip();
				out.write(bb.array());

			} else if (object instanceof TpccOrderline) {

				TpccOrderline orderline = (TpccOrderline) object;

				String str = orderline.OL_DELIVERY_D;
				byte[] strBytes = str.getBytes();

				String str1 = orderline.OL_DIST_INFO;
				byte[] strBytes1 = str1.getBytes();

				bb = ByteBuffer.allocate(2 + 4 + 4 + 4 + 4 + 4
						+ strBytes.length + 4 + strBytes1.length + 8);

				bb.putShort((short) 5);

				bb.putInt(orderline.OL_QUANTITY);
				bb.putInt(orderline.OL_I_ID);
				bb.putInt(orderline.OL_SUPPLY_W_ID);
				bb.putInt(orderline.OL_AMOUNT);

				bb.putInt(strBytes.length);
				bb.put(strBytes);

				bb.putInt(strBytes1.length);
				bb.put(strBytes1);

				bb.putLong(orderline.getVersion());

				bb.flip();
				out.write(bb.array());

			} else if (object instanceof TpccStock) {
				bb = ByteBuffer.allocate(2 + 8);

				bb.putShort((short) 6);

				TpccStock stock = (TpccStock) object;

				bb.putLong(stock.getVersion());

				bb.flip();
				out.write(bb.array());

			} else {
				System.out
						.println("Tpcc Object serialization: object not defined");
			}
		}

		return out.toByteArray();
	}

	@Override
	public TransactionContext deserializeTransactionContext(byte[] bytes)
			throws IOException {

		ByteBuffer bb = ByteBuffer.wrap(bytes);

		//int Tid = bb.getInt();
		
		//System.out.println("Deseriailize " + Tid);	
		TransactionContext ctx = new TransactionContext();

		
		int readsetSize = bb.getInt();
		for (int i = 0; i < readsetSize; i++) {
			byte[] value = new byte[bb.getInt()];
			bb.get(value);

			String id = new String(value, Charset.forName("UTF-8"));

			long version = bb.getLong();

			TpccItem object = new TpccItem(id);
			object.setVersion(version);

			ctx.addObjectToReadSet(id, object);
		}

		int writesetSize = bb.getInt();
		for (int i = 0; i < writesetSize; i++) {
			byte[] value = new byte[bb.getInt()];
			bb.get(value);

			String id = new String(value, Charset.forName("UTF-8"));

			short type = bb.getShort();

			switch (type) {
			case 0: {
				TpccWarehouse object = new TpccWarehouse(id);

				object.setVersion(bb.getLong());

				ctx.addObjectToWriteSet(id, object);
				break;
			}
			case 1: {
				TpccCustomer object = new TpccCustomer(id);

				object.C_BALANCE = bb.getDouble();
				object.C_YTD_PAYMENT = bb.getDouble();
				object.C_DELIVERY_CNT = bb.getInt();
				object.C_PAYMENT_CNT = bb.getInt();

				object.setVersion(bb.getLong());

				ctx.addObjectToWriteSet(id, object);
				break;
			}
			case 2: {
				TpccDistrict object = new TpccDistrict(id);

				object.D_NEXT_O_ID = bb.getInt();
				object.D_YTD = bb.getDouble();

				object.setVersion(bb.getLong());

				ctx.addObjectToWriteSet(id, object);
				break;
			}
			case 3: {
				TpccItem object = new TpccItem(id);

				object.setVersion(bb.getLong());

				ctx.addObjectToWriteSet(id, object);
				break;
			}
			case 4: {
				TpccOrder object = new TpccOrder(id);

				object.O_C_ID = bb.getInt();

				byte[] v = new byte[bb.getInt()];
				bb.get(v);

				String v_string = new String(value, Charset.forName("UTF-8"));

				object.O_CARRIER_ID = v_string;

				object.O_ALL_LOCAL = (bb.get() != 0);

				object.setVersion(bb.getLong());

				ctx.addObjectToWriteSet(id, object);
				break;
			}
			case 5: {
				TpccOrderline object = new TpccOrderline(id);

				object.OL_QUANTITY = bb.getInt();
				object.OL_I_ID = bb.getInt();
				object.OL_SUPPLY_W_ID = bb.getInt();
				object.OL_AMOUNT = bb.getInt();

				byte[] v = new byte[bb.getInt()];
				bb.get(v);
				String v_string = new String(value, Charset.forName("UTF-8"));
				object.OL_DELIVERY_D = v_string;

				v = new byte[bb.getInt()];
				bb.get(v);
				v_string = new String(value, Charset.forName("UTF-8"));
				object.OL_DIST_INFO = v_string;

				object.setVersion(bb.getLong());

				ctx.addObjectToWriteSet(id, object);
				break;
			}
			case 6: {
				TpccStock object = new TpccStock(id);

				object.setVersion(bb.getLong());

				ctx.addObjectToWriteSet(id, object);
				break;
			}
			default:
				System.out.println("Invalid Object Type");
			}

		}

		return ctx;
	}

	/**
	 * This method fills the parameters in the request byte array for client for
	 * the bank benchmark.
	 * 
	 * @param request
	 *            : byte array
	 * @param readOnly
	 *            : boolean specifying what should be the transaction type
	 */
	public byte[] createRequest(boolean readOnly, boolean TpccProfile,
			int percent) {
		byte[] request = new byte[DEFAULT_LENGTH];

		ByteBuffer buffer = ByteBuffer.wrap(request);

		if (TpccProfile) {
			// TPCC workload
			// int percent = random.nextInt(100);
			if (percent < 4) {
				buffer.put(READ_ONLY_TX);
				buffer.put(TX_ORDER_STATUS);
			} else if (percent < 8) {
				buffer.put(READ_ONLY_TX);
				buffer.put(TX_STOCKLEVEL);
			} else if (percent < 12) {
				buffer.put(READ_WRITE_TX);
				buffer.put(TX_DELIVERY);
			} else if (percent < 55) {
				buffer.put(READ_WRITE_TX);
				buffer.put(TX_PAYMENT);
			} else {
				buffer.put(READ_WRITE_TX);
				buffer.put(TX_NEWORDER);
			}
		} else {
			if (readOnly) {
				buffer.put(READ_ONLY_TX);
				int command = random.nextInt(2);
				switch (command) {
				case 0:
					buffer.put(TX_ORDER_STATUS);
					break;
				case 1:
					buffer.put(TX_STOCKLEVEL);
					break;
				}
			} else {
				buffer.put(READ_WRITE_TX);
				int command = random.nextInt(3);
				switch (command) {
				case 0:
					buffer.put(TX_DELIVERY);
					break;
				case 1:
					buffer.put(TX_NEWORDER);
					break;
				case 2:
					buffer.put(TX_PAYMENT);
					break;
				}
			}
		}

		int count = random.nextInt(max - min) + min;

		buffer.putInt(count);

		buffer.flip();
		return request;
	}

	@Override
	public Replica getReplica() {
		return this.replica;
	}

	/**
	 * Shuts down the executors if invoked. Here after no transaction can be
	 * performed.
	 * 
	 * @return
	 */
	public long shutDownExecutors() {
		return stmInstance.shutDownExecutors();
	}

	@Override
	public byte[] execute(byte[] value, int executeSeqNo) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void askForSnapshot(int lastSnapshotNextRequestSeqNo) {
		// TODO Auto-generated method stub

	}

	@Override
	public void forceSnapshot(int lastSnapshotNextRequestSeqNo) {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateToSnapshot(int nextRequestSeqNo, byte[] snapshot) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addSnapshotListener(SnapshotListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeSnapshotListener(SnapshotListener listener) {
		// TODO Auto-generated method stub

	}

	public Object getId() {
		return id;
	}

}
