package org.buildoop.spark.auditactivelogins.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;
import org.buildoop.spark.auditactivelogins.common.CounterMap;
import org.buildoop.spark.auditactivelogins.common.CounterMap.Counter;


public class HBaseCounterIncrementor {

	static HBaseCounterIncrementor singleton;
	static String tableName;
	static String columnFamily;
	static HTable hTable;
	static long lastUsed;
	static long flushInterval = 0;
	static CloserThread closerThread;
	static FlushThread flushThread;
	static HashMap<String, CounterMap> rowKeyCounterMap = new HashMap<String, CounterMap>(); 
	static Object locker = new Object();
	
	private HBaseCounterIncrementor(String tableName, String columnFamily) {
		HBaseCounterIncrementor.tableName = tableName;
		HBaseCounterIncrementor.columnFamily = columnFamily;
	}
	
	public static HBaseCounterIncrementor getInstance(String tableName, String columnFamily) {
		
		if (singleton == null) {
			synchronized(locker) {
				if (singleton == null) {
					singleton = new HBaseCounterIncrementor(tableName, columnFamily);
					initialize();
				}
			}
		}
		return singleton;
	}
	
	private static void initialize() {
		if (hTable == null) {
			synchronized(locker) {
				if (hTable == null) {
					Configuration hConfig = HBaseConfiguration.create();
					try {
						hTable = new HTable(hConfig, tableName);
						updateLastUsed();
						
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
					flushThread = new FlushThread(flushInterval);
					flushThread.start();
					closerThread = new CloserThread();
					closerThread.start();
				}
			}
		}
	}
	
	public void increment(String rowKey, String key, int increment) {
		increment(rowKey, key, (long)increment);
	}
	
	public void increment(String rowKey, String key, long increment) {
		CounterMap counterMap = rowKeyCounterMap.get(rowKey);
		if (counterMap == null) {
			counterMap = new CounterMap();
			rowKeyCounterMap.put(rowKey, counterMap);
		}
		counterMap.increment(key, increment);
		
		initialize();
	}
	
	private static void updateLastUsed() {
		lastUsed = System.currentTimeMillis();
	}
	
	
	protected void close() {
		if (hTable != null) {
			synchronized(locker) {
				if (hTable != null) {
					if (hTable != null && System.currentTimeMillis() - lastUsed > 30000 ) {
						flushThread.stopLoop();
						flushThread = null;
						try {
							hTable.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
		
						hTable = null;
					}
				}
			}
		}
	}
	
	public static class CloserThread extends Thread {

		boolean continueLoop = true;
		
		@Override
		public void run() {
			while (continueLoop) {
				
				if (System.currentTimeMillis() - lastUsed > 30000 ) {
					singleton.close();
					break;
				}
				
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		public void stopLoop() {
			continueLoop = false;
		}
	}
	
	protected static class FlushThread extends Thread {
		long sleepTime;
		boolean continueLoop = true;

		public FlushThread(long sleepTime) {
			this.sleepTime = sleepTime;
		}

		@Override
		public void run() {
			while (continueLoop) {
				try {
					flushToHBase();	
				} catch (IOException e) {
					e.printStackTrace();
					break;
				}
				
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		private void flushToHBase() throws IOException {
			synchronized(hTable) {
				if (hTable == null) {
					initialize();
				}
				updateLastUsed();
				
				for (Entry<String, CounterMap> entry: rowKeyCounterMap.entrySet()) {
					CounterMap pastCounterMap = entry.getValue();
					rowKeyCounterMap.put(entry.getKey(), new CounterMap());
		
					Increment increment = new Increment(Bytes.toBytes(entry.getKey()));
		
					boolean hasColumns = false;
					for (Entry<String, Counter> entry2 : pastCounterMap.entrySet()) {
						increment.addColumn(Bytes.toBytes(columnFamily),
								Bytes.toBytes(entry2.getKey()), entry2.getValue().value);
						hasColumns = true;
					}
					if (hasColumns) {
						updateLastUsed();
						hTable.increment(increment);
					}
				}
				updateLastUsed();
			}
		}
		
		public void stopLoop() {
			continueLoop = false;
		}
	}
		
		
}
