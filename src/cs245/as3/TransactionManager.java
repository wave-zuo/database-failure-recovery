package cs245.as3;

import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	private StorageManager storagemanager;
	private LogManager logmanager;
	private PriorityQueue<Long> checkpoints;//检查点，记录record的偏移量
	public TransactionManager() {
		writesets = new HashMap<>();
		checkpoints = new PriorityQueue<>();
		//see initAndRecover
		latestValues = null;
		storagemanager = null;
		logmanager = null;
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		latestValues = sm.readStoredTable();
		storagemanager = sm;
		logmanager = lm;
		int curOffset = logmanager.getLogTruncationOffset();
		int endOffset = logmanager.getLogEndOffset();

		//对于已成功提交事务进行redo，否则为故障情况，无法判断事务是否写完日志，应回滚
		//正常提交的事务会有已提交OK标志,所以需先找到正常事务，忽略异常事务（相当于undo）
		Set<Long> sucessTxs = new HashSet<>();
		//记录偏移量，由于该成员方法现实中应该单独执行不会并发，所以顺序记录即可
		ArrayList<Integer> tags = new ArrayList<>();
		//通常数据库存储在磁盘，IO成本大，因此尽量将数据放入缓存以减少次数
		List<Record> recordsBuffer = new ArrayList<>();

		while(curOffset < endOffset) {
			//不定长存储，需要先读长度，然后再读整条记录
			byte[] len = logmanager.readLogRecord(curOffset, Short.BYTES);
			ByteBuffer buffer = ByteBuffer.wrap(len);
			short length = buffer.getShort();
			//反序列化
			Record record = Record.deserialized(logmanager.readLogRecord(curOffset, length));
			if(Arrays.equals(record.getValue(), "OK".getBytes())) {
				//是已提交标志，记录事务id
				sucessTxs.add(record.getTxID());
			}
			else {
				recordsBuffer.add(record);//普通record
				tags.add(curOffset);
			}
			curOffset += length;
		}
		for(int i=0; i<recordsBuffer.size(); i++) {
			Record record = recordsBuffer.get(i);
			if(sucessTxs.contains(record.getTxID())) {
				//写入latestValues
				latestValues.put(record.getKey(), new TaggedValue(tags.get(i), record.getValue()));
				//将已提交事务进行redo
				storagemanager.queueWrite(record.getKey(), tags.get(i), record.getValue());
			}
		}
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this
		writesets.put(txID, null);
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key. 
	 */
	public void write(long txID, long key, byte[] value) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));//先写入buffer
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		//WAL先写日志，后写数据。刷日志前先写入了redo log buffer
		HashMap<Long, Long> keyToTag = new HashMap<>();
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				//tag is unused in this implementation:
				//写入latestValues，tag无用
				latestValues.put(x.key, new TaggedValue(0, x.value));

				Record re = new Record(txID, x.key, x.value);
				//将更改先写日志：序列化后写入redo日志，返回偏移量
				long tag = logmanager.appendLogRecord(re.serialized());
				keyToTag.put(x.key, tag);
			}
			//为方便故障恢复时保证事务是原子性的，对已提交事务插入已提交标志。
			Record end = new Record(txID, (long)-1,"OK".getBytes());
			logmanager.appendLogRecord(end.serialized());
			//后写入数据库
			for(WritesetEntry x : writeset) {
				storagemanager.queueWrite(x.key, keyToTag.get(x.key), x.value);
				//记录record的偏移量
				checkpoints.add(keyToTag.get(x.key));
			}
			writesets.remove(txID);
		}
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		writesets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
		checkpoints.remove(persisted_tag);//该record已持久化，不需再保存
		if(checkpoints.size()>0) {
			//当前record已持久化，下次仅需从下一个record位置读取
			logmanager.setLogTruncationOffset(checkpoints.peek().intValue());
		}
		//已到达末尾
		else logmanager.setLogTruncationOffset(logmanager.getLogEndOffset());

	}
}
