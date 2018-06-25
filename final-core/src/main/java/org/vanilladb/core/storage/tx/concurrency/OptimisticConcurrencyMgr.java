package org.vanilladb.core.storage.tx.concurrency;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.Transaction.RecordField;

public class OptimisticConcurrencyMgr extends ConcurrencyMgr {
	private static AtomicInteger tnc = new AtomicInteger(-1);
	private static ArrayList<Transaction> currentTxs = new ArrayList<Transaction>();
	
	@Override
	public void onTxStart(Transaction tx) {
		tx.setStartTn(tnc.get());
	}
	
	@Override
	public void onTxCommit(Transaction tx) throws ValidationFaildException {
		boolean valid = true;
		synchronized (tnc) {
			tx.setFinishTn(tnc.get());
			
			for (int i=tx.getStartTn()+1;i<=tx.getFinishTn();i++) {
			    HashMap<RecordField, Constant> tWrite = currentTxs.get(i).getWriteSet();
			    HashSet<RecordField> txRead = tx.getReadSet();
			    
			    for (RecordField k: txRead){
			    	if(tWrite.containsKey(k)) {
				    	valid = false;
				    	break;
				   	}
			    }
			    if(!valid)break;
			}
			if(valid) {
				if(!tx.isReadOnly()) {
					tx.upgradeWriteLock();
					tx.certify();
					tx.commitwriteset();					
					tx.setFinalTn(tnc.incrementAndGet());
					currentTxs.add(tx);
				}
			}
		}
		if(valid) {
			//(cleanup)
		}else{
			//(backup)
			tx.getWriteSet().clear();
			tx.getReadSet().clear();
			tx.setStartTn(tnc.get());
			tx.setFinishTn(0);
			throw new ValidationFaildException("Validation failed! Do backup.");
		}
	}

	@Override
	public void onTxRollback(Transaction tx) {
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
	}

	@Override
	public void modifyFile(String fileName) {
	}

	@Override
	public void readFile(String fileName) {
	}

	@Override
	public void insertBlock(BlockId blk) {
	}

	@Override
	public void modifyBlock(BlockId blk) {
	}

	@Override
	public void readBlock(BlockId blk) {
	}

	@Override
	public void modifyRecord(RecordId recId) {
	}

	@Override
	public void shadowModifyRecord(RecordId recId) {
	}

	@Override
	public void readRecord(RecordId recId) {
	}

	@Override
	public void modifyIndex(String dataFileName) {
	}

	@Override
	public void readIndex(String dataFileName) {
	}

	@Override
	public void modifyLeafBlock(BlockId blk) {
	}

	@Override
	public void readLeafBlock(BlockId blk) {
	}

}
