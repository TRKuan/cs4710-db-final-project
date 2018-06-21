package org.vanilladb.core.storage.tx.concurrency;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class OptimisticConcurrencyMgr extends ConcurrencyMgr {

	@Override
	public void onTxCommit(Transaction tx) {
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
