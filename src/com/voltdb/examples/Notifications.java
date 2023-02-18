package com.voltdb.examples;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Notifications extends VoltProcedure {

	public static SQLStmt CHECK_USAGE = new SQLStmt("");
	private final SQLStmt INSERT_NOTIF = new SQLStmt("insert into notifications values (?, ?, ?)");
	
	public VoltTable[] run() {
		voltQueueSQL(CHECK_USAGE);
		VoltTable usageRecords = voltExecuteSQL()[0];
		
		while(usageRecords.advanceRow()) {
			voltQueueSQL(INSERT_NOTIF);
			voltExecuteSQL();
		}
		
		return null;
	}
}
