package org.buildoop.spark.auditactivelogins.hbase;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.compress.Compression;

public class HBaseCreateTable {
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("CreateTable {tableName} {columnFamilyName}");
			return;
		}

		String tableName = args[0];
		String columnFamilyName = args[1];

		HBaseAdmin admin = new HBaseAdmin(new Configuration());

		HTableDescriptor tableDescriptor = new HTableDescriptor(); 
		tableDescriptor.setName(Bytes.toBytes(tableName));

		HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamilyName);

		columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
		columnDescriptor.setBlocksize(64 * 1024);
		columnDescriptor.setBloomFilterType(BloomType.ROW);

		tableDescriptor.addFamily(columnDescriptor);

		//tableDescriptor.setValue(tableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());

		System.out.println("-Creating Table");
		admin.createTable(tableDescriptor);

		admin.close();
		System.out.println("-Done");
	}
}
