package com.inspur.HbasePro;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

//import com.inspur.bigdata.store.image_sfile.util.Consts;

/**
 * hbase 连接初始化
 * 
 * @author niubingru 2017年10月24日
 */
public class Hbase35Init {
	private static Logger logger = Logger.getLogger(Hbase35Init.class);
	public static Connection connection = null;
	private static boolean flag = false;
	private static Object lock = new Object();

	/**
	 * hbase初始化连接
	 * 
	 * @throws IOException
	 */
	public static boolean init() throws IOException {
		if (connection == null) {
			synchronized (lock) {
				if (null == connection) {
					HBaseConfiguration.create();

					Configuration conf = HBaseConfiguration.create();
					conf.set("hbase.zookeeper.quorum",
							"sdwjw-agent-35.sdwjw.com,sdwjw-agent-37.sdwjw.com,sdwjw-agent-36.sdwjw.com");
					conf.set("zookeeper.znode.parent", "/hbase-secure");
					conf.set("hbase.zookeeper.property.clientPort", "2181");
					conf.set("hadoop.security.authentication", "kerberos");
					conf.set("hadoop.security.authorization", "true");
					conf.set("hbase.security.authentication", "kerberos");
					conf.set("hbase.security.authorization", "true");
					// HMaster地址
					conf.set("hbase.master.kerberos.principal", "hbase/_HOST@SDWJW.COM");
					conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@SDWJW.COM");
					// 自定义超时时间，方便调试，可不设置
					conf.setInt("hbase.rpc.timeout", 100000);
					conf.setInt("hbase.client.operation.timeout", 100000);
					conf.setInt("hbase.client.scanner.timeout.period", 100000);

					String principal = "hbase-cluster1@SDWJW.COM";

					System.clearProperty("java.security.krb5.conf");

					String krbStr = null;
					String userkeytab = null;
					
					// 获取krb5.conf文件
//					if(Consts.onWindows){
//						krbStr = "d://krb35//krb5.conf";
//						userkeytab = "d://krb35//hbase.service.keytab";
//					}else{
//						krbStr = "/home/niu/hdfsTest/conf/krb5.conf";
//						userkeytab = "/home/niu/hdfsTest/conf/hbase.headless.keytab";
//					}
					krbStr = "/home/niu/hdfsTest/conf/krb5.conf";
					userkeytab = "/home/niu/hdfsTest/conf/hbase.headless.keytab";
					/*String krbStr = Thread.currentThread().getContextClassLoader().getResource("krb5.conf").getFile();
					String userkeytab = Thread.currentThread().getContextClassLoader().getResource("hbase.headless.keytab").getFile();*/
					
					// 初始化配置文件
					System.setProperty("java.security.krb5.conf", krbStr);
					// 使用票据和凭证进行认证
					UserGroupInformation.setConfiguration(conf);
					UserGroupInformation.loginUserFromKeytab(principal, userkeytab);

					try {
						connection = ConnectionFactory.createConnection(conf);
						flag = true;
					} catch (IOException e) {
						connection = null;
						logger.error("hbase connect exception.", e);
					}
				}
			}
		}

		return flag;
	}

	private Hbase35Init() {
	}

	public static HBaseAdmin getHBaseAdmin() throws IOException {
		if (null == connection || connection.isClosed() || connection.isAborted()) {
			throw new RuntimeException("The connection is invalid!");
		}

		return (HBaseAdmin) connection.getAdmin();
	}

	public static HTable getTable(TableName tableName) throws IOException {
		if (null == connection || connection.isClosed() || connection.isAborted()) {
			throw new RuntimeException("The connection is invalid!");
		}

		return (HTable) connection.getTable(tableName);
	}

	/**
	 * 关闭连接
	 */
	public static void stop() {
		try {
			connection.close();
			connection = null;
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}
//	public static void main(String[] args) throws IOException{
//		init();
//
//		HBaseAdmin admin = getHBaseAdmin();		
//		if(admin.tableExists("niu1")){
//			admin.disableTable(Bytes.toBytes("niu1"));
//			admin.deleteTable(Bytes.toBytes("niu1"));
//		}
//		
//		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("niu1"));
//		HColumnDescriptor fimalyDesc = new HColumnDescriptor("v");
//		tableDesc.addFamily(fimalyDesc);
//		
//		admin.createTable(tableDesc,Bytes.toBytes("1"),Bytes.toBytes("9"),10);
//		/*admin.disableTable(Bytes.toBytes("niu1"));
//		admin.deleteTable(Bytes.toBytes("niu1"));*/
//		System.out.println("done");
//	
//	}
}
