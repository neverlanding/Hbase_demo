package com.inspur.HbasePro;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

//import com.inspur.bigdata.store.image_sfile.util.Consts;

public class Hdfs35Init {

	private static Logger logger = Logger.getLogger(Hdfs35Init.class);
	private static Configuration conf = new Configuration();

	public static FileSystem getFileSystem() {
		FileSystem fs = null;
		String principal = "hdfs-cluster1@SDWJW.COM";// user为用户
		try {
			// 初始化配置文件
			conf.set("fs.defaultFS", "hdfs://nnha");
			conf.set("dfs.nameservices", "nnha");			
			conf.set("dfs.namenode.rpc-address.nnha.nn1", "sdwjw-agent-35.sdwjw.com:8020");
			conf.set("dfs.namenode.rpc-address.nnha.nn2", "sdwjw-server-92.sdwjw.com:8020");
			conf.set("dfs.ha.namenodes.nnha", "nn1,nn2");
			conf.set("dfs.client.failover.proxy.provider.nnha",
					"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
			conf.set("hadoop.security.authentication", "kerberos");
			conf.set("hadoop.security.authorization", "true");
			conf.set("dfs.namenode.kerberos.principal.pattern", "*");
			
			// 找到集群的管理员用户
			conf.set("dfs.nnha.administrators", "hdfs");
			
			System.clearProperty("java.security.krb5.conf");
			
			String krbStr = null;
			String userkeytab = null;
			
			// 获取krb5.conf文件
//			if(Consts.onWindows){
//				krbStr = "d://krb130//krb5.conf";
//				userkeytab = "d://krb130//superadmin.keytab";
//			}else{
//				krbStr = "/home/niu/hdfsTest/conf/krb5.conf";
//				userkeytab = "/home/niu/hdfsTest/conf/hdfs.headless.keytab";
//			}
			krbStr = "/home/niu/hdfsTest/conf/krb5.conf";
			userkeytab = "/home/niu/hdfsTest/conf/hdfs.headless.keytab";
			// 初始化配置文件
			System.setProperty("java.security.krb5.conf", krbStr);
			// 使用票据和凭证进行认证
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(principal, userkeytab);
			fs = FileSystem.get(conf);
			System.out.println(fs.getFileStatus(new Path("/healthData")));
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
		}
		return fs;
	}

//	public static void main(String[] args){
//		Hdfs35Init.getFileSystem();
//	}

}
