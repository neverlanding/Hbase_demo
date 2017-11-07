package com.inspur.HbasePro;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer.Option;

public class HbaseOpts35 {
	
	
	public static void main(String[] args){
//		String dd = "/1/2/3/4";
//		String[] ddd = dd.split("/");
//		
//		System.out.println(ddd[ddd.length-1]);
        try {
			System.out.println("init2");
			Hbase35Init.init();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		getData("EMR_INSPECTION", "372925198904", null, null, null, null);

		System.out.println("test");
		
		List<String> li2=getData("EMR_INSPECTION", "372925198904", null, null, null, null);
		for(int i = 0;i < li2.size(); i ++){
			   System.out.println(li2.get(i));
		}
		
	}
	

	/**
	 * 根据报告单号查询 对应 检查图片存储地址列表/检查项目编号列表/影像文件存储地址列表
	 * 
	 * @param tableName
	 * @param reportId
	 * @return
	 */
	public static List<String> getInspectImage(String tableName, String reportId) {
		System.out.println("查询检查单号： " + reportId + " 对应的图像文件列表！");
		String reportIdMD5 = MD5(reportId);// 将要查询的检查单号转成MD5
		List<String> imagePaths = new ArrayList<String>();
		Scan scan = new Scan();
		scan.setCaching(100);
		scan.setStartRow(Bytes.toBytes(reportIdMD5));
		PrefixFilter filter = new PrefixFilter(Bytes.toBytes(reportIdMD5));
		QualifierFilter imagePathQFilter = null;
		switch (tableName) {
		case "EMR_INSPECTION_IMAGE":
			imagePathQFilter = new QualifierFilter(CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes("IMAGE_SAVE_PATH")));
			break;
		case "EMR_INSPECTION_ITEM":
			imagePathQFilter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("ITEM_CODE")));
			break;
		case "EMR_EXAM_IMAGE_PATH":
			imagePathQFilter = new QualifierFilter(CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes("IMAGE_SAVE_PATH")));
			break;
		}

		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filterList.addFilter(filter);
		filterList.addFilter(imagePathQFilter);
		scan.setFilter(filterList);

		try {
			HTable table = Hbase35Init.getTable(TableName.valueOf(tableName));
			ResultScanner scanner = table.getScanner(scan);

			for (Result res : scanner) {
				format(res);
				for (Cell cell : res.listCells()) {
					imagePaths.add(Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}

		} catch (Exception e) {
			System.out.println("scan table EMR_INSPECTION" + " failed...The reason is " + e);
		}
		return imagePaths;

	}

	/**
	 * 
	 * 查询EMR_INSPECTION/EMR_EXAM_IMAGE_REP，读取报告单号(列表)
	 * 
	 * @param tableName
	 *            必填，EMR_INSPECTION或EMR_EXAM_IMAGE_REP
	 * @param ID_NUM
	 *            必填
	 * @param EFFECTIVE_TIME
	 *            可以为null
	 * @param ORG_CODE
	 *            可以为null
	 * @param INSPECTION_TYPE
	 *            可以为null,查询EMR_INSPECTION表使用，查询EMR_EXAM_IMAGE_REP时必须为空
	 * @param EXAM_TYPE
	 *            可以为null,查询EMR_EXAM_IMAGE_REP表使用，查询EMR_INSPECTION时必须为空
	 * 
	 * @return 符合条件的检查列表
	 */
	public static List<String> getData(String tableName, String ID_NUM, String EFFECTIVE_TIME, String ORG_CODE,
			String INSPECTION_TYPE, String EXAM_TYPE) {

		System.out.println("查询表格： " + tableName + ", 身份证号： " + ID_NUM);
		List<String> reports = new ArrayList<String>();
		Scan scan = new Scan();
		scan.setCaching(100);
		scan.setStartRow(Bytes.toBytes(ID_NUM.substring(0, 6)));
		// 前缀过滤器，查询行键以指定身份证开头的记录
		PrefixFilter filter = new PrefixFilter(Bytes.toBytes(ID_NUM));
		// scan.setFilter(filter);

		FilterList filterQList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		// 列过滤器，过滤出列名为REPORT_NUM的列
		QualifierFilter reportQFilter = new QualifierFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("REPORT_NUM")));
		filterQList.addFilter(reportQFilter);

		FilterList filterCVList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		if (EFFECTIVE_TIME != null) {
			SingleColumnValueFilter timeCVFilter = new SingleColumnValueFilter(Bytes.toBytes("info"),
					Bytes.toBytes("EFFECTIVE_TIME"), CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(EFFECTIVE_TIME)));
			filterCVList.addFilter(timeCVFilter);
		}
		if (ORG_CODE != null) {
			SingleColumnValueFilter orgCVFilter = new SingleColumnValueFilter(Bytes.toBytes("info"),
					Bytes.toBytes("ORG_CODE"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(ORG_CODE)));
			filterCVList.addFilter(orgCVFilter);
		}
		if (INSPECTION_TYPE != null) {
			SingleColumnValueFilter typeCVFilter = new SingleColumnValueFilter(Bytes.toBytes("info"),
					Bytes.toBytes("INSPECTION_TYPE"), CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(INSPECTION_TYPE)));
			filterCVList.addFilter(typeCVFilter);
		}
		if (EXAM_TYPE != null) {
			SingleColumnValueFilter typeCVFilter = new SingleColumnValueFilter(Bytes.toBytes("info"),
					Bytes.toBytes("EXAM_TYPE"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(EXAM_TYPE)));
			filterCVList.addFilter(typeCVFilter);
		}
		filterCVList.addFilter(filterQList);
		filterCVList.addFilter(filter);
		scan.setFilter(filterCVList);
		try {
			HTable table = Hbase35Init.getTable(TableName.valueOf(tableName));
			ResultScanner scanner = table.getScanner(scan);

			for (Result res : scanner) {
				format(res);
				for (Cell cell : res.listCells()) {
					reports.add(Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}

		} catch (Exception e) {
			System.out.println("scan table EMR_INSPECTION" + " failed...The reason is " + e);
		}
		return reports;
	}
	
	
	/**
	 * 根据住院病案ID获取费用信息
	 * 
	 * @param tableName
	 * @param ICHId
	 * @return
	 */
	public static List<String> getICHCharge(String tableName, String ICHId) {
		System.out.println("查询病案ID： " + ICHId + " 对应的费用信息！");
		String reportIdMD5 = MD5(ICHId);// 将要查询的检查单号转成MD5
		List<String> imagePaths = new ArrayList<String>();
		Scan scan = new Scan();
		scan.setCaching(100);
		scan.setStartRow(Bytes.toBytes(reportIdMD5));
		PrefixFilter filter = new PrefixFilter(Bytes.toBytes(reportIdMD5));

		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filterList.addFilter(filter);
		scan.setFilter(filterList);

		try {
			HTable table = Hbase35Init.getTable(TableName.valueOf(tableName));
			ResultScanner scanner = table.getScanner(scan);

			for (Result res : scanner) {
				format(res);
				for (Cell cell : res.listCells()) {
					imagePaths.add(Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}

		} catch (Exception e) {
			System.out.println("scan table EMR_ICH_CHARGE" + " failed...The reason is " + e);
		}
		return imagePaths;

	}

	/**
	 * 
	 * 查询EMR_ICH，读取住院病案首页(列表)
	 * 
	 * @param tableName
	 *            必填，EMR_ICH
	 * @param ID_NUM
	 *            必填
	 * @param ADMISSION_DATE 入院日期
	 *            可以为null
	 * @param ORG_CODE
	 *            可以为null
	 * @return 符合条件的检查列表
	 */
	public static List<String> getICHID(String tableName, String ID_NUM, String date,
			String ORG_CODE) {

		System.out.println("查询表格： " + tableName + ", 身份证号： " + ID_NUM);
		List<String> reports = new ArrayList<String>();
		Scan scan = new Scan();
		scan.setCaching(100);
		scan.setStartRow(Bytes.toBytes(ID_NUM.substring(0, 6)));
		// 前缀过滤器，查询行键以指定身份证开头的记录
		PrefixFilter filter = new PrefixFilter(Bytes.toBytes(ID_NUM));
		// scan.setFilter(filter);

		FilterList filterQList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		// 列过滤器，过滤出列名为ID的列
		QualifierFilter ICHIDQFilter = new QualifierFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("ID")));
		filterQList.addFilter(ICHIDQFilter);

		FilterList filterCVList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		if (date != null) {
			SingleColumnValueFilter timeCVFilter = new SingleColumnValueFilter(Bytes.toBytes("info"),
					Bytes.toBytes("ADMISSION_DATE"), CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(date)));
			filterCVList.addFilter(timeCVFilter);
		}
		if (ORG_CODE != null) {
			SingleColumnValueFilter typeCVFilter = new SingleColumnValueFilter(Bytes.toBytes("info"),
					Bytes.toBytes("ORG_CODE"), CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(ORG_CODE)));
			filterCVList.addFilter(typeCVFilter);
		}
		filterCVList.addFilter(filterQList);
		filterCVList.addFilter(filter);
		scan.setFilter(filterCVList);
		try {
			HTable table = Hbase35Init.getTable(TableName.valueOf(tableName));
			ResultScanner scanner = table.getScanner(scan);

			for (Result res : scanner) {
				format(res);
				for (Cell cell : res.listCells()) {
					reports.add(Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}

		} catch (Exception e) {
			System.out.println("scan table EMR_ICH" + " failed...The reason is " + e);
		}
		return reports;
	}
	
	

	/**
	 * 小文件合并写入hdfs
	 * @param fs hdfs文件系统
	 * @param tableName 索引写入的hbase表
	 * @param path  写入hdfs路径
	 * @param srcFile	小文件源存储路径(本地)
	 * @throws IOException
	 */
	public static void FileWriter(FileSystem fs, String tableName, String path, String srcFile) throws IOException {
		Path p = new Path(path);
		
		String[] srcs = srcFile.split("/");
		
		Text key = new Text(srcs[srcs.length-1]);
		Text value = new Text();

		byte[] image = fileutil.FileReader(srcFile);

		SequenceFile.Writer writer = null;
		Option optPath = SequenceFile.Writer.file(p);
		Option optKey = SequenceFile.Writer.keyClass(key.getClass());
		Option optVal = SequenceFile.Writer.valueClass(value.getClass());
		Option optExist = SequenceFile.Writer.appendIfExists(true);
		Option optCompress = SequenceFile.Writer.compression(CompressionType.RECORD);

		try {
			writer = SequenceFile.createWriter(fs.getConf(), optPath, optKey, optVal, optExist, optCompress);

			long startPos = writer.getLength();
			value.set(image);
			writer.append(key, value);
			value.clear();
			Put put = new Put(Bytes.toBytes(srcFile));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("file"), Bytes.toBytes(path));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("start"), Bytes.toBytes(startPos));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("end"), Bytes.toBytes(writer.getLength()));

			HTable table = Hbase35Init.getTable(TableName.valueOf(tableName));
			table.put(put);
			table.close();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	/**
	 * 读取指定rowkey(小文件源存储路径)对应的索引数据(即在hdfs上的存储路径及在合并文件中的偏移信息)
	 * 
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	public static HbaseStoryEntity GetIdx(String tableName, String rowKey) throws IOException {
		HTable table = Hbase35Init.getTable(TableName.valueOf(tableName));

		Get get = new Get(Bytes.toBytes(rowKey));

		Result r = table.get(get);
		HbaseStoryEntity imageFile = new HbaseStoryEntity();
		for (Cell cell : r.listCells()) {
			if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("file")) {
				imageFile.setFileName(Bytes.toString(CellUtil.cloneValue(cell)));
			} else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("start")) {
				imageFile.setStartPos(Bytes.toLong(CellUtil.cloneValue(cell)));
			} else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("end")) {
				imageFile.setEndPos(Bytes.toLong(CellUtil.cloneValue(cell)));
			}
		}
		return imageFile;
	}

	/**
	 * 根据从hbase中读取到的小文件在hdfs中的存储信息，索引到该小文件，并存储在本地
	 * @param fs
	 * @param imageIdx
	 * @throws IOException
	 */
	public static void FileReader(FileSystem fs, HbaseStoryEntity imageIdx) throws IOException {

		SequenceFile.Reader.Option optlen = SequenceFile.Reader.length(imageIdx.getEndPos());
		SequenceFile.Reader reader = new SequenceFile.Reader(fs.getConf(),
				SequenceFile.Reader.file(new Path(imageIdx.getFileName())), optlen);
		reader.seek(imageIdx.getStartPos());
		Text key = new Text();
		Text value = new Text();
		while (reader.next(key, value)) {
			fileutil.FileWriter("/home/niu/image/"+key, value.getBytes());
			break;
		}
		IOUtils.closeStream(reader);
	}

	public static void format(Result result) {
		System.out.println("rowkey是:" + Bytes.toString(result.getRow()));
		for (Cell cell : result.listCells()) {
			System.out.println("family是:" + Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println("qualifier是:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println("value是:" + Bytes.toString(CellUtil.cloneValue(cell)) + " 长度：  "
					+ Bytes.toString(CellUtil.cloneValue(cell)).length());
		}
		System.out.println("-------------------------------------------------------------");
	}

	/**
	 * 对字符串md5加密(大写+数字)
	 * 
	 * @param str
	 *            传入要加密的字符串
	 * @return MD5加密后的字符串
	 */

	public static String MD5(String s) {
		char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

		try {
			byte[] btInput = s.getBytes();
			// 获得MD5摘要算法的 MessageDigest 对象
			MessageDigest mdInst = MessageDigest.getInstance("MD5");
			// 使用指定的字节更新摘要
			mdInst.update(btInput);
			// 获得密文
			byte[] md = mdInst.digest();
			// 把密文转换成十六进制的字符串形式
			int j = md.length;
			char str[] = new char[j * 2];
			int k = 0;
			for (int i = 0; i < j; i++) {
				byte byte0 = md[i];
				str[k++] = hexDigits[byte0 >>> 4 & 0xf];
				str[k++] = hexDigits[byte0 & 0xf];
			}
			return new String(str);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
