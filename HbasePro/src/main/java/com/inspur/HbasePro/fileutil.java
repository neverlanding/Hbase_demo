package com.inspur.HbasePro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
/**
 * 本地文件读写
 * @author niubingru
 * 2017年7月17日
 */
public class fileutil {

	/**
	 * 读取制定路径的文件
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static byte[] FileReader(String path) throws IOException {

		byte[] buffer = null;

		File image = new File(path);
		FileInputStream fis = new FileInputStream(image);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] b = new byte[1024];
		int n;
		while ((n = fis.read(b)) != -1) {
			bos.write(b, 0, n);
		}
		fis.close();
		bos.close();
		buffer = bos.toByteArray();

		return buffer;
	}

	/**
	 * 将制定内容写入制定路径的文件
	 * @param path
	 * @param buffer
	 * @throws IOException
	 */
	public static void FileWriter(String path, byte[] buffer) throws IOException {

		File file = new File(path);
		@SuppressWarnings("resource")
		FileOutputStream fos = new FileOutputStream(file);
		fos.write(buffer);
	}

//	public static void main(String[] args) throws IOException{
////		byte[] dd = FileReader("d://txh.zip");
////		System.out.println(dd.length);
////		FileWriter("d://txh1.zip", dd);
//		System.out.println("22");
//	}
}
