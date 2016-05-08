/* 
 * Copyright 2016 Oliver Zihler 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.tomp2p.mapreduce.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Some useful file methods
 * 
 * @author Oliver Zihler
 *
 */
public enum FileUtils {

	INSTANCE;

	/**
	 * Creates a new tmp folder at the specified location. Deletes an existing folder and all its content.
	 * 
	 * @param inputFilePath
	 *            where a new folder should be created
	 * @return a new {@link File} representing the new folder.
	 */
	public File createTmpFolder(String inputFilePath) {
		File folder = new File(inputFilePath + "/tmp/");
		if (folder.exists()) {
			deleteTmpFolder(folder);
		}
		folder.mkdirs();
		return folder;
	}

	/**
	 * Creates a new tmp folder at the specified location. Deletes an existing folder and all its content.
	 * 
	 * @param inputFilePath
	 *            where a new folder should be created
	 * @param tmpFolderName
	 *            the name of the folder
	 * @return a new {@link File} representing the new folder.
	 */
	public File createTmpFolder(String inputFilePath, String tmpFolderName) {
		File folder = new File(inputFilePath + "/" + tmpFolderName + "/");
		if (folder.exists()) {
			deleteTmpFolder(folder);
		}
		folder.mkdirs();
		return folder;
	}

	/**
	 * Deletes a folder and all its content
	 * 
	 * @param folder
	 *            to delete
	 */
	public void deleteTmpFolder(File folder) {
		String[] entries = folder.list();
		for (String s : entries) {
			File currentFile = new File(folder.getPath(), s);
			currentFile.delete();
		}
		folder.delete();
	}

	/**
	 * Recoursively retrieves all absolute paths (with filenames) contained in a folder specified
	 * 
	 * @param folder
	 *            to retrieve all files from
	 * @param pathVisitor
	 *            a list that will be filled with all file names
	 */
	public void getFiles(File folder, List<String> pathVisitor) {

		if (folder.isFile())
			pathVisitor.add(folder.getAbsolutePath());
		else {
			File files[] = folder.listFiles();
			if (files != null) {
				for (int i = 0; i < files.length; i++) {
					getFiles(files[i], pathVisitor);
				}
			}
		}
	}

	/**
	 * Reads all lines from a file and stores it in one String. Lines are seperated by \n
	 * 
	 * @param filePath
	 *            the file to read
	 * @param charset
	 *            of the file
	 * @return String consisting of the whole text of filePath
	 */
	public String readLines(String filePath, Charset charset) {
		String linesAsLine = "";
		ArrayList<String> lines = readLinesFromFile(filePath, charset);
		for (String line : lines) {
			linesAsLine += line + "\n";
		}
		return linesAsLine;
	}

	/**
	 * Reads all lines from a file and stores them in an {@link ArrayList} .
	 * 
	 * @param filePath
	 *            the file to read
	 * @param charset
	 *            of the file
	 * @return {@link ArrayList} consisting of all lines of filePath
	 */
	public ArrayList<String> readLinesFromFile(String filePath, Charset charset) {
		ArrayList<String> lines = new ArrayList<String>();
		String line = null;

		try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), charset)) {
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
		} catch (IOException x) {
			System.err.format("IOException:" + line + " %s%n", x);
		}
		return lines;
	}

}