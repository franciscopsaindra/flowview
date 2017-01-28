package genfiles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Splits a comma separated value CDR file into a set of smaller files, partitioned
 * by date, which is assumed to be the first value on each CDR
 * 
 * @author francisco
 *
 */
public class SplitFiles {
	
	private static final String FILE_PREFIX = "playback_";

	/**
	 * args[0] name of file
	 * args[1] name of output directory
	 * args[2] seconds per file
	 * args[3] playback speed
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if(args.length < 1){
			System.out.println("Usage: SplitFiles.sh <name_of_file> [<name_of_output_dir>] [<seconds per file>] [<playback_speed>]");
			return;
		}
		
		String inputFile = args[0];
		
		Properties confProperties = new Properties();
		confProperties.load(new FileInputStream(new File(CreateFiles.class.getResource("/conf/splitfiles.properties").toURI())));
		String outputDir = confProperties.getProperty("outputDirectory", "/tmp/streaminput")+"/";
		String dateFormat = confProperties.getProperty("dateFormat", "yyyy-MM-dd HH:mm:ss");
		long secondsPerFile = Long.parseLong(confProperties.getProperty("secondsPerFile", "60"));
		long playbackSpeed = Long.parseLong(confProperties.getProperty("playbackSpeed", "10"));
		
		if(args.length > 1) outputDir = args[1] + "/";
		if(args.length > 2) secondsPerFile = Long.parseLong(args[2]);
		if(args.length > 3) Long.parseLong(args[3]);
		
		String tmpDir = System.getProperty("java.io.tmpdir")+"/";
		if (!new File(outputDir).exists()){
			System.out.println(outputDir+" does not exist. Exiting");
			return;
		}
		
		// Empty output directory
		deleteFiles(outputDir);
		
		// Create dir for tmp file
		new File(tmpDir).mkdirs();
		
		long fileNumber = 1;
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		long realStartTime = new Date().getTime();
		long fileStartTime = 0;
		long fileTargetTime = 0;
		long waitTime;
		String inputLine;
		PrintWriter pw=null;
		while(null!=(inputLine=br.readLine())){
			// Get time of current line
			String[] items = inputLine.split(",");
			long lineTime=sdf.parse(items[0]).getTime();
			if(fileStartTime == 0){
				System.out.println("Initial file time: "+items[0]);
				fileStartTime = lineTime;
			}
			
			// Initialize file and targetTime of batch if not already set
			if(pw==null){
				pw=new PrintWriter(new FileWriter(tmpDir+FILE_PREFIX+fileNumber+".tmp"));
				fileTargetTime=fileStartTime+fileNumber*secondsPerFile*1000;
				System.out.println("New file target time: "+sdf.format(new Date(fileTargetTime)));
			}
			
			if(lineTime<fileTargetTime){
				// Write line in open file
				pw.println(inputLine);
			} else {
				// Time has been exceeded. Close and move previous file
				pw.close();
				pw=null;
				new File(tmpDir+FILE_PREFIX+fileNumber+".tmp").renameTo(new File(outputDir+FILE_PREFIX+fileNumber+".txt"));
				fileNumber++;
				// Wait 
				// realTargetTime-realStartTime = fileNumber*secondsPerFile*1000/playbackSpeed
				waitTime = realStartTime+(fileNumber)*secondsPerFile*1000L/playbackSpeed-new Date().getTime();
				System.out.println("Waiting for "+waitTime+" milliseconds");
				Thread.sleep(waitTime);
			}
		}
		if(pw!=null){
			pw.close();
			new File(tmpDir+FILE_PREFIX+fileNumber+".tmp").renameTo(new File(outputDir+FILE_PREFIX+fileNumber+".txt"));
		}
		
		br.close();
		
		Thread.sleep(40000);
		deleteFiles(outputDir);
		
		System.out.println("Finished");

	}
	
	private static void deleteFiles(String dir){
		// Empty output directory
		File inputDir = new File(dir);
		File[] filesToDelete = inputDir.listFiles(new FilenameFilter() {
   
            public boolean accept(File dir, String name) {
            	if(name.startsWith(FILE_PREFIX)) return true; else return false;
            }
		});
		for(File file: filesToDelete) file.delete();
	}
}
