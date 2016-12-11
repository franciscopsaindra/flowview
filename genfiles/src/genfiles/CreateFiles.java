package genfiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;


/**
 * Generates CDR files simulating the random behavior of a specified number of clients.
 * 
 * Creates one file per second
 * 
 * Configuration in conf/createfiles.properties
 * 
 * Specification of bras, vlan or dlsam without traffic in conf/state.properties. The file
 * is read before generating each file, and may be changed during execution
 * 
 * Accepts as an argument the number of seconds to run
 * 	TELEPHONE, LOGIN, NAS_PORT, NAS_IP_ADDRESS, LAST_DOWNLOADED_BYTES, LAST_UPLOADED_BYTES, LAST_DURATION_SECONDS, USER_IP_ADDRESS, LASTSERVER, TERMINATION_CAUSE

 * 
 * Fields in the generated CDR
 * <idx> is the index of the client, from 1 to <number-of-clients>
 * 
 * 	LAST_UPDATED: yyyy-mm-dd hh:mm:ss
 *  START_TIME: yyyy-mm-dd hh:mm:ss
 *  SESSION_ID: session-id-<number>, starting from 1 and being increased in each session
 *  STATE: "A" or "C"
 *  TELEPHONE: Number from 1 to <number-of-clients>
 *  LOGIN: <idx>@speedy
 *  NAS_PORT: Calculated based on the number of clients, dslams and bras
 *  NAS_IP_ADDRESS: 10.0.0.<idx % nBRAS>
 *  LAST_DOWNLOADED_BYTES: random number from 0 to MAX_BYTES_DOWN
 *  LAST_UPLOADED_BYTES: random number from 0 to MAX_BYTES_UP
 *  LAST_DURATION: Session time. Will be random
 *  IP_ADDRESS: 172.*.*.*
 *  LAST_SERVER: server<idx%numRadius>
 *  TERMINATION_CAUSE: User-Request
 * 
 * @author francisco
 *
 */
public class CreateFiles {
	
	private static String FILE_PREFIX = "stream_";
	private static long MAX_BYTES_UP = 100000;
	private static long MAX_BYTES_DOWN = 1000000;
	
	private static ArrayList<Integer> brokenVLANList=new ArrayList<Integer>();
	private static ArrayList<Integer> brokenBRASList=new ArrayList<Integer>();
	private static ArrayList<String> brokenDSLAMList=new ArrayList<String>();
	
	private static class ClientState {
		public long sessionId;
		public long startTime;
		public ClientState(long s, long t){sessionId=s; startTime=t;}
	}
	
	private static long lastSessionId = 0;
	
	public static void emptyInputDirectory(String outputDir){
		File inputDir = new File(outputDir);
		File[] filesToDelete = inputDir.listFiles(new FilenameFilter() {
   
            public boolean accept(File dir, String name) {
            	if(name.startsWith(FILE_PREFIX)) return true; else return false;
            }
		});
		for(File file: filesToDelete) file.delete();
	}

	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException, FileNotFoundException {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		
		Properties confProperties = new Properties();
		confProperties.load(new FileInputStream(new File(CreateFiles.class.getResource("/conf/createfiles.properties").toURI())));
		String outputDir = confProperties.getProperty("outputDirectory", "/tmp/streaminput")+"/";
		int numClients = Integer.parseInt(confProperties.getProperty("numClients", "1000"));
		int cdrPerFile = Integer.parseInt(confProperties.getProperty("cdrPerFile", "100"));
		int numSeconds = Integer.parseInt(confProperties.getProperty("numSeconds", "100"));
		int numBRAS = Integer.parseInt(confProperties.getProperty("numBRAS", "10"));
		int numDSLAM = Integer.parseInt(confProperties.getProperty("numDSLAM", "100"));
		int numRadius = Integer.parseInt(confProperties.getProperty("numRadius", "6"));
		
		// Check bounds
		if(numClients > 4096*4096) throw new IllegalArgumentException("Too many clients "+numClients);
		if(numBRAS > 255) throw new IllegalArgumentException("Too many BRAS "+numBRAS);
		
		int brasFactor = 256 / numBRAS; 
		int dslamFactor = (numDSLAM*4096)/(numClients*numBRAS);
		
		if(args.length == 1){
			numSeconds = Integer.parseInt(args[0]);
			System.out.printf("Overriding number of seconds to %s", args[0]);
		}
		
		String tmpDir = System.getProperty("java.io.tmpdir")+"/";
		if (!new File(outputDir).exists()){
			System.out.println(outputDir+" does not exist. Exiting");
			return;
		}

		System.out.printf("Creating files for %d clients and %d CDR per file for %d seconds\n", numClients, cdrPerFile, numSeconds);
		System.out.printf("Using %d DSLAM and %d BRAS \n", numDSLAM, numBRAS);
		
		// Empty input directory
		emptyInputDirectory(outputDir);
		
		// Initialize client states
		ArrayList<ClientState> clientStates = new ArrayList<ClientState>();
		for(int i = 0; i < numClients; i++) clientStates.add(i, new ClientState(0, 0));
		System.out.printf("Client states created\n");
		
		//LAST_UPDATE, START_TIME, SESSION_ID, STATE, TELEPHONE, LOGIN, NAS_PORT, NAS_IP_ADDRESS, LAST_DOWNLOADED_BYTES, LAST_UPLOADED_BYTES, LAST_DURATION_SECONDS, USER_IP_ADDRESS, LASTSERVER, TERMINATION_CAUSE
		
		long startTime = new Date().getTime();
		StringBuilder sb = new StringBuilder();
		int fileNumber = 0;
		processState();
		while(fileNumber < numSeconds){
			Date currentDate = new Date();
			fileNumber++;
			PrintWriter pw=new PrintWriter(new FileWriter(tmpDir+"."+FILE_PREFIX+fileNumber+".tmp"));
			for(int j=0; j < cdrPerFile; j++){
				
				// Get a random client
				int idx = (int)Math.floor((Math.random()*numClients));
				
				// Skip if VLAN is in the list of broken VLAN
				if(brokenVLANList.contains(((idx*dslamFactor) / 4096))){
					// System.out.printf("Skipping session due to VLAN in broken list %d\n",((idx*dslamFactor) / 4096));
					continue;
				}
				// Skip if BRAS is in the list of broken BRAS
				if(brokenBRASList.contains(idx%(256/brasFactor))){
					// System.out.printf("Skipping session due to BRAS in broken list %d\n",idx%(256/brasFactor));
					continue;
				}
				// Skip if VLAN is in the list of broken VLAN
				if(brokenDSLAMList.contains("10.0.0."+(idx%(256/brasFactor))+"/"+((idx*dslamFactor) / 4096))){
					// System.out.printf("Skipping session due to DSLAM in broken list %s\n",(idx%(256/brasFactor))+"/"+((idx*dslamFactor) / 4096));
					continue;
				}
				
				ClientState s = clientStates.get(idx);
				Date eventDate = new Date(currentDate.getTime() + (1000/cdrPerFile)*j);
				if(s.sessionId == 0){
					sb.setLength(0);
					sb.append(sdf.format(eventDate)).append(","); // last_updated
					sb.append(sdf.format(eventDate)).append(","); // start_time
					lastSessionId++; s.sessionId = lastSessionId;   
					s.startTime = eventDate.getTime();            
					sb.append("session-id-"+lastSessionId).append(","); // session-id
					sb.append("A").append(",");
					sb.append(idx).append(",");	// Telephone
					sb.append(idx+"@speedy").append(","); // Login
					sb.append(idx*dslamFactor).append(","); // nasport
					sb.append("10.0.0."+(idx%(256/brasFactor))).append(","); // nasip
					sb.append(0).append(",");
					sb.append(0).append(",");
					sb.append(0).append(",");
					sb.append("172."+(idx/(256*256))%256+"."+(idx/256)%256+"."+idx%256).append(",");
					sb.append("server"+(idx%numRadius)).append(",");
					sb.append("User-Request");
				} else {
					sb.setLength(0);
					sb.append(sdf.format(eventDate)).append(","); // last_updated
					sb.append(sdf.format(s.startTime)).append(","); // start_time
					sb.append("session-id-"+s.sessionId).append(",");
					sb.append("C").append(",");
					sb.append(idx).append(",");
					sb.append(idx+"@speedy").append(",");
					sb.append(idx*dslamFactor).append(","); // nasport
					sb.append("10.0.0."+(idx%(256/brasFactor))).append(","); // nasip
					sb.append((int)Math.floor(Math.random()*MAX_BYTES_DOWN)).append(",");
					sb.append((int)Math.floor(Math.random()*MAX_BYTES_UP)).append(",");
					sb.append((eventDate.getTime()-s.startTime)/1000).append(",");
					sb.append("172."+(idx/(256*256))%256+"."+(idx/256)%256+"."+idx%256).append(",");
					sb.append("server"+(idx%numRadius)).append(",");
					sb.append("User-Request");
					s.sessionId = 0;
					s.startTime = 0;
				}
				pw.println(sb.toString());
			}
			pw.close();
			new File(tmpDir+"."+FILE_PREFIX+fileNumber+".tmp").renameTo(new File(outputDir+FILE_PREFIX+fileNumber+".txt"));
			long sleepMillis = startTime + (fileNumber)*1000 - new Date().getTime();
			processState();
			System.out.printf("Created %d.txt. Sleeping for %d milliseconds\n", fileNumber, sleepMillis);
			Thread.sleep(sleepMillis);
		}
		
		Thread.sleep(6000);
		
		// Empty input directory
		//emptyInputDirectory(outputDir);
	}
	
	private static void processState() throws FileNotFoundException, URISyntaxException, IOException{
		Properties confProperties = new Properties();
		confProperties.load(new FileInputStream(new File(CreateFiles.class.getResource("/conf/state.properties").toURI())));
		
		String[] vlans = confProperties.getProperty("brokenVLAN", "").split(",");
		brokenVLANList = new ArrayList<Integer>();
		brokenBRASList = new ArrayList<Integer>();
		brokenDSLAMList = new ArrayList<String>();
		for(String vlan : vlans){
			if(vlan.trim().length() > 0) brokenVLANList.add(Integer.valueOf(vlan.trim()));
		}
		
		String[] brases = confProperties.getProperty("brokenBRAS", "").split(",");
		brokenBRASList = new ArrayList<Integer>();
		for(String bras : brases){
			if(bras.trim().length() > 0) brokenBRASList.add(Integer.valueOf(bras.trim()));
		}
		
		String[] dslams = confProperties.getProperty("brokenDSLAM", "").split(",");
		brokenDSLAMList = new ArrayList<String>();
		for(String dslam : dslams){
			if(dslam.trim().length() > 0) brokenDSLAMList.add(dslam.trim());
		}
	}
}
