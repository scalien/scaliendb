import java.io.*;

public class ConfigLoader {
	private String[] controllers;
	private String configFile;
	private String logFile;
	private boolean trace;

	ConfigLoader(String[] controllers) {
		this.controllers = controllers;
		trace = false;
		configFile = "client.conf";		
		logFile = null;
		loadFile();
	}
	
	private boolean loadFile() {
		System.out.println("Reading config file...");
		try {
			FileInputStream fstream = new FileInputStream(configFile);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;
	        while ((line = reader.readLine()) != null) {
				String[] parts = line.split("=");
	            if (parts.length > 1) {
					String namepart = parts[0].trim();
					String valuepart = parts[1].trim();
	                if ("controllers".equals(namepart)) {
	                    String[] controllerParts = parts[1].split(",");
	                    controllers = new String[controllerParts.length];
	                    int i = 0;
	                    for (String controller : controllerParts) {
	                        controllers[i] = controller.trim();
	                        i += 1;
	                    }
	                }
					else if ("log.trace".equals(namepart)) {
						trace = isBooleanTrueString(valuepart);
					}
					else if ("log.file".equals(namepart)) {
						logFile = valuepart;
					}
	            }
	        }
			in.close();
		} catch (Exception e) {
			System.out.println(e.toString());
			return false;
		}
		return true;
	}
	
	private boolean isBooleanTrueString(String s) {
		if ("true".equals(s) || "on".equals(s) || "yes".equals(s))
			return true;
		return false;
	}
	
	public String[] getControllers() {
		return controllers;
	}
	
	public boolean isTrace() {
		return trace;
	}
	
	public String getLogFile() {
	    return logFile;
	}
}