import java.io.*;

public class ConfigLoader {
	private String[] controllers;
	private String configFile;

	ConfigLoader(String[] controllers) {
		this.controllers = controllers;
		configFile = "client.conf";		
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
	                if ("controllers".equals(parts[0].trim())) {
	                    String[] controllerParts = parts[1].split(",");
	                    controllers = new String[controllerParts.length];
	                    int i = 0;
	                    for (String controller : controllerParts) {
	                        controllers[i] = controller.trim();
	                        i += 1;
	                    }
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
	
	public String[] getControllers() {
		return controllers;
	}
}