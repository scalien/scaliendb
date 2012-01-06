using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.IO;
using System.Reflection;

namespace Scalien
{
    public class ConfigFile
    {
        private Dictionary<string, string> conf;
 
        public ConfigFile(string filePath = null)
        {
            conf = new Dictionary<string, string>();

            if (filePath == null)
                filePath = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "config.txt");

            try
            {
                // Read the file by line.
                using (System.IO.StreamReader file = new System.IO.StreamReader(filePath))
                {
                    int counter = 0;
                    string line;
                    while ((line = file.ReadLine()) != null)
                    {
                        // Strip comments
                        if (line.Contains('#'))
                            line = line.Substring(0, line.IndexOf('#'));

                        string[] kvParts = line.Split(new char[] { '=' });
                        if (kvParts.Length > 1)
                        {
                            string keyPart = kvParts[0].Trim();
                            string valuePart = kvParts[1].Trim();

                            conf.Add(keyPart, valuePart);
                            counter++;
                        }
                    }
                }
            }
            catch (Exception)
            {
            }
        }

        public string GetStringValue(string key, string defaultValue = null)
        {
            if (conf.ContainsKey(key))
                return conf[key];
            else
                return defaultValue;
        }

        public string[] GetStringArrayValue(string key, string[] defaultValue = null)
        {
            if (conf.ContainsKey(key))
            {
                string stringValue = conf[key];
                return stringValue.Split(new char[] { ',' });
            }
            else
                return defaultValue;
        }
    }
}
