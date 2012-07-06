using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.IO;
using System.Reflection;
using System.Collections.Specialized;

namespace Scalien
{
    public class ConfigFile
    {
        public static string Filename = "config.txt";
        public static ConfigFile Config
        {
            get
            {
                lock (typeof(ConfigFile))
                {
                    if (instance == null)
                        instance = new ConfigFile();
                    return instance;
                }
            }
        }
        
        private static ConfigFile instance;
        private StringDictionary conf;
 
        public ConfigFile(string filePath = null)
        {
            Open(filePath);
        }

        private void Open(string filePath)
        {
            conf = new StringDictionary();

            if (filePath == null)
                filePath = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), Filename);

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

                        string[] kvParts = line.Split(new char[] { '=' }, 2);
                        if (kvParts.Length > 1)
                        {
                            string keyPart = kvParts[0].Trim();
                            string valuePart = kvParts[1].Trim();

                            conf.Add(keyPart.ToLower(), valuePart);
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
            key = key.ToLower();
            if (conf.ContainsKey(key))
                return conf[key];
            else
                return defaultValue;
        }

        public string[] GetStringArrayValue(string key, string[] defaultValue = null)
        {
            key = key.ToLower();
            if (conf.ContainsKey(key))
            {
                string stringValue = conf[key];
                return stringValue.Split(new char[] { ',' });
            }
            else
                return defaultValue;
        }

        public int GetIntValue(string key, int defaultValue = 0)
        {
            string stringValue = GetStringValue(key, "" + defaultValue);
            return Convert.ToInt32(stringValue);
        }

        public Int64 GetInt64Value(string key, Int64 defaultValue = 0)
        {
            string stringValue = GetStringValue(key, "" + defaultValue);
            return Convert.ToInt64(stringValue);
        }

        public UInt64 GetUInt64Value(string key, UInt64 defaultValue = 0)
        {
            string stringValue = GetStringValue(key, "" + defaultValue);
            return Convert.ToUInt64(stringValue);
        }

        public bool GetBoolValue(string key, bool defaultValue)
        {
            string stringValue = GetStringValue(key);
            if (stringValue == null)
                return defaultValue;
            
            if (stringValue == "true" || stringValue == "yes" || stringValue == "on")
                return true;
            
            if (stringValue == "false" || stringValue == "no" || stringValue == "off")
                return false;

            throw new FormatException();
        }

        // Retrieve a parameter value if it exists
        public string this[string Param]
        {
            get
            {
                return (conf[Param]);
            }
        }

    }
}
