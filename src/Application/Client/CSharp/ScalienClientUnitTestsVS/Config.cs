using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.IO;
using System.Reflection;

using Scalien;

namespace ScalienClientUnitTesting
{
    class Config
    {
        public string[] _default_nodes = { "localhost:7080" };

        private Dictionary<string, string[]> conf;

        private static Config instance;

        public Config(string FilePath = "c:/Users/Public/")
        {
            string filePath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            try
            {
                byte[] data = Utils.ReadFile(filePath + "\\UnitTestConfig.txt");
                if (data == null)
                    conf = null;
                else
                    conf = Utils.JsonDeserialize<Dictionary<string, string[]>>(data);
            }
            catch (Exception)
            {
                conf = null;
            }
        }

        private string[] _GetNodes(string section)
        {
            if ((conf != null) && conf.ContainsKey(section))
                return conf[section];
            else
                return _default_nodes;
        }

        public static string[] GetNodes(string section = "default")
        {
            if (instance == null) instance = new Config();
            return instance._GetNodes(section);
        }

        public static void CreateSample()
        {
            Dictionary<string, string[]> sample_conf = new Dictionary<string, string[]>();
            string[] sample_nodes = { "192.168.137.103:37080", "192.168.137.51:37080", "192.168.137.52:37080" };

            sample_conf.Add("default", sample_nodes);

            string filePath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location); ;

            Utils.WriteFile(filePath + "\\UnitTestConfig.sample.txt", Utils.JsonSerialize(sample_conf));
        }
    }
}
