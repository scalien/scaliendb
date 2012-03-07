using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using Microsoft.Win32.SafeHandles;
using System.Runtime.ConstrainedExecution;
using System.Web.Hosting;

namespace Scalien
{
    internal class NativeMethods
    {
        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern IntPtr LoadLibrary(string lpFileName);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool FreeLibrary(IntPtr hModule);

        [DllImport("kernel32", SetLastError = true, CallingConvention = CallingConvention.Winapi)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool IsWow64Process(
            [In] IntPtr hProcess,
            [Out] out bool wow64Process
        );
    }

    internal class SafeDLLHandle : SafeHandleZeroOrMinusOneIsInvalid
    {
        public string filePath;

        public SafeDLLHandle(IntPtr dllHandle, string fileName) : base(true)
        {
            SetHandle(dllHandle);
            this.filePath = fileName;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
           
            try
            {
                // Try to delete file from disk
                FileInfo file = new FileInfo(filePath);
                file.Delete();

                // Try to delete directory from disk
                Directory.Delete(Path.GetDirectoryName(filePath), true);
            }
            catch (Exception)
            {
            }
        }

        [ReliabilityContract(Consistency.WillNotCorruptState, Cer.MayFail)]
        override protected bool ReleaseHandle()
        {
            // This is a hack to free all references of the DLL
            while (NativeMethods.FreeLibrary(handle)) ;
                
            return true;
        }
    }

    public class NativeLoader
    {
        static SafeDLLHandle handle;

        static NativeLoader()
        {
            // This whole loader magic works only on Windows
            if (!IsWindows())
                return;

            bool is64BitProcess = (IntPtr.Size == 8);
            bool is64BitOperatingSystem = is64BitProcess || InternalCheckIsWow64();
            string name = "scaliendb_client";
            string bitness = is64BitProcess ? "x64" : "x86";
            ExtractNativeDLL(name, bitness);
        }

        public static void Load()
        {
        }

        public static void Cleanup()
        {
            if (handle == null)
                return;

            handle.Close();
            handle = null;
        }

        static string TryExtractDLL(string dirName, string name, string bitness)
        {
            string fileName = name + ".dll";

            try
            {
                if (!Directory.Exists(dirName))
                    Directory.CreateDirectory(dirName);
            }
            catch (Exception)
            {
                return null;
            }


            string dllPath = Path.Combine(dirName, fileName);
            string[] names = Assembly.GetExecutingAssembly().GetManifestResourceNames();
            var assemblyName = Assembly.GetExecutingAssembly().GetName().Name;
            var namespaceName = typeof(NativeLoader).Namespace;
            var resourceName = namespaceName + ".Properties.scaliendb_client." + bitness + ".dll";
            // Get the embedded resource stream that holds the Internal DLL in this assembly.
            // The name looks funny because it must be the default namespace of this project
            // (MyAssembly.) plus the name of the Properties subdirectory where the
            // embedded resource resides (Properties.) plus the name of the file.
            using (Stream stm = Assembly.GetExecutingAssembly().GetManifestResourceStream(resourceName))
            {
                // Check if the file exists and it is the correct version
                if (File.Exists(dllPath))
                {
                    FileInfo fi = new FileInfo(dllPath);
                    if (fi.Length == stm.Length)
                    {
                        // TODO: calculate checksum
                        return dllPath;
                    }
                }

                // Copy the assembly to the temporary file
                try
                {
                    using (Stream outFile = File.Create(dllPath))
                    {
                        const int sz = 1024 * 1024;
                        byte[] buf = new byte[sz];
                        while (true)
                        {
                            int nRead = stm.Read(buf, 0, sz);
                            if (nRead < 1)
                                break;
                            outFile.Write(buf, 0, nRead);
                        }
                    }
                }
                catch
                {
                    // This may happen if another process has already created and loaded the file.
                    // Since the directory includes the version number of this assembly we can
                    // assume that it's the same bits, so we just ignore the excecption here and
                    // load the DLL.
                }
            }

            return dllPath;
        }

        static bool TryLoadDLL(string dllPath)
        {
            IntPtr dllHandle = NativeMethods.LoadLibrary(dllPath);
            if (dllHandle != IntPtr.Zero)
            {
                handle = new SafeDLLHandle(dllHandle, dllPath);
                return true;
            }

            return false;
        }

        static void ExtractNativeDLL(string name, string bitness)
        {
            string location;
            // Select different default location when the process is hosted by IIS
            if (HostingEnvironment.IsHosted)
            {
                location = HostingEnvironment.MapPath("~/bin");
            }
            else
            {
                location = Assembly.GetExecutingAssembly().Location;
            }

            Debug.WriteLine("Location: {0}", location);
            FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);

            string clientPrefix = "ScalienClient.NativeDLL";
            string separator = "-";
            string fileName = string.Join(separator, new string[] {clientPrefix, bitness, fvi.FileVersion});

            // First try to load the DLL from the default directory
            if (TryLoadDLL(name))
                return;

            // Then try to load the DLL from the directory of the executing assembly
            string dirName = Path.Combine(Path.GetDirectoryName(location), fileName);
            string dllPath = TryExtractDLL(dirName, name, bitness);
            if (dllPath != null)
            {
                if (TryLoadDLL(dllPath))
                    return;
            }

            // Finally try to load the DLL from the temp directory
            string tempDirName = Path.Combine(Path.GetTempPath(), fileName);
            dllPath = TryExtractDLL(tempDirName, name, bitness);
            if (dllPath != null)
            {
                if (TryLoadDLL(dllPath))
                    return;
            }

            Debug.Assert(false, "Unable to load library " + dllPath);
        }

        public static bool IsWindows()
        {
            string env = System.Environment.OSVersion.Platform.ToString();
            if (env == "Win32NT")
                return true;
            return false;
        }

        public static bool InternalCheckIsWow64()
        {
            if ((Environment.OSVersion.Version.Major == 5 && Environment.OSVersion.Version.Minor >= 1) ||
                Environment.OSVersion.Version.Major >= 6)
            {
                using (Process p = Process.GetCurrentProcess())
                {
                    bool retVal;
                    if (!NativeMethods.IsWow64Process(p.Handle, out retVal))
                    {
                        return false;
                    }
                    return retVal;
                }
            }
            else
            {
                return false;
            }
        }
    }
}
