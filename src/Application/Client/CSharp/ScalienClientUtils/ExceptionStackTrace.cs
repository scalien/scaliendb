using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace Scalien
{
    public class ExceptionStackTrace
    {
        private string message;
        public string Message
        {
            get
            {
                return message;
            }
        }

        private string stackTrace;
        public string StackTrace
        {
            get
            {
                return stackTrace;
            }
        }

        private string source;
        public string Source
        {
            get
            {
                return source;
            }
        }

        private string fileName;
        public string FileName
        {
            get
            {
                return fileName;
            }
        }

        private int lineNumber;
        public int LineNumber
        {
            get
            {
                return lineNumber;
            }
        }

        public ExceptionStackTrace(Exception exception)
        {
            message = exception.Message;

            stackTrace = GetExceptionStackTraces(exception);

        }

        private void UpdateByLastFrame(Exception exception, StringBuilder sb)
        {
            StackTrace stackTrace = new StackTrace(exception, true);
            if (stackTrace.FrameCount > 0)
            {
                sb.Append(stackTrace.ToString());
                StackFrame frame = stackTrace.GetFrames().Last();
                source = frame.GetMethod().DeclaringType.FullName;
                fileName = frame.GetFileName();
                lineNumber = frame.GetFileLineNumber();
            }
        }

        private string GetExceptionStackTraces(Exception exception)
        {
            StringBuilder sb = new StringBuilder();

            while (true)
            {
                UpdateByLastFrame(exception, sb);

                exception = exception.InnerException;
                if (exception == null)
                    break;

                sb.Append(exception.GetType().FullName).Append(" : ").Append(exception.Message).Append('\n');
            }

            return sb.ToString();
        }

        private static Tuple<string, int> GetStackTraceFileInfo(Exception exception)
        {
            var trace = new StackTrace(exception, true);

            // Find the first frame with valid file name
            int firstFrame = Array.FindIndex(trace.GetFrames(), f => f.GetFileName() != null);

            if (firstFrame < 0)
                firstFrame = 0;

            StackFrame frame = trace.GetFrame(firstFrame);

            return new Tuple<string, int>(frame.GetFileName() ?? "", frame.GetFileLineNumber());
        }
    }
}
