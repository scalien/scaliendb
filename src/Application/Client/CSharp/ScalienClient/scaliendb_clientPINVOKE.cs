/* ----------------------------------------------------------------------------
 * This file was automatically generated by SWIG (http://www.swig.org).
 * Version 2.0.4
 *
 * Do not make changes to this file unless you know what you are doing--modify
 * the SWIG interface file instead.
 * ----------------------------------------------------------------------------- */

namespace Scalien {

using System;
using System.Runtime.InteropServices;

class scaliendb_clientPINVOKE {

  protected class SWIGExceptionHelper {

    public delegate void ExceptionDelegate(string message);
    public delegate void ExceptionArgumentDelegate(string message, string paramName);

    static ExceptionDelegate applicationDelegate = new ExceptionDelegate(SetPendingApplicationException);
    static ExceptionDelegate arithmeticDelegate = new ExceptionDelegate(SetPendingArithmeticException);
    static ExceptionDelegate divideByZeroDelegate = new ExceptionDelegate(SetPendingDivideByZeroException);
    static ExceptionDelegate indexOutOfRangeDelegate = new ExceptionDelegate(SetPendingIndexOutOfRangeException);
    static ExceptionDelegate invalidCastDelegate = new ExceptionDelegate(SetPendingInvalidCastException);
    static ExceptionDelegate invalidOperationDelegate = new ExceptionDelegate(SetPendingInvalidOperationException);
    static ExceptionDelegate ioDelegate = new ExceptionDelegate(SetPendingIOException);
    static ExceptionDelegate nullReferenceDelegate = new ExceptionDelegate(SetPendingNullReferenceException);
    static ExceptionDelegate outOfMemoryDelegate = new ExceptionDelegate(SetPendingOutOfMemoryException);
    static ExceptionDelegate overflowDelegate = new ExceptionDelegate(SetPendingOverflowException);
    static ExceptionDelegate systemDelegate = new ExceptionDelegate(SetPendingSystemException);

    static ExceptionArgumentDelegate argumentDelegate = new ExceptionArgumentDelegate(SetPendingArgumentException);
    static ExceptionArgumentDelegate argumentNullDelegate = new ExceptionArgumentDelegate(SetPendingArgumentNullException);
    static ExceptionArgumentDelegate argumentOutOfRangeDelegate = new ExceptionArgumentDelegate(SetPendingArgumentOutOfRangeException);

    [DllImport("scaliendb_client", EntryPoint="SWIGRegisterExceptionCallbacks_scaliendb_client")]
    public static extern void SWIGRegisterExceptionCallbacks_scaliendb_client(
                                ExceptionDelegate applicationDelegate,
                                ExceptionDelegate arithmeticDelegate,
                                ExceptionDelegate divideByZeroDelegate, 
                                ExceptionDelegate indexOutOfRangeDelegate, 
                                ExceptionDelegate invalidCastDelegate,
                                ExceptionDelegate invalidOperationDelegate,
                                ExceptionDelegate ioDelegate,
                                ExceptionDelegate nullReferenceDelegate,
                                ExceptionDelegate outOfMemoryDelegate, 
                                ExceptionDelegate overflowDelegate, 
                                ExceptionDelegate systemExceptionDelegate);

    [DllImport("scaliendb_client", EntryPoint="SWIGRegisterExceptionArgumentCallbacks_scaliendb_client")]
    public static extern void SWIGRegisterExceptionCallbacksArgument_scaliendb_client(
                                ExceptionArgumentDelegate argumentDelegate,
                                ExceptionArgumentDelegate argumentNullDelegate,
                                ExceptionArgumentDelegate argumentOutOfRangeDelegate);

    static void SetPendingApplicationException(string message) {
      SWIGPendingException.Set(new System.ApplicationException(message, SWIGPendingException.Retrieve()));
    }
    static void SetPendingArithmeticException(string message) {
      SWIGPendingException.Set(new System.ArithmeticException(message, SWIGPendingException.Retrieve()));
    }
    static void SetPendingDivideByZeroException(string message) {
      SWIGPendingException.Set(new System.DivideByZeroException(message, SWIGPendingException.Retrieve()));
    }
    static void SetPendingIndexOutOfRangeException(string message) {
      SWIGPendingException.Set(new System.IndexOutOfRangeException(message, SWIGPendingException.Retrieve()));
    }
    static void SetPendingInvalidCastException(string message) {
      SWIGPendingException.Set(new System.InvalidCastException(message, SWIGPendingException.Retrieve()));
    }
    static void SetPendingInvalidOperationException(string message) {
      SWIGPendingException.Set(new System.InvalidOperationException(message, SWIGPendingException.Retrieve()));
    }
    static void SetPendingIOException(string message) {
      SWIGPendingException.Set(new System.IO.IOException(message, SWIGPendingException.Retrieve()));
    }
    static void SetPendingNullReferenceException(string message) {
      SWIGPendingException.Set(new System.NullReferenceException(message, SWIGPendingException.Retrieve()));
    }
    static void SetPendingOutOfMemoryException(string message) {
      SWIGPendingException.Set(new System.OutOfMemoryException(message, SWIGPendingException.Retrieve()));
    }
    static void SetPendingOverflowException(string message) {
      SWIGPendingException.Set(new System.OverflowException(message, SWIGPendingException.Retrieve()));
    }
    static void SetPendingSystemException(string message) {
      SWIGPendingException.Set(new System.SystemException(message, SWIGPendingException.Retrieve()));
    }

    static void SetPendingArgumentException(string message, string paramName) {
      SWIGPendingException.Set(new System.ArgumentException(message, paramName, SWIGPendingException.Retrieve()));
    }
    static void SetPendingArgumentNullException(string message, string paramName) {
      Exception e = SWIGPendingException.Retrieve();
      if (e != null) message = message + " Inner Exception: " + e.Message;
      SWIGPendingException.Set(new System.ArgumentNullException(paramName, message));
    }
    static void SetPendingArgumentOutOfRangeException(string message, string paramName) {
      Exception e = SWIGPendingException.Retrieve();
      if (e != null) message = message + " Inner Exception: " + e.Message;
      SWIGPendingException.Set(new System.ArgumentOutOfRangeException(paramName, message));
    }

    static SWIGExceptionHelper() {
      SWIGRegisterExceptionCallbacks_scaliendb_client(
                                applicationDelegate,
                                arithmeticDelegate,
                                divideByZeroDelegate,
                                indexOutOfRangeDelegate,
                                invalidCastDelegate,
                                invalidOperationDelegate,
                                ioDelegate,
                                nullReferenceDelegate,
                                outOfMemoryDelegate,
                                overflowDelegate,
                                systemDelegate);

      SWIGRegisterExceptionCallbacksArgument_scaliendb_client(
                                argumentDelegate,
                                argumentNullDelegate,
                                argumentOutOfRangeDelegate);
    }
  }

  protected static SWIGExceptionHelper swigExceptionHelper = new SWIGExceptionHelper();

  public class SWIGPendingException {
    [ThreadStatic]
    private static Exception pendingException = null;
    private static int numExceptionsPending = 0;

    public static bool Pending {
      get {
        bool pending = false;
        if (numExceptionsPending > 0)
          if (pendingException != null)
            pending = true;
        return pending;
      } 
    }

    public static void Set(Exception e) {
      if (pendingException != null)
        throw new ApplicationException("FATAL: An earlier pending exception from unmanaged code was missed and thus not thrown (" + pendingException.ToString() + ")", e);
      pendingException = e;
      lock(typeof(scaliendb_clientPINVOKE)) {
        numExceptionsPending++;
      }
    }

    public static Exception Retrieve() {
      Exception e = null;
      if (numExceptionsPending > 0) {
        if (pendingException != null) {
          e = pendingException;
          pendingException = null;
          lock(typeof(scaliendb_clientPINVOKE)) {
            numExceptionsPending--;
          }
        }
      }
      return e;
    }
  }


  protected class SWIGStringHelper {

    public delegate string SWIGStringDelegate(string message);
    static SWIGStringDelegate stringDelegate = new SWIGStringDelegate(CreateString);

    [DllImport("scaliendb_client", EntryPoint="SWIGRegisterStringCallback_scaliendb_client")]
    public static extern void SWIGRegisterStringCallback_scaliendb_client(SWIGStringDelegate stringDelegate);

    static string CreateString(string cString) {
      return cString;
    }

    static SWIGStringHelper() {
      SWIGRegisterStringCallback_scaliendb_client(stringDelegate);
    }
  }

  static protected SWIGStringHelper swigStringHelper = new SWIGStringHelper();


  static scaliendb_clientPINVOKE() {
  }


  protected class SWIGWStringHelper {

    public delegate string SWIGWStringDelegate(IntPtr message);
    static SWIGWStringDelegate wstringDelegate = new SWIGWStringDelegate(CreateWString);

    [DllImport("scaliendb_client", EntryPoint="SWIGRegisterWStringCallback_scaliendb_client")]
    public static extern void SWIGRegisterWStringCallback_scaliendb_client(SWIGWStringDelegate wstringDelegate);

    static string CreateWString([MarshalAs(UnmanagedType.LPWStr)]IntPtr cString) {
      return System.Runtime.InteropServices.Marshal.PtrToStringUni(cString);
    }

    static SWIGWStringHelper() {
      SWIGRegisterWStringCallback_scaliendb_client(wstringDelegate);
    }
  }

  static protected SWIGWStringHelper swigWStringHelper = new SWIGWStringHelper();


  [DllImport("scaliendb_client", EntryPoint="CSharp_imaxdiv_t_quot_set")]
  public static extern void imaxdiv_t_quot_set(HandleRef jarg1, long jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_imaxdiv_t_quot_get")]
  public static extern long imaxdiv_t_quot_get(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_imaxdiv_t_rem_set")]
  public static extern void imaxdiv_t_rem_set(HandleRef jarg1, long jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_imaxdiv_t_rem_get")]
  public static extern long imaxdiv_t_rem_get(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_new_imaxdiv_t")]
  public static extern IntPtr new_imaxdiv_t();

  [DllImport("scaliendb_client", EntryPoint="CSharp_delete_imaxdiv_t")]
  public static extern void delete_imaxdiv_t(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_imaxabs")]
  public static extern long imaxabs(long jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_imaxdiv")]
  public static extern IntPtr imaxdiv(long jarg1, long jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_strtoimax")]
  public static extern long strtoimax(string jarg1, HandleRef jarg2, int jarg3);

  [DllImport("scaliendb_client", EntryPoint="CSharp_strtoumax")]
  public static extern ulong strtoumax(string jarg1, HandleRef jarg2, int jarg3);

  [DllImport("scaliendb_client", EntryPoint="CSharp_new_SDBP_NodeParams")]
  public static extern IntPtr new_SDBP_NodeParams(int jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_delete_SDBP_NodeParams")]
  public static extern void delete_SDBP_NodeParams(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_NodeParams_Close")]
  public static extern void SDBP_NodeParams_Close(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_NodeParams_AddNode")]
  public static extern void SDBP_NodeParams_AddNode(HandleRef jarg1, string jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_NodeParams_nodec_set")]
  public static extern void SDBP_NodeParams_nodec_set(HandleRef jarg1, int jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_NodeParams_nodec_get")]
  public static extern int SDBP_NodeParams_nodec_get(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_NodeParams_nodes_set")]
  public static extern void SDBP_NodeParams_nodes_set(HandleRef jarg1, HandleRef jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_NodeParams_nodes_get")]
  public static extern IntPtr SDBP_NodeParams_nodes_get(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_NodeParams_num_set")]
  public static extern void SDBP_NodeParams_num_set(HandleRef jarg1, int jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_NodeParams_num_get")]
  public static extern int SDBP_NodeParams_num_get(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_new_SDBP_Buffer")]
  public static extern IntPtr new_SDBP_Buffer();

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Buffer_SetBuffer")]
  public static extern void SDBP_Buffer_SetBuffer(HandleRef jarg1, IntPtr jarg2, int jarg3);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Buffer_data_set")]
  public static extern void SDBP_Buffer_data_set(HandleRef jarg1, IntPtr jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Buffer_data_get")]
  public static extern IntPtr SDBP_Buffer_data_get(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Buffer_len_set")]
  public static extern void SDBP_Buffer_len_set(HandleRef jarg1, int jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Buffer_len_get")]
  public static extern int SDBP_Buffer_len_get(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_delete_SDBP_Buffer")]
  public static extern void delete_SDBP_Buffer(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultClose")]
  public static extern void SDBP_ResultClose(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultKey")]
  public static extern string SDBP_ResultKey(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultValue")]
  public static extern string SDBP_ResultValue(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultKeyBuffer")]
  public static extern IntPtr SDBP_ResultKeyBuffer(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultValueBuffer")]
  public static extern IntPtr SDBP_ResultValueBuffer(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultSignedNumber")]
  public static extern long SDBP_ResultSignedNumber(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultNumber")]
  public static extern ulong SDBP_ResultNumber(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultIsConditionalSuccess")]
  public static extern bool SDBP_ResultIsConditionalSuccess(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultDatabaseID")]
  public static extern ulong SDBP_ResultDatabaseID(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultTableID")]
  public static extern ulong SDBP_ResultTableID(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultBegin")]
  public static extern void SDBP_ResultBegin(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultNext")]
  public static extern void SDBP_ResultNext(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultIsEnd")]
  public static extern bool SDBP_ResultIsEnd(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultIsFinished")]
  public static extern bool SDBP_ResultIsFinished(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultTransportStatus")]
  public static extern int SDBP_ResultTransportStatus(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultCommandStatus")]
  public static extern int SDBP_ResultCommandStatus(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultNumNodes")]
  public static extern uint SDBP_ResultNumNodes(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultNodeID")]
  public static extern ulong SDBP_ResultNodeID(HandleRef jarg1, uint jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ResultElapsedTime")]
  public static extern uint SDBP_ResultElapsedTime(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Create")]
  public static extern IntPtr SDBP_Create();

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Init")]
  public static extern int SDBP_Init(HandleRef jarg1, HandleRef jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Destroy")]
  public static extern void SDBP_Destroy(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetResult")]
  public static extern IntPtr SDBP_GetResult(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetGlobalTimeout")]
  public static extern void SDBP_SetGlobalTimeout(HandleRef jarg1, ulong jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetMasterTimeout")]
  public static extern void SDBP_SetMasterTimeout(HandleRef jarg1, ulong jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetGlobalTimeout")]
  public static extern ulong SDBP_GetGlobalTimeout(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetMasterTimeout")]
  public static extern ulong SDBP_GetMasterTimeout(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetJSONConfigState")]
  public static extern string SDBP_GetJSONConfigState(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_WaitConfigState")]
  public static extern void SDBP_WaitConfigState(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetConsistencyMode")]
  public static extern void SDBP_SetConsistencyMode(HandleRef jarg1, int jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetBatchMode")]
  public static extern void SDBP_SetBatchMode(HandleRef jarg1, int jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetBatchLimit")]
  public static extern void SDBP_SetBatchLimit(HandleRef jarg1, uint jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_CreateDatabase")]
  public static extern int SDBP_CreateDatabase(HandleRef jarg1, string jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_RenameDatabase")]
  public static extern int SDBP_RenameDatabase(HandleRef jarg1, ulong jarg2, string jarg3);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_DeleteDatabase")]
  public static extern int SDBP_DeleteDatabase(HandleRef jarg1, ulong jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_CreateTable")]
  public static extern int SDBP_CreateTable(HandleRef jarg1, ulong jarg2, ulong jarg3, string jarg4);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_RenameTable")]
  public static extern int SDBP_RenameTable(HandleRef jarg1, ulong jarg2, string jarg3);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_DeleteTable")]
  public static extern int SDBP_DeleteTable(HandleRef jarg1, ulong jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_TruncateTable")]
  public static extern int SDBP_TruncateTable(HandleRef jarg1, ulong jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetNumQuorums")]
  public static extern uint SDBP_GetNumQuorums(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetQuorumIDAt")]
  public static extern ulong SDBP_GetQuorumIDAt(HandleRef jarg1, uint jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetQuorumNameAt")]
  public static extern string SDBP_GetQuorumNameAt(HandleRef jarg1, uint jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetQuorumIDByName")]
  public static extern ulong SDBP_GetQuorumIDByName(HandleRef jarg1, string jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetNumDatabases")]
  public static extern uint SDBP_GetNumDatabases(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetDatabaseIDAt")]
  public static extern ulong SDBP_GetDatabaseIDAt(HandleRef jarg1, uint jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetDatabaseNameAt")]
  public static extern string SDBP_GetDatabaseNameAt(HandleRef jarg1, uint jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetDatabaseIDByName")]
  public static extern ulong SDBP_GetDatabaseIDByName(HandleRef jarg1, string jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetNumTables")]
  public static extern uint SDBP_GetNumTables(HandleRef jarg1, ulong jarg2);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetTableIDAt")]
  public static extern ulong SDBP_GetTableIDAt(HandleRef jarg1, ulong jarg2, uint jarg3);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetTableNameAt")]
  public static extern string SDBP_GetTableNameAt(HandleRef jarg1, ulong jarg2, uint jarg3);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetTableIDByName")]
  public static extern ulong SDBP_GetTableIDByName(HandleRef jarg1, ulong jarg2, string jarg3);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Get")]
  public static extern int SDBP_Get(HandleRef jarg1, ulong jarg2, string jarg3);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetCStr")]
  public static extern int SDBP_GetCStr(HandleRef jarg1, ulong jarg2, IntPtr jarg3, int jarg4);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Set")]
  public static extern int SDBP_Set(HandleRef jarg1, ulong jarg2, string jarg3, string jarg4);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetCStr")]
  public static extern int SDBP_SetCStr(HandleRef jarg1, ulong jarg2, IntPtr jarg3, int jarg4, IntPtr jarg5, int jarg6);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Add")]
  public static extern int SDBP_Add(HandleRef jarg1, ulong jarg2, string jarg3, long jarg4);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_AddCStr")]
  public static extern int SDBP_AddCStr(HandleRef jarg1, ulong jarg2, IntPtr jarg3, int jarg4, long jarg5);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Delete")]
  public static extern int SDBP_Delete(HandleRef jarg1, ulong jarg2, string jarg3);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_DeleteCStr")]
  public static extern int SDBP_DeleteCStr(HandleRef jarg1, ulong jarg2, IntPtr jarg3, int jarg4);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SequenceSet")]
  public static extern int SDBP_SequenceSet(HandleRef jarg1, ulong jarg2, string jarg3, ulong jarg4);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SequenceSetCStr")]
  public static extern int SDBP_SequenceSetCStr(HandleRef jarg1, ulong jarg2, IntPtr jarg3, int jarg4, ulong jarg5);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SequenceNext")]
  public static extern int SDBP_SequenceNext(HandleRef jarg1, ulong jarg2, string jarg3);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SequenceNextCStr")]
  public static extern int SDBP_SequenceNextCStr(HandleRef jarg1, ulong jarg2, IntPtr jarg3, int jarg4);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ListKeys")]
  public static extern int SDBP_ListKeys(HandleRef jarg1, ulong jarg2, string jarg3, string jarg4, string jarg5, uint jarg6, bool jarg7, bool jarg8);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ListKeysCStr")]
  public static extern int SDBP_ListKeysCStr(HandleRef jarg1, ulong jarg2, IntPtr jarg3, int jarg4, IntPtr jarg5, int jarg6, IntPtr jarg7, int jarg8, uint jarg9, bool jarg10, bool jarg11);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ListKeyValues")]
  public static extern int SDBP_ListKeyValues(HandleRef jarg1, ulong jarg2, string jarg3, string jarg4, string jarg5, uint jarg6, bool jarg7, bool jarg8);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_ListKeyValuesCStr")]
  public static extern int SDBP_ListKeyValuesCStr(HandleRef jarg1, ulong jarg2, IntPtr jarg3, int jarg4, IntPtr jarg5, int jarg6, IntPtr jarg7, int jarg8, uint jarg9, bool jarg10, bool jarg11);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Count")]
  public static extern int SDBP_Count(HandleRef jarg1, ulong jarg2, string jarg3, string jarg4, string jarg5, bool jarg6);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_CountCStr")]
  public static extern int SDBP_CountCStr(HandleRef jarg1, ulong jarg2, IntPtr jarg3, int jarg4, IntPtr jarg5, int jarg6, IntPtr jarg7, int jarg8, bool jarg9);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Begin")]
  public static extern int SDBP_Begin(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Submit")]
  public static extern int SDBP_Submit(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_Cancel")]
  public static extern int SDBP_Cancel(HandleRef jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetTrace")]
  public static extern void SDBP_SetTrace(bool jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetDebug")]
  public static extern void SDBP_SetDebug(bool jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetLogFile")]
  public static extern void SDBP_SetLogFile(string jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetTraceBufferSize")]
  public static extern void SDBP_SetTraceBufferSize(uint jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_LogTrace")]
  public static extern void SDBP_LogTrace(string jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetCrashReporter")]
  public static extern void SDBP_SetCrashReporter(bool jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetShardPoolSize")]
  public static extern void SDBP_SetShardPoolSize(uint jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_SetMaxConnections")]
  public static extern void SDBP_SetMaxConnections(uint jarg1);

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetVersion")]
  public static extern string SDBP_GetVersion();

  [DllImport("scaliendb_client", EntryPoint="CSharp_SDBP_GetDebugString")]
  public static extern string SDBP_GetDebugString();
}

}
