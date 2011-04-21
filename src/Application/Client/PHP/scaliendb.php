<?php

include_once("scaliendb_client.php");

define(SDBP_SUCCESS, 0);
define(SDBP_API_ERROR, -1);

define(SDBP_PARTIAL, -101);
define(SDBP_FAILURE, -102);

define(SDBP_NOMASTER, -201);
define(SDBP_NOCONNECTION, -202);
define(SDBP_NOPRIMARY, -203);

define(SDBP_MASTER_TIMEOUT, -301);
define(SDBP_GLOBAL_TIMEOUT, -302);
define(SDBP_PRIMARY_TIMEOUT, -303);

define(SDBP_NOSERVICE, -401);
define(SDBP_FAILED, -402);
define(SDBP_BADSCHEMA, -403);

define(SDBP_CONSISTENCY_ANY, 0);
define(SDBP_CONSISTENCY_RYW, 1);
define(SDBP_CONSISTENCY_STRICT, 2);

class Result {
    private $cptr;
    
    public function __contstruct($cptr) {
        $this->cptr = $cptr;
    }
    
    public function __destruct() {
        $this->close();
    }
    
    public function close() {
        scaliendb_client::SDBP_ResultClose($this->cptr);
        $this->cptr = NULL;
    }
    
    public function getKey() {
        return scaliendb_client::SDBP_ResultKey($this->cptr);
    }
    
    public function getValue() {
        return scaliendb_client::SDBP_ResultValue($this->cptr);
    }
    
    public function getNumber() {
        return scaliendb_client::SDBP_ResultNumber($this->cptr);
    }
    
    public function getDatabaseID() {
        return scaliendb_client::SDBP_ResultDatabaseID($this->cptr);
    }

    public function getTableID() {
        return scaliendb_client::SDBP_ResultTableID($this->cptr);
    }

    public function getNumNodes() {
        return scaliendb_client::SDBP_ResultNumNodes($this->cptr);
    }
    
    public function getNodeID($node) {
        return scaliendb_client::SDBP_ResultNodeID($this->cptr, $node);
    }
    
    public function getElapsedTime() {
        return scaliendb_client::SDBP_ResultElapsedTime($this->cptr);
    }
    
    public function begin() {
        scaliendb_client::SDBP_ResultBegin($this->cptr);
    }
    
    public function isEnd() {
        return scaliendb_client::SDBP_ResultIsEnd($this->cptr);
    }
    
    public function next() {
        scaliendb_client::SDBP_ResultNext($this->cptr);
    }
    
    public function getCommandStatus() {
        return scaliendb_client::SDBP_ResultCommandStatus($this->cptr);
    }
    
    public function getTransportStatus() {
        return scaliendb_client::SDBP_ResultTransportStatus($this->cptr);
    }
    
    public function getConnectivityStatus() {
        return scaliendb_client::SDBP_ResultConnectivityStatus($this->cptr);
    }
    
    public function getTimeoutStatus() {
        return scaliendb_client::SDBP_ResultTimeoutStatus($this->cptr);
    }
    
    public function getKeys() {
        $keys = array();
        for ($this->begin(); !$this->isEnd(); $this->next()) {
            $keys[] = $this->getKey();
        }
        return $keys;
    }
    
    public function getKeyValues() {
        $kv = array();
        for ($this->begin(); !$this->isEnd(); $this->next()) {
            $kv[$this->getKey()] = $this->getValue();
        }        
        return $kv;
    }
}

class ScalienClient {
    private $cptr = NULL;
    private $result = NULL;
    
    public function __construct($nodes) {
        $this->cptr = scaliendb_client::SDBP_Create();
        $nodeParams = new SDBP_NodeParams(count($nodes));
        foreach ($nodes as $node) {
            $nodeParams->AddNode($node);
        }
        scaliendb_client::SDBP_Init($this->cptr, $nodeParams);
        $nodeParams->Close();
    }
    
    public function __destruct() {
        if ($this->result != NULL)
            $this->result->close();
        scaliendb_client::SDBP_Destroy($this->cptr);
    }
    
    public function statusToClient($status) {
        switch ($status) {
        case SDBP_SUCCESS:
            return "SDBP_SUCCESS";
        case SDBP_API_ERROR:
            return "SDBP_API_ERROR";

        case SDBP_PARTIAL:
            return "SDBP_PARTIAL";
        case SDBP_FAILURE:
            return "SDBP_FAILURE";

        case SDBP_NOMASTER:
            return "SDBP_NOMASTER";
        case SDBP_NOCONNECTION:
            return "SDBP_NOCONNECTION";
        case SDBP_NOPRIMARY:
            return "SDBP_NOPRIMARY";

        case SDBP_MASTER_TIMEOUT:
            return "SDBP_MASTER_TIMEOUT";
        case SDBP_GLOBAL_TIMEOUT:
            return "SDBP_GLOBAL_TIMEOUT";
        case SDBP_PRIMARY_TIMEOUT:
            return "SDBP_PRIMARY_TIMEOUT";

        case SDBP_NOSERVICE:
            return "SDBP_NOSERVICE";
        case SDBP_FAILED:
            return "SDBP_FAILED";
        case SDBP_BADSCHEMA:
            return "SDBP_BADSCHEMA";

        default:
            return "<UNKNOWN>";
        }
    }
    
    public function setGlobalTimeout($timeout) {
        scaliendb_client::SDBP_SetGlobalTimeout($this->cptr, $timeout);
    }

    public function getGlobalTimeout() {
        return scaliendb_client::SDBP_GetGlobalTimeout($this->cptr);
    }

    public function setMasterTimeout($timeout) {
        scaliendb_client::SDBP_SetMasterTimeout($this->cptr, $timeout);
    }

    public function getMasterTimeout() {
        return scaliendb_client::SDBP_GetMasterTimeout($this->cptr);
    }
    
    public function setBatchLimit($limit) {
        scaliendb_client::SDBP_SetBatchLimit($this->cptr, $limit);
    }

    public function setBulkLoading($isBulk) {
        scaliendb_client::SDBP_SetBulkLoading($this->cptr, $isBulk);
    }
    
    public function setConsistencyLevel($consistencyLevel) {
        scaliendb_client::SDBP_SetConsistencyLevel($this->cptr, $consistencyLevel);
    }
    
    public function getJSONConfigState() {
        scaliendb_client::SDBP_GetJSONConfigState($this->cptr);
    }

    public function createQuorum($nodes) {
        $nodeParams = new SDBP_NodeParams(count($nodes));
        foreach ($nodes as $node) {
            $nodeParams->AddNode($node);
        }
        $status = scaliendb_client::SDBP_CreateQuorum($this->cptr, $nodeParams);
        $nodeParams->Close();
        $this->result = new Result(scaliendb_client::SDBP_GetResult($this->cptr));
        $this->_checkStatus($status);
        return $this->result->getNumber();
    }
    
    // TODO: deleteQuorum, activateNode
    
    public function createDatabase($name) {
        $status = scaliendb_client::SDBP_CreateDatabase($this->cptr, $name);
        $this->result = new Result(scaliendb_client::SDBP_GetResult($this->cptr));
        $this->_checkStatus($status);
        return $this->result->getNumber();
    }
    
    // TODO: renameDatabase, deleteDatabase
    
    public function createTable($databaseID, $quorumID, $name) {
        $status = scaliendb_client::SDBP_CreateTable($this->cptr, $databaseID, $quorumID, $name);
        $this->result = new Result(scaliendb_client::SDBP_GetResult($this->cptr));
        $this->_checkStatus($status);
        return $this->result->getNumber();        
    }
    
    // TODO: renameTable, deleteTable, truncateTable
    
    public function getDatabaseID($name) {
        $databaseID = scaliendb_client::SDBP_GetDatabaseID($this->cptr, $name);
        if ($databaseID == 0)
        {
            // TODO: set error
            return NULL;
        }
        return $databaseID;
    }

    public function getTableID($databaseID, $name) {
        $tableID = scaliendb_client::SDBP_GetTableID($this->cptr, $databaseID, $name);
        if ($tableID == 0)
        {
            // TODO: set error
            return NULL;
        }
        return $tableID;
    }
    
    public function useDatabase($name) {
        $status = scaliendb_client::SDBP_UseDatabase($this->cptr, $name);
        if ($status != SDBP_SUCCESS)
        {
            // TODO: set error
            return NULL;
        }
    }

    public function useTable($name) {
        $status = scaliendb_client::SDBP_UseTable($this->cptr, $name);
        if ($status != SDBP_SUCCESS)
        {
            // TODO: set error
            return NULL;
        }
    }
    
    public function get($key) {
        $status = $this->_dataCommand("SDBP_Get", $key);
        if ($status != SDBP_SUCCESS)
            return NULL;
        return $this->result->getValue();
    }
    
    public function set($key, $value) {
        return $this->_dataCommand("SDBP_Set", $key, $value);
    }
    
    private function _dataCommand() {
        $numargs = func_num_args();
        if ($numargs == 0)
            return SDBP_API_ERROR;
        $args = func_get_args();
        $func = $args[0];
        $funcargs = array();
        $funcargs[] = $this->cptr;
        for ($i = 1; $i < $numargs; $i++) {
            $funcargs[] = $args[$i];
        }
        $status = call_user_func_array(array("scaliendb_client", $func), $funcargs);
        return $status;
    }
}
