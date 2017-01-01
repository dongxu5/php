<?php
/**
 * Simple DB ORM
 * 
 * @author dongxu
 *
 */
abstract class Data_ORM_Abstract{
    protected $db_pool = "wenda";
    protected $db_obj;
    protected $table;
    protected $hash_table = false;
    protected $debug = false;
    private $_where = array();
    private $_where_data = array();
    private $_dup = array();
    private $_dup_data = array();
    private $_set = array();
    private $_set_data = array();
    private $_ord = array();
    private $_grp = array();
    private $_lmt = array();
    private $_mode = false;
    private $_f = false;
    private $_lt = false;
    use Trait_Log;
    public function __construct($hash_key = false) {
        $this->db_obj = Comm_Db::pool($this->db_pool);
        $req = new Yaf_Request_Simple();
        //如果是cli模式，数据库操作将每次进行重新连接,防止connection gone away
        if($req->isCli()){
            $this->setMode();
        }
        if ($hash_key !== false){
            $this->hashTable($hash_key);
        }
    }
    protected function hashid($id) {
        return sprintf("%02s", dechex(fmod(sprintf("%u", crc32($id)), intval($this->hash_table))));
    }
    public function hashTable($hash_key){
        $this->table = sprintf($this->table, $this->hashid($hash_key));
        return $this;
    }
    /**
     * 是否每次重连数据库
     * @param bool|string $mode
     */
    public function setMode($mode = true){
        $this->_mode = $mode;
    }
    //事务处理
    public function beginTransaction(){
        return $this->db_obj->beginTransaction();
    }
    public function commit(){
        return $this->db_obj->commit();
    }
    public function rollback(){
        return $this->db_obj->rollBack();
    }
    public function getDBInstance(){
        return $this->db_obj;
    }
    public function fuid($fuid){
        return $this->where("fuid", $fuid);
    }
    public function tuid($tuid){
        return $this->where("tuid", $tuid);
    }
    public function vuid($vuid){
        return $this->where("vuid", $vuid);
    }
    public function oid($oid){
        return $this->where("oid", $oid);
    }
    public function cid($cid){
        return $this->where("cid", $cid);
    }
    protected function db_log($sql, $msg){
        self::log("table:{$this->table} sql:{$sql}  msg:{$msg}");
    }
    /**
     * 指定查询条件
     * @param string $key
     * @param string $symbol
     * @param string $value
     * @param bool|string $prepare
     * @return $this
     */
    public function where($key, $symbol, $value = null, $prepare = true){
        if (is_callable($key)){
            $this->_where[] = $this->_f ? " and (" : "(";
            $key($this);
            $this->_where[] = ")";
            $this->_lt = true;
        }
        else{
            if ($value === null &&  $symbol !== null){
                return $this->where($key, "=", $symbol, $prepare);
            }
            if ($this->_f){
                return $this->andWhere($key, $symbol, $value, $prepare);
            }
            if ($prepare){
                $this->_where_data[] = $value;
                $value = "?";
            }
            $this->_where[] = " {$key} {$symbol} {$value} ";
            $this->_f = true;
        }
        return $this;
    }

    /**
     * 指定查询范围
     * @param string $key
     * @param array $value
     * @return $this
     */
    public function whereIn($key, array $value){
        $value = array_map(function($v){
            return "'" . addslashes($v) . "'";
        }, $value);
        $value = "(" . implode(",", $value) . ")";
        if (!empty($this->_where)){
            return $this->andWhere($key, "IN", $value, false);
        }
        $this->_where[] = " {$key} in {$value} ";
        return $this;
    }


    /**
     * @param string $key
     * @param string $symbol
     * @param string $value
     * @param bool $prepare
     * @return $this
     */
    public function andWhere($key, $symbol, $value = null, $prepare = true){
        if (is_callable($key)){
            $this->_where[] = "and (";
            $key($this);
            $this->_where[] = ")";
            $this->_lt = true;
        }
        else{
            if ($value === null &&  $symbol !== null){
                return $this->andWhere($key, "=", $symbol, $prepare);
            }
            if ($prepare){
                $this->_where_data[] = $value;
                $value = "?";
            }
            $and = " and ";
            if ($this->_lt){
                $and = "";
            }
            $this->_lt = false;
            $this->_where[] = " {$and} {$key} {$symbol} {$value} ";
        }
        return $this;
    }


    /**
     * @param string $key
     * @param string $symbol
     * @param string $value
     * @param bool $prepare
     * @return $this
     */
    public function orWhere($key, $symbol, $value = null, $prepare = true){
        if (is_callable($key)){
            $this->_where[] = " or (";
            $key($this);
            $this->_where[] = ")";
            $this->_lt = true;
        }
        else{
            if ($value === null &&  $symbol !== null){
                return $this->orWhere($key, "=", $symbol, $prepare);
            }
            if ($prepare){
                $this->_where_data[] = $value;
                $value = "?";
            }
            $or = " or ";
            if ($this->_lt){
                $or = "";
            }
            $this->_lt = false;
            $this->_where[] = " {$or} {$key} {$symbol} {$value} ";
        }
        return $this;
    }

    /**
     * 添加 mysql on Duplicate key 时候 更新的内容
     * @param string $key
     * @param bool|string $value
     * @param bool|string $prepare
     * @return $this
     */
    public function onDuplicate($key, $value = true, $prepare = true){
        if (is_array($key)){
            $prepare = $value;
            foreach ($key as $k => $v){
                $this->onDuplicate($k, $v, $prepare);
            }
        }
        if ($prepare){
            $this->_dup_data[] = $value;
            $value = "?";
        }
        $this->_dup[] = " `{$key}` = {$value} ";
        return $this;
    }

    /**
     * 指定插入的字段以及数据
     * @param string $key
     * @param bool|string $value
     * @param bool|string $prepare
     * @return $this
     */
    public function set($key, $value = true, $prepare = true){
        if (is_array($key)){
            $prepare = $value;
            foreach ($key as $k => $v){
                $this->set($k, $v, $prepare);
            }
            return $this;
        }
        if ($prepare){
            $this->_set_data[] = $value;
            $value = "?";
        }
        $this->_set[] = " `{$key}` = {$value} ";
        return $this;
    }
    /**
     * 指定字段的自增1
     * @param string $key
     * @return $this
     */
    public function incr($key, $num = 1){
        return $this->set($key, "{$key} + {$num}", false);
    }
    /**
     * 指定字段的自减1
     * @param string $key
     * @return $this
     */
    public function decr($key, $num = 1){
        return $this->set($key, "{$key} - {$num}", false);
    }
    /**
     * 指定排序
     * @param string $name
     * @param string $sort
     * @return $this
     */
    public function orderBy($name, $sort = "ASC"){
        $this->_ord[] = " {$name} {$sort} ";
        return $this;
    }
    /**
     * 指定排序 and条件
     * @param string $name
     * @param string $sort
     * @return $this
     */
    public function andOrderBy($name, $sort = "ASC"){
        return $this->orderBy($name, $sort);
    }
    /**
     * 指定分组
     * @param string $name
     * @return $this
     */
    public function groupBy($name){
        $this->_grp[] = $name;
        return $this;
    }


    /**
     * @param int $start
     * @param null|int $limit
     * @return $this
     */
    public function limit($start, $limit = null){
        if (empty($this->_lmt)){
            $this->_lmt[] = intval($start);
            if ($limit){
                $this->_lmt[] = intval($limit);
            }
        }
        
        return $this;
    }
    /**
     * 删除某条记录
     */
    public function delete(){
        try{
            if (empty($this->_where)){
                throw new Exception("sql no where");
            }
            $db_obj = $this->db_obj;
            $sql = "DELETE from `{$this->table}` WHERE " . implode(" ", $this->_where);
            $ret = $db_obj->exec($sql, $this->_where_data, $this->_mode);
            $this->clear();
            return $ret;
        }catch (Exception $e) {
            $this->db_log($sql, $e->getMessage());
            throw $e;
        }
    }
    /**
     * 更新某条记录
     */
    public function update(){
        try{
            if (empty($this->_where)){
                throw new Exception("sql no where");
            }
            $db_obj = $this->db_obj;
            $upt_str = implode(",", $this->_set);
            $where_str = implode(" ", $this->_where);
            $sql = "UPDATE  `{$this->table}` set {$upt_str} WHERE {$where_str}";
            $ret = $db_obj->exec($sql, array_merge($this->_set_data, $this->_where_data), $this->_mode);
            $this->clear();
            return $ret;
        }catch (Exception $e) {
            $this->db_log($sql, $e->getMessage());
            throw $e;
        }
    }

    /**
     * 添加一条记录
     * @param bool $ignore
     * @return bool
     * @throws Exception
     */
    public function insert($ignore = true){
        try{
            $db_obj = $this->db_obj;
            if (empty($this->_set)){
                throw new Exception("no insert data");
            }
            $upt_str = implode(",", $this->_set);
            $sql = "INSERT " . ($ignore ? "IGNORE" : "") . " INTO `{$this->table}` set {$upt_str}";
            if (!empty($this->_dup)){
                $sql .= " ON DUPLICATE KEY UPDATE " . implode(",", $this->_dup);
            }
            $ret = $db_obj->exec($sql, array_merge($this->_set_data, $this->_dup_data), $this->_mode);
            $this->clear();
            return $ret;
        }catch (Exception $e) {
            $this->db_log($sql, $e->getMessage());
            throw $e;
        }
    }
    public function lastId()
    {
        try {
            return $this->db_obj->lastInsertId();
        } catch (Exception $e) {
            return false;
        }
    }
    /**
     * 记录查询 select
     */
    public function select($row = "*"){
        try{
            $db_obj = $this->db_obj;
            $sql = $this->_get_select_sql($row);
            $ret = $db_obj->fetch_all($sql, $this->_where_data, false, $this->_mode);
            $this->clear();
            return $ret;
        }catch (Exception $e) {
            $this->db_log($sql, $e->getMessage());
            throw $e;
        }
    }
    /**
     * 记录查询 get
     */
    public function get($limit = 20){
        return $this->limit(20)->select();
    }
    /**
     * 记录查询,返回一条
     */
    public function getOne($row = "*"){
        try{
            $db_obj = $this->db_obj;
            $sql = $this->_get_select_sql($row);
            $ret = $db_obj->fetch_row($sql, $this->_where_data, false, $this->_mode);
            $this->clear();
            return $ret;
        }catch (Exception $e) {
            $this->db_log($sql, $e->getMessage());
            throw $e;
        }
    }

    /**
     * count
     * @param string $row
     * @return bool|int
     */
    public function count($row = "*"){
        try{
            $db_obj = $this->db_obj;
            $sql = $this->_get_select_sql("count({$row}) as total");
            $ret = $db_obj->fetch_row($sql, $this->_where_data, false, $this->_mode);
            $this->clear();
            return $ret["total"];
        }catch (Exception $e) {
            $this->db_log($sql, $e->getMessage());
            return 0;
        }
    }

    public function clear(){
        $this->_where = array();
        $this->_where_data = array();
        $this->_dup = array();
        $this->_dup_data = array();
        $this->_set = array();
        $this->_set_data = array();
        $this->_ord = array();
        $this->_grp = array();
        $this->_lmt = array();
    }
    private function _get_select_sql($row = "*"){
        $sql = "SELECT {$row} from `{$this->table}`";
        if (!empty($this->_where)){
            $sql .= " WHERE " . implode("", $this->_where);
        }
        if (!empty($this->_grp)){
            $sql .= " GROUP BY " . implode(",", $this->_grp);
        }
        if (!empty($this->_ord)){
            $sql .= " ORDER BY " . implode(",", $this->_ord);
        }
        if (!empty($this->_lmt)){
            $sql .= " LIMIT " . implode(",", $this->_lmt);
        }
        return $sql;
    }
}
