<?php
/**
 * @description sharding
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-05-09 16:07:46
 *
 */
namespace Kovey\Sharding\Model;

use Kovey\Db\Sql\Insert;
use Kovey\Db\Sql\Update;
use Kovey\Db\Sql\Delete;
use Kovey\Db\Sql\BatchInsert;
use Kovey\Db\Exception\DbException;

abstract class Base
{
    /**
     * @description table name
     *
     * @var string
     */
    protected string $tableName;

    /**
     * @description insert data
     *
     * @param Array $data
     *
     * @param string | int $shardingKey
     *
     * @return int
     *
     * @throws DbException
     */
    public function insert(Array $data, string | int $shardingKey) : int
    {
        $insert = new Insert($this->getTableName($this->database->getShardingKey($shardingKey)));
        foreach ($data as $key => $val) {
            $insert->$key = $val;
        }

        return $this->database->insert($insert, $shardingKey);
    }

    /**
     * @description update data
     *
     * @param Array $data
     *
     * @param Array $condition
     *
     * @param string | int $shardingKey
     *
     * @return int
     *
     * @throws DbException
     */
    public function update(Array $data, Array $condition, string | int $shardingKey) : int
    {
        $update = new Update($this->getTableName($this->database->getShardingKey($shardingKey)));
        foreach ($data as $key => $val) {
            $update->$key = $val;
        }

        $update->where($condition);
        return $this->database->update($update, $shardingKey);
    }

    /**
     * @description fetch row
     *
     * @param Array $condition
     *
     * @param Array $columns
     *
     * @param string | int $shardingKey
     *
     * @return Array | bool
     *
     * @throws DbException
     */
    public function fetchRow(Array $condition, Array $columns, string | int $shardingKey) : Array | bool
    {
        if (empty($columns)) {
            throw new DbException('selected columns is empty.', 1004); 
        }

        return $this->database->fetchRow($this->getTableName($this->database->getShardingKey($shardingKey)), $condition, $columns, $shardingKey);
    }

    /**
     * @description fetch all rows
     *
     * @param Array $condition
     *
     * @param Array  $columns
     *
     * @param string | int $shardingKey
     *
     * @return Array | false
     *
     * @throws DbException
     */
    public function fetchAll(Array $condition, Array $columns, string | int $shardingKey) : Array | bool
    {
        if (empty($columns)) {
            throw new DbException('selected columns is empty.', 1005); 
        }

        return $this->database->fetchAll($this->getTableName($this->database->getShardingKey($shardingKey)), $condition, $columns, $shardingKey);
    }

    /**
     * @description batch insert
     *
     * @param Array $rows
     *
     * @param string | int $shardingKey
     *
     * @return int
     *
     * @throws DbException
     */
    public function batchInsert(Array $rows, string | int $shardingKey) : int
    {
        if (empty($rows)) {
            throw new DbException('rows is empty.', 1006);
        }

        $sk = $this->database->getShardingKey($shardingKey);
        $batchInsert = new BatchInsert($this->getTableName($sk));
        foreach ($rows as $row) {
            $insert = new Insert($this->getTableName($sk));
            foreach ($row as $key => $val) {
                $insert->$key = $val;
            }

            $batchInsert->add($insert);
        }

        return $this->database->batchInsert($batchInsert, $shardingKey);
    }

    /**
     * @description delete
     *
     * @param Array $data
     *
     * @param Array $condition
     *
     * @param string | int $shardingKey
     *
     * @return int
     *
     * @throws DbException
     */
    public function delete(Array $condition,  string | int $shardingKey) : int
    {
        $delete = new Delete($this->getTableName($this->database->getShardingKey($shardingKey)));
        $delete->where($condition);
        return $this->database->delete($delete, $shardingKey);
    }

    /**
     * @description get table name
     *
     * @param int $shardingKey
     *
     * @return string
     */
    public function getTableName(int $shardingKey) : string
    {
        return $this->tableName . '_' . $shardingKey;
    }
}
