<?php
/**
 * @description
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-10-29 09:42:02
 *
 */
namespace Kovey\Sharding;

use Kovey\Db\Sql\Update;
use Kovey\Db\Sql\Insert;
use Kovey\Db\Sql\Select;
use Kovey\Db\Sql\Delete;
use Kovey\Db\Sql\BatchInsert;
use Kovey\Db\Sql\Where;
use Kovey\Sharding\Sharding\Database;
use Kovey\Db\Exception\DbException;
use Kovey\Connection\Pool;

class Mysql extends Base implements DbInterface
{
    /**
     * @description data base
     *
     * @var Database
     */
    protected Database $database;

    protected function initAlgorithm(int $count) : void
    {
        $this->database = new Database($dbCount);
    }

    /**
     * @description get sharding key
     *
     * @param string | int $shardingKey
     *
     * @return int
     */
    public function getShardingKey(string | int $shardingKey) : int
    {
        return $this->database->getShardingKey($shardingKey);
    }

    /**
     * @description query
     *
     * @param string $sql
     *
     * @param string | int $shardingKey
     *
     * @return Array
     */
    public function query(string $sql, string | int $shardingKey) : Array
    {
        return $this->getConnection($shardingKey)->query($sql);
    }

    /**
     * @description commit transation
     *
     * @return bool
     *
     * @throws DbException
     */
    public function commit() : bool
    {
        foreach ($this->connections as $connection) {
            if (!$connection->getConnection()->inTransaction()) {
                continue;
            }

            if (!$connection->getConnection()->commit()) {
                throw new DbException('commit fail: ' . $connection->getConnection()->getError(), 1010);
            }
        }

        return true;
    }

    /**
     * @description open transation
     *
     * @return bool
     */
    public function beginTransaction() : bool
    {
        foreach ($this->connections as $connection) {
            if ($connection->getConnection()->inTransaction()) {
                continue;
            }

            if (!$connection->getConnection()->beginTransaction()) {
                $this->rollBack();
                return false;
            }
        }

        return true;
    }

    /**
     * @description cancel transation
     *
     * @return bool
     */
    public function rollBack() : bool
    {
        foreach ($this->connections as $connection) {
            if (!$connection->getConnection()->inTransaction()) {
                continue;
            }

            $connection->getConnection()->rollBack();
        }

        return true;
    }

    /**
     * @description fetch row
     *
     * @param string $table
     *
     * @param Array $condition
     *
     * @param Array $columns
     *
     * @return Array | bool
     *
     * @throws Exception
     */
    public function fetchRow(string $table, Array | Where $condition, Array $columns, string | int $shardingKey) : Array | bool
    {
        return $this->getConnection($shardingKey)->fetchRow($table, $condition, $columns);
    }

    /**
     * @description fetch all rows
     *
     * @param string $table
     *
     * @param Array $condition
     *
     * @param Array $columns
     *
     * @return Array
     *
     * @throws Exception
     */
    public function fetchAll(string $table, Array | Where $condition, Array $columns, string | int $shardingKey) : array
    {
        return $this->getConnection($shardingKey)->fetchAll($table, $condition, $columns);
    }

    /**
     * @description execute update sql
     *
     * @param Update $update
     *
     * @return int
     */
    public function update(Update $update, string | int $shardingKey) : int
    {
        return $this->getConnection($shardingKey)->update($update);
    }

    /**
     * @description execute insert sql
     *
     * @param Insert $insert
     *
     * @return int
     */
    public function insert(Insert $insert, string | int $shardingKey) : int
    {
        return $this->getConnection($shardingKey)->insert($insert);
    }

    /**
     * @description execute select sql
     *
     * @param Select $select
     *
     * @param int $type
     *
     * @return Array | bool
     */
    public function select(Select $select, string | int $shardingKey, int $type = Select::ALL)
    {
        return $this->getConnection($shardingKey)->select($select, $type);
    }

    /**
     * @description batch insert
     *
     * @param BatchInsert $batchInsert
     *
     * @return int
     *
     * @throws DbException
     *
     */
    public function batchInsert(BatchInsert $batchInsert, string | int $shardingKey) : int
    {
        return $this->getConnection($shardingKey)->batchInsert($batchInsert);
    }

    /**
     * @description 删除
     *
     * @param Delete $delete
     *
     * @return bool
     *
     * @throws Exception
     */
    public function delete(Delete $delete, string | int $shardingKey) : int
    {
        return $this->getConnection($shardingKey)->delete($delete);
    }

    /**
     * @description run transation
     *
     * @param callable $fun
     *
     * @param mixed $finally
     *
     * @param ...$params
     *
     * @return bool
     *
     * @throws DbException
     */
    public function transaction(callable $fun, mixed $finally, mixed ...$params) : bool
    {
        $this->beginTransaction();
        try {
            call_user_func($fun, $this, ...$params);
            $this->commit();
        } catch (DbException $e) {
            $this->rollBack();
            throw $e;
        } finally {
            if (is_callable($finally)) {
                call_user_func($finally, $this, ...$params);
            }
        }

        return true;
    }

    /**
     * @description exec sql
     *
     * @param string $sql
     *
     * @return int
     *
     * @throws DbException
     */
    public function exec(string $sql, string | int $shardingKey) : int
    {
        return $this->getConnection($shardingKey)->exec($sql);
    }
}
