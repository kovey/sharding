<?php
/**
 *
 * @description database interface
 *
 * @package     Db
 *
 * @time        Tue Sep 24 09:03:29 2019
 *
 * @author      kovey
 */
namespace Kovey\Sharding;

use Kovey\Db\Sql\Update;
use Kovey\Db\Sql\Insert;
use Kovey\Db\Sql\Select;
use Kovey\Db\Sql\Delete;
use Kovey\Db\Sql\BatchInsert;
use Kovey\Db\DbInterface as DI;

interface DbInterface
{
	/**
	 * @description construct
	 *
	 * @param Array $shardingKeys
	 */
	public function __construct(Array $shardingKeys, int $dbCount);

	/**
	 * @description query
     *
     * @param string $sql
     *
     * @param string | int $shardingKey
	 *
	 * @return Array
	 */
	public function query(string $sql, string | int $shardingKey) : Array;

	/**
	 * @description commit transation
	 *
	 * @return bool
	 */
	public function commit() : bool;

	/**
	 * @description open transation
	 *
	 * @return bool
	 */
	public function beginTransaction() : bool;

	/**
	 * @description cancel transation
	 *
	 * @return bool
	 */
	public function rollBack() : bool;

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
	public function fetchRow(string $table, Array $condition, Array $columns, string | int $shardingKey) : Array | bool;

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
	public function fetchAll(string $table, Array $condition = array(), Array $columns, string | int $shardingKey) : array;

	/**
	 * @description execute update sql
	 *
	 * @param Update $update
	 *
	 * @return int
	 */
	public function update(Update $update, string | int $shardingKey) : int;

	/**
	 * @description execute insert sql
	 *
	 * @param Insert $insert
	 *
	 * @return int
	 */
	public function insert(Insert $insert, string | int $shardingKey) : int;

	/**
	 * @description execute select sql
	 *
	 * @param Select $select
	 *
	 * @param int $type
	 *
	 * @return Array | bool
	 */
	public function select(Select $select, string | int $shardingKey, int $type = Select::ALL);

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
    public function batchInsert(BatchInsert $batchInsert, string | int $shardingKey) : int;

	/**
	 * @description 删除
	 *
	 * @param Delete $delete
	 *
     * @return bool
     *
     * @throws Exception
	 */
    public function delete(Delete $delete, string | int $shardingKey) : int;

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
    public function transation(callable $fun, $finally, ...$params) : bool;

    /**
     * @description exec sql
     *
     * @param string $sql
     *
     * @return int
     *
     * @throws DbException
     */
    public function exec(string $sql, string | int $shardingKey) : int;

    /**
     * @description get sharding key
     *
     * @param string | int $shardingKey
     *
     * @return int
     */
    public function getShardingKey(string | int $shardingKey) : int;

    /**
     * @description get connection
     *
     * @param string | int $shardingKey
     *
     * @return DbInterface
     */
    public function getConnection(string | int $shardingKey) : DI;
}
