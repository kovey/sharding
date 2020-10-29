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
use Kovey\Sharding\DbInterface;
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
	 * @param DbInterface $db
     *
     * @param string | int $shardingKey
	 *
	 * @return int
	 *
	 * @throws DbException
	 */
	public function insert(Array $data, DbInterface $db, string | int $shardingKey) : int
	{
		$insert = new Insert($this->getTableName($db->getShardingKey($shardingKey)));
		foreach ($data as $key => $val) {
			$insert->$key = $val;
		}

		return $db->insert($insert, $shardingKey);
	}

	/**
	 * @description update data
	 *
	 * @param Array $data
	 *
	 * @param Array $condition
	 *
	 * @param DbInterface $db
     *
     * @param string | int $shardingKey
	 *
	 * @return int
	 *
	 * @throws DbException
	 */
	public function update(Array $data, Array $condition, DbInterface $db, string | int $shardingKey) : int
	{
		$update = new Update($this->getTableName($db->getShardingKey($shardingKey)));
		foreach ($data as $key => $val) {
			$update->$key = $val;
		}

		$update->where($condition);
		return $db->update($update, $shardingKey);
	}

	/**
	 * @description fetch row
	 *
	 * @param Array $condition
	 *
	 * @param Array $columns
	 *
	 * @param DbInterface $db
     *
     * @param string | int $shardingKey
	 *
	 * @return Array | bool
	 *
	 * @throws DbException
	 */
	public function fetchRow(Array $condition, Array $columns, DbInterface $db, string | int $shardingKey) : Array | bool
	{
		if (empty($columns)) {
			throw new DbException('selected columns is empty.', 1004); 
		}

		return $db->fetchRow($this->getTableName($db->getShardingKey($shardingKey)), $condition, $columns, $shardingKey);
	}

	/**
	 * @description fetch all rows
	 *
	 * @param Array $condition
	 *
	 * @param Array  $columns
	 *
	 * @param DbInterface $db
     *
     * @param string | int $shardingKey
	 *
	 * @return Array | false
	 *
	 * @throws DbException
	 */
	public function fetchAll(Array $condition, Array $columns, DbInterface $db, string | int $shardingKey) : Array | bool
	{
		if (empty($columns)) {
			throw new DbException('selected columns is empty.', 1005); 
		}

		return $db->fetchAll($this->getTableName($db->getShardingKey($shardingKey)), $condition, $columns, $shardingKey);
	}

	/**
	 * @description batch insert
	 *
	 * @param Array $rows
	 *
	 * @param DbInterface $db
     *
     * @param string | int $shardingKey
	 *
	 * @return int
	 *
	 * @throws DbException
	 */
	public function batchInsert(Array $rows, DbInterface $db, string | int $shardingKey) : int
	{
		if (empty($rows)) {
			throw new DbException('rows is empty.', 1006);
		}

        $sk = $db->getShardingKey($shardingKey);
		$batchInsert = new BatchInsert($this->getTableName($sk));
		foreach ($rows as $row) {
			$insert = new Insert($this->getTableName($sk));
			foreach ($row as $key => $val) {
				$insert->$key = $val;
			}

			$batchInsert->add($insert);
		}

		return $db->batchInsert($batchInsert, $shardingKey);
	}

	/**
	 * @description delete
	 *
	 * @param Array $data
	 *
	 * @param Array $condition
	 *
	 * @param DbInterface $db
     *
     * @param string | int $shardingKey
	 *
	 * @return int
	 *
	 * @throws DbException
	 */
	public function delete(Array $condition, DbInterface $db, string | int $shardingKey) : int
	{
		$delete = new Delete($this->getTableName($db->getShardingKey($shardingKey)));
		$delete->where($condition);
		return $db->delete($delete, $shardingKey);
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
