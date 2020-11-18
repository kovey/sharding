<?php
/**
 * @description
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-11-16 23:18:26
 *
 */
namespace Kovey\Sharding;

interface RedisInterface
{
	/**
	 * @description construct
	 *
     * @param int $redisCount
     *
     * @param callable $initPool
     *
     * @param Array $shardingKeys
	 */
	public function __construct(int $redisCount, callable | Array $initPool, Array $shardingKeys = array());

    /**
     * @description get sharding key
     *
     * @param string | int $shardingKey
     *
     * @return int : string
     */
    public function getShardingKey(string | int $shardingKey) : int | string;

    /**
     * @description add sharding key
     *
     * @param string | int $shardingKey
     *
     * @return DbInterface
     *
     */
    public function addShardingKey(string | int $shardingKey) : RedisInterface;
}
