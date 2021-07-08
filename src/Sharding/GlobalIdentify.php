<?php
/**
 *
 * @description global uniqid
 *
 * @package     Db\Sharding
 *
 * @time        Tue Oct  1 00:47:28 2019
 *
 * @author      kovey
 */
namespace Kovey\Sharding\Sharding;

use Kovey\Redis\RedisInterface;

class GlobalIdentify
{
    /**
     * @description redis
     *
     * @var Kovey\Redis\RedisInterface
     */
    private RedisInterface $redis;

    /**
     * @description global key
     *
     * @var string
     */
    private string $key;

    /**
     * @description construct
     *
     * @param Kovey\Redis\RedisInterface $redis
     *
     * @return GlobalIdentify
     */
    public function __construct(RedisInterface $redis, string $key)
    {
        $this->redis = $redis;
        $this->key = $key;
    }

    /**
     * @description get global indetify
     *
     * @return int
     */
    public function getGlobalIdentify() : int
    {
        return $this->redis->incr($this->key);
    }
}
