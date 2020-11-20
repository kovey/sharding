<?php
/**
 * @description
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-11-20 17:03:55
 *
 */
namespace Kovey\Sharding;

use PHPUnit\Framework\TestCase;
use Kovey\Connection\Pool;
use Kovey\Connection\Pool\Redis as PR;

class RedisTest extends TestCase
{
    public function testRedis()
    {
        $redis = new Redis(2, function ($shardingKey) {
            $pool = new PR(array(
                'min' => 1,
                'max' => 2
            ), array(
                'host' => '127.0.0.1',
                'port' => 6379,
                'db' => 0
            ));
            $pool->init();
            return new Pool($pool);
        });
        $redis->addShardingKey('kovey')
            ->addShardingKey(1000);
        $redis->set('kovey', 'framework');
        $redis->hSet(1000, 'kovey', '1000');

        $this->assertEquals('framework', $redis->get('kovey'));
        $this->assertEquals('1000', $redis->hGet(1000, 'kovey'));

        $redis->del('kovey');
        $redis->del(1000);
    }
}
