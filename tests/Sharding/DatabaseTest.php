<?php
/**
 * @description
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-10-22 17:35:29
 *
 */
namespace Kovey\Sharding\Sharding;

use PHPUnit\Framework\TestCase;

class DatabaseTest extends TestCase
{
    public function testGetShardingKey()
    {
        $data = new Database(2);
        $this->assertEquals(0, $data->getShardingKey(130));
        $this->assertEquals(1, $data->getShardingKey('kovey'));
    }
}
