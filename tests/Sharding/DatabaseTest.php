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
namespace Kovey\Db\Sharding;

use PHPUnit\Framework\TestCase;

class DatabaseTest extends TestCase
{
    public function testGetShardingKey()
    {
        $data = new Database();
        $this->assertEquals(11, $data->getShardingKey(139));
        $this->assertEquals(31, $data->getShardingKey('aaa'));
    }
}
