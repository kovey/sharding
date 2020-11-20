<?php
/**
 * @description
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-11-17 23:17:37
 *
 */
namespace Kovey\Sharding\Algorithm;

use PHPUnit\Framework\TestCase;

class ConsistencyHashTest extends TestCase
{
    public function testGetNode()
    {
        $hash = new ConsistencyHash();
        $nodes = array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        $this->assertInstanceOf(ConsistencyHash::class, $hash->addNodes($nodes));
        $this->assertEquals(5, $hash->getNode('kovey'));
        $this->assertEquals(7, $hash->getNode('kovey1'));
    }
}
