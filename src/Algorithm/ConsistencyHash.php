<?php
/**
 * @description consistency hash
 *
 * @package
 *
 * @author kovey
 *
 * @time 2020-11-16 23:44:15
 *
 */
namespace Kovey\Sharding\Algorithm;

class ConsistencyHash
{

    /**
     * @description nodes
     *
     * @var Array
     */
    protected Array $nodes = array();

    /**
     * @description virtual nodes
     *
     * @var Array
     */
    protected Array $vNodes = array();

    /**
     * @description is sort
     *
     * @var bool
     */
    protected bool $isSort = false;

    /**
     * @description virtual notte count
     *
     * @var int
     */
    protected int $vNodesCount;

    /**
     * @description construct
     *
     * @param int $vNodesCount
     *
     * @return ConsistencyHash
     */
    public function __construct(int $vNodesCount = 10)
    {
        $this->vNodesCount = $vNodesCount;
    }

    /**
     * @description add node
     *
     * @param string | int $node
     *
     * @return ConsistencyHash
     */
    public function addNode(string | int $node) : ConsistencyHash
    {
        $this->nodes[] = $node;

        for ($i = 0; $i < $this->vNodesCount; $i ++ ) {
            $vHashKey = sprintf("%u", crc32($node . $i));
            $this->vNodes[$vHashKey] = $node;
        }

        return $this;
    }

    /**
     * @description add nodes
     *
     * @param Array $nodes
     *
     * @return ConsistencyHash
     */
    public function addNodes(Array $nodes) : ConsistencyHash
    {
        foreach ($nodes as $node) {
            $this->addNode($node);
        }

        return $this;
    }

    /**
     * @description get node
     *
     * @param string | int $key
     *
     * @return string | int
     */
    public function getNode(string | int $key) : string | int
    {
        if (!$this->isSort) {
            ksort($this->vNodes);
            $this->isSort = true;
        }

        $hKey = sprintf("%u", crc32($key));

        foreach ($this->vNodes as $vHashKey => $node) {
            if ($hKey < $vHashKey) {
                return $node;
            }
        }

        return $this->nodes[0];
    }
}
