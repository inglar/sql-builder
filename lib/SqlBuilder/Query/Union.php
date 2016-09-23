<?php

namespace SqlBuilder\Query;

use SqlBuilder\DbAdapter\DbAdapter;

class Union
{
    const ALL = "ALL";
    const DISTINCT = "DISTINCT";

    /**
     * @var DbAdapter
     */
    private $db;

    /**
     * @var array
     */
    private static $types = array(
        self::ALL,
        self::DISTINCT,
    );

    /**
     * @var Select[]
     */
    private $selects;

    /**
     * @var array
     */
    private $unionTypes;

    /**
     * @var null|string
     */
    private $alias;

    /**
     * @param DbAdapter $db
     * @param Select[] $selects
     * @param array $unionTypes
     * @param string $alias
     */
    public function __construct(DbAdapter $db, array $selects, array $unionTypes = [], $alias = null)
    {
        $this->db = $db;
        $this->selects = $selects;
        $this->unionTypes = $unionTypes;
        $this->alias = $alias;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        $sql = "";
        $idx = -1;

        foreach ($this->selects as $select) {
            if (!empty($sql)) {
                $type = isset($this->unionTypes[$idx]) && in_array($this->unionTypes[$idx], self::$types) ? $this->unionTypes[$idx] : "ALL";
                $sql .= " UNION $type ";
            }

            $sql .= $select;
            $idx++;
        }

        return $sql;
    }

    /**
     * @return array
     */
    public function getBindParams()
    {
        $params = [];

        foreach ($this->selects as $select) {
            $params = array_merge($params, $select->getBindParams());
        }

        return $params;
    }
}
