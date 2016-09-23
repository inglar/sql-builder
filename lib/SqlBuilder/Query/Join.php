<?php

namespace SqlBuilder\Query;

use SqlBuilder\DbAdapter\DbAdapter;
use SqlBuilder\SqlBuilderException;

class Join
{
    const INNER = "INNER JOIN";
    const LEFT = "LEFT JOIN";
    const RIGHT = "RIGHT JOIN";

    /**
     * @var DbAdapter
     */
    private $db;

    /**
     * @var string
     */
    private $type = self::INNER;

    /**
     * @var string|Select
     */
    private $table;

    /**
     * @var string
     */
    private $alias;

    /**
     * @var string
     */
    private $condition;

    /**
     * @param DbAdapter $db
     * @param string|Select $table
     * @param string $condition
     * @param string $alias
     * @param string $type
     * @throws SqlBuilderException
     */
    public function __construct(DbAdapter $db, $table, $condition, $alias = null, $type = self::INNER)
    {
        $this->db = $db;

        if (null === $alias && $table instanceof Select) {
            throw new SqlBuilderException("Alias can't be null if table parameter is instance of Select class");
        }

        $this->table = $table;
        $this->condition = $condition;
        $this->alias = $alias;
        $this->type = $type;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        $sql = $this->type . " ";

        if ($this->table instanceof Select) {
            $sql .= "(" . $this->table . ") AS " . $this->db->quoteIdentifier($this->alias);
        } else {
            $sql .= $this->db->quoteIdentifier($this->table);

            if (null !== $this->alias) {
                $sql .= " AS " . $this->db->quoteIdentifier($this->alias);
            }
        }

        $sql .= " ON {$this->condition}";

        return $sql;
    }

    /**
     * @return array
     */
    public function getBindParams()
    {
        if ($this->table instanceof Select) {
            return $this->table->getBindParams();
        } else {
            return [];
        }
    }
}
