<?php

namespace SqlBuilder\Query;

use SqlBuilder\DbAdapter\DbAdapter;
use SqlBuilder\SqlBuilderException;

class Select
{
    /** "Order by" qualifier - ascending */
    const ASC = 'ASC';

    /** "Order by" qualifier - descending */
    const DESC = 'DESC';

    const LOCK_FOR_SHARE = 'SHARE';
    const LOCK_FOR_UPDATE = 'UPDATE';

    /**
     * @var DbAdapter
     */
    private $db;

    /**
     * @var array
     */
    private $orderDirections = array(
        self::ASC,
        self::DESC,
    );

    private $lockOptions = array(
        self::LOCK_FOR_SHARE,
        self::LOCK_FOR_UPDATE,
    );

    /**
     * @var Select[]
     */
    private $with = [];

    /**
     * @var bool
     */
    private $distinct = false;

    /**
     * @var array
     */
    private $columns = [];

    /**
     * @var array
     */
    private $from = [];

    /**
     * @var array
     */
    private $join = [];

    /**
     * @var array
     */
    private $where = [];

    /**
     * @var array
     */
    private $groupBy = [];

    /**
     * @var array
     */
    private $having = [];

    /**
     * @var array
     */
    private $orderBy = [];

    /**
     * @var int
     */
    private $limitRows;

    /**
     * @var int
     */
    private $limitOffset;

    /**
     * @var string
     */
    private $lock;

    /**
     * @var array
     */
    private $bindParams = [];

    /**
     * @var array
     */
    private $replacePlaceholders = [];

    /**
     * @param DbAdapter $db
     */
    public function __construct(DbAdapter $db)
    {
        $this->db = $db;
    }

    /**
     * @param string $alias
     * @param Select $select
     * @return $this
     * @throws SqlBuilderException
     */
    public function with($alias, Select $select)
    {
        if (!$this->db->supportWith()) {
            throw new SqlBuilderException("Current db driver doesn't support WITH clause");
        }

        if (empty($alias)) {
            throw new SqlBuilderException("Alias can't be empty");
        }

        $this->with[$alias] = $select;
        return $this;
    }

    /**
     * @param bool $v
     * @return $this
     */
    public function distinct($v = true)
    {
        $this->distinct = (bool)$v;
        return $this;
    }

    /**
     * @param string $clause
     * @param null|string $alias
     * @return $this
     * @throws SqlBuilderException
     */
    public function column($clause, $alias = null)
    {
        if (null === $alias && $clause instanceof Select) {
            throw new SqlBuilderException("Alias can't be null if clause parameter is instance of Select class");
        }

        if (null !== $alias) {
            $this->columns[$alias] = $clause;
        } else {
            $this->columns[] = $clause;
        }

        return $this;
    }

    /**
     * @param array $columns
     *      array('col1', 'col2', ...)
     *      or
     *  array('alias1' => 'col1', 'col2', ...)
     * @return $this
     */
    public function columns(array $columns)
    {
        foreach ($columns as $alias => $column) {
            if (is_string($alias)) {
                $this->column($column, $alias);
            } else {
                $this->column($column);
            }
        }

        return $this;
    }

    /**
     * @param string|Select|Union $table
     * @param null|string $alias
     * @return $this
     * @throws SqlBuilderException
     */
    public function from($table, $alias = null)
    {
        if (null === $alias && ($table instanceof Select || $table instanceof Union)) {
            throw new SqlBuilderException("Alias can't be null if table parameter is instance of Select class");
        }

        if (null !== $alias) {
            $this->from[$alias] = $table;
        } else {
            $this->from[] = $table;
        }

        return $this;
    }

    /**
     * @param string|Select $table
     * @param string $condition
     * @param string $alias
     * @param string $type
     * @return $this
     */
    public function join($table, $condition, $alias = null, $type = Join::INNER)
    {
        $this->join[] = new Join($this->db, $table, $condition, $alias, $type);
        return $this;
    }

    /**
     * @param string $clause
     * @return $this
     */
    public function where($clause)
    {
        $this->where[] = $clause;
        return $this;
    }

    /**
     * @param string $column
     * @return $this
     */
    public function groupBy($column)
    {
        $this->groupBy[] = $column;
        return $this;
    }

    /**
     * @param string $clause
     * @return $this
     */
    public function having($clause)
    {
        $this->having[] = $clause;
        return $this;
    }

    /**
     * @param string $column
     * @param string $direction
     * @return $this
     * @throws SqlBuilderException
     */
    public function orderBy($column, $direction = self::ASC)
    {
        $direction = strtoupper($direction);

        if (!in_array($direction, $this->orderDirections)) {
            throw new SqlBuilderException("Invalid direction parameter ($direction)");
        }

        $this->orderBy[$column] = $direction;
        return $this;
    }

    /**
     * @param int $rows
     * @param null|int $offset
     * @return $this
     */
    public function limit($rows, $offset = null)
    {
        $this->limitRows = (int)$rows;

        if (!empty($offset)) {
            $this->limitOffset = (int)$offset;
        }

        return $this;
    }

    /**
     * @param string $lock
     * @return $this
     * @throws SqlBuilderException
     */
    public function lock($lock)
    {
        if (!in_array($lock, $this->lockOptions)) {
            throw new SqlBuilderException("Invalid lock parameter ($lock)");
        }

        $this->lock = $lock;
        return $this;
    }

    /**
     * @param $key
     * @param $value
     * @param bool|false $overwrite
     * @return $this
     * @throws SqlBuilderException
     */
    public function bindParam($key, $value, $overwrite = false)
    {
        if (preg_match('/[^:\w\d_]/', $key)) {
            throw new \InvalidArgumentException("Symbols except letters, digits and underscore not allowed in bind parameter key");
        }

        if (!$overwrite && isset($this->bindParams[$key])) {
            throw new SqlBuilderException("Bind parameter key '$key' already set");
        }

        if (is_array($value) || $value instanceof \Iterator) {
            $idx = 1;
            $replace = [];

            foreach ($value as $item) {
                $this->bindParams[$key . $idx] = $item;
                $replace[] = $key . $idx;
                $idx++;
            }

            $this->replacePlaceholders[$key] = implode(', ', $replace);
        } else {
            $this->bindParams[$key] = $value;
        }

        return $this;
    }

    /**
     * @param array $params
     * @return $this
     */
    public function bindParams(array $params)
    {
        foreach ($params as $key => $value) {
            $this->bindParam($key, $value);
        }

        return $this;
    }

    /**
     * @return array
     */
    public function getBindParams()
    {
        $bindParams = $this->bindParams;

        foreach ($this->with as $with) {
            if ($with instanceof Select) {
                $bindParams = array_merge($bindParams, $with->getBindParams());
            }
        }

        foreach ($this->columns as $column) {
            if ($column instanceof Select) {
                $bindParams = array_merge($bindParams, $column->getBindParams());
            }
        }

        foreach ($this->from as $from) {
            if ($from instanceof Select || $from instanceof Union) {
                $bindParams = array_merge($bindParams, $from->getBindParams());
            }
        }

        foreach ($this->join as $join) {
            if ($join instanceof Join) {
                $bindParams = array_merge($bindParams, $join->getBindParams());
            }
        }

        return $bindParams;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        $withSql = $this->getWithSql();
        $sql = (!empty($withSql) ? $withSql . " " : '')
            . "SELECT"
            . ($this->distinct ? ' DISTINCT' : '')
            . " {$this->getColumnsSql()}"
            . " FROM {$this->getFromSql()}"
            . " {$this->getJoinSql()}"
            . " {$this->getWhereSql()}"
            . " {$this->getGroupBySql()}"
            . " {$this->getHavingSql()}"
            . " {$this->getOrderBySql()}"
            . " {$this->getLimitSql()}"
            . " {$this->getLockSql()}";

        $sql = $this->replaceArrayParameterPlaceholders($sql);

        return trim(preg_replace('/\s{2,}/', ' ', $sql));
    }

    /**
     * @return string
     */
    private function getWithSql()
    {
        $sql = "";

        if (empty($this->with)) {
            return $sql;
        }

        $sql .= "WITH ";
        $withArr = [];

        foreach ($this->with as $alias => $with) {
            $withArr[] = $this->db->quoteIdentifier($alias) . " AS ($with)";
        }

        $sql .= implode(', ', $withArr);

        return $sql;
    }

    /**
     * @return string
     */
    private function getColumnsSql()
    {
        $sql = "";

        foreach ($this->columns as $alias => $clause) {
            if (!empty($sql)) {
                $sql .= ", ";
            }

            if ($clause instanceof Select) {
                $sql .= "($clause)";
            } else if ('*' == $clause || is_numeric($clause) || 'NULL' == $clause
                || false !== strpos($clause, '.') || false !== strpos($clause, "'")
                || false !== strpos($clause, '(') || false !== strpos($clause, ')')) {
                $sql .= $clause;
            } else {
                $sql .= $this->db->quoteIdentifier($clause);
            }

            if (is_string($alias)) {
                $sql .= " AS " . $this->db->quoteIdentifier($alias);
            }
        }

        return $sql;
    }

    /**
     * @return string
     */
    private function getFromSql()
    {
        $sql = "";

        foreach ($this->from as $alias => $table) {
            if (!empty($sql)) {
                $sql .= ", ";
            }

            if ($table instanceof Select
                || $table instanceof Union) {
                $sql .= "($table) AS " . $this->db->quoteIdentifier($alias);
            } else {
                $sql .= $this->db->quoteIdentifier($table);

                if (is_string($alias)) {
                    $sql .= " AS " . $this->db->quoteIdentifier($alias);
                }
            }
        }

        return $sql;
    }

    /**
     * @return string
     */
    private function getJoinSql()
    {
        $sql = "";

        foreach ($this->join as $join) {
            if (!empty($sql)) {
                $sql .= " ";
            }

            $sql .= $join;
        }

        return $sql;
    }

    /**
     * @return string
     */
    private function getWhereSql()
    {
        $sql = "";

        foreach ($this->where as $clause) {
            if (empty($sql)) {
                $sql .= "WHERE ";
            } else {
                $sql .= " AND ";
            }

            $sql .= $clause;
        }

        return $sql;
    }

    /**
     * @return string
     */
    private function getGroupBySql()
    {
        $sql = "";

        foreach ($this->groupBy as $column) {
            $sql .= (empty($sql)) ? "GROUP BY " : ", ";
            $sql .= $column;
        }

        return $sql;
    }

    /**
     * @return string
     */
    private function getHavingSql()
    {
        $sql = "";

        foreach ($this->having as $clause) {
            if (empty($sql)) {
                $sql .= "HAVING ";
            } else {
                $sql .= " AND ";
            }

            $sql .= $clause;
        }

        return $sql;
    }

    /**
     * @return string
     */
    private function getOrderBySql()
    {
        $sql = "";

        foreach ($this->orderBy as $column => $direction) {
            $sql .= (empty($sql)) ? "ORDER BY " : ", ";
            $sql .= $column;

            if (self::DESC == $direction) {
                $sql .= " " . $direction;
            }
        }

        return $sql;
    }

    /**
     * @return string
     */
    private function getLimitSql()
    {
        if (null === $this->limitRows && null === $this->limitOffset) {
            return '';
        }

        if (null !== $this->limitOffset) {
            return "LIMIT {$this->limitRows} OFFSET {$this->limitOffset}";
        } else {
            return "LIMIT {$this->limitRows}";
        }
    }

    /**
     * @return string
     */
    private function getLockSql()
    {
        if (null === $this->lock) {
            return '';
        }

        return "FOR " . $this->lock;
    }

    /**
     * @param string $sql
     * @return string
     */
    private function replaceArrayParameterPlaceholders($sql)
    {
        foreach ($this->replacePlaceholders as $key => $value) {
            $sql = preg_replace('/' . preg_quote($key) . '([^\w\d_])/', $value . '$1', $sql);
        }

        return $sql;
    }
}
