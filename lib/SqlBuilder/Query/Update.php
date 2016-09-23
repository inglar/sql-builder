<?php

namespace SqlBuilder\Query;

use SqlBuilder\DbAdapter\DbAdapter;
use SqlBuilder\SqlBuilderException;

class Update
{
    const OPERATOR_ADDITION = '+';
    const OPERATOR_SUBTRACTION = '-';
    const OPERATOR_MULTIPLICATION = '*';
    const OPERATOR_DIVISION = '/';

    /**
     * @var array
     */
    private static $operators = [
        self::OPERATOR_ADDITION,
        self::OPERATOR_SUBTRACTION,
        self::OPERATOR_MULTIPLICATION,
        self::OPERATOR_DIVISION,
    ];

    /**
     * @var DbAdapter
     */
    private $db;

    /**
     * @var string
     */
    private $table;

    /**
     * @var array
     */
    private $columns = [];

    /**
     * @var array
     */
    private $where = [];

    /**
     * @var array
     */
    private $bindParams = [];

    /**
     * @var array
     */
    private $allowedColumns = [];

    /**
     * @param DbAdapter $db
     * @param array $allowedColumns
     */
    public function __construct(DbAdapter $db, array $allowedColumns = [])
    {
        $this->db = $db;
        $this->allowedColumns = $allowedColumns;
    }

    /**
     * @param string $table
     * @return $this
     */
    public function table($table)
    {
        $this->table = $table;
        return $this;
    }

    /**
     * @param string $column
     * @param int|string|null $value
     * @return $this
     */
    public function column($column, $value)
    {
        if (!empty($this->allowedColumns) && !in_array($column, $this->allowedColumns)) {
            return $this;
        }

        $this->columns[$column] = $value;
        return $this;
    }

    /**
     * @param array $columns
     *      array('col1' => 'val1', ..., 'colN' => 'valN')
     * @return $this
     */
    public function columns(array $columns)
    {
        foreach ($columns as $column => $value) {
            $this->column($column, $value);
        }

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
     * @param string $key
     * @param int|string $value
     * @param bool|false $overwrite
     * @return $this
     * @throws SqlBuilderException
     */
    public function bindParam($key, $value, $overwrite = false)
    {
        if (!$overwrite && isset($this->bindParams[$key])) {
            throw new SqlBuilderException("Bind parameter key '$key' already set");
        }

        $this->bindParams[$key] = $value;
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

        foreach ($this->columns as $column => $value) {
            if ($value instanceof Select) {
                $bindParams = array_merge($bindParams, $value->getBindParams());
            }
        }

        return $bindParams;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        $sql = "UPDATE"
            . " {$this->getTableSql()}"
            . " SET {$this->getColumnsSql()}"
            . " {$this->getWhereSql()}";

        return trim(preg_replace('/\s{2,}/', ' ', $sql));
    }

    private function getTableSql()
    {
        return $this->db->quoteIdentifier($this->table);
    }

    /**
     * @return string
     * @throws SqlBuilderException
     */
    private function getColumnsSql()
    {
        if (empty($this->columns)) {
            throw new SqlBuilderException("'columns' property can't be empty in update statement");
        }

        /**
         * @param null|int|string $value
         * @return string
         */
        $getValueStr = function($value) {
            if (null === $value) {
                return "NULL";
            } else if (is_numeric($value)) {
                return $value;
            } else if (':' === substr($value, 0, 1)) {
                return $value;
            } else {
                return "'$value'";
            }
        };

        $sql = "";

        foreach ($this->columns as $column => $value) {
            if (!empty($sql)) {
                $sql .= ", ";
            }

            $sql .= $this->db->quoteIdentifier($column) . " = ";

            if ($value instanceof Select) {
                $sql .= "($value)";
            } else if (is_array($value)) {
                if (!in_array($value[1], self::$operators)) {
                    throw new SqlBuilderException("Invalid operator {$value[1]}");
                }

                $sql .= $this->db->quoteIdentifier($column) . " {$value[1]} " . $getValueStr($value[0]);
            } else {
                $sql .= $getValueStr($value);
            }
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
}
