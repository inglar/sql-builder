<?php

namespace SqlBuilder\DbAdapter;

abstract class DbAdapter
{
    /**
     * @param string $text
     * @return string
     */
    public function quoteIdentifier($text)
    {
        return '"' . $text . '"';
    }

    /**
     * @return bool
     */
    abstract public function supportWith();
}
