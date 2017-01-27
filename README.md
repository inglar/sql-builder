Installation
============

Composer
--------

Open a command console, enter your project directory and execute the following command to download the latest stable version of this package:

```bash
$ composer require inglar/sql-builder
```

This command requires you to have Composer installed globally, as explained
in the [installation chapter](https://getcomposer.org/doc/00-intro.md)
of the Composer documentation.

Supported Adapters
==================

SqlBuilder supports the following database adapters:

* MySQL (specify *mysql*)
* PostgreSQL (specify *pgsql*)

Usage
=====

**Simple select**

```php
$builder = new SqlBuilder('pgsql');
$select = $builder->select()
    ->column('*')
    ->from('table')
    ->where('id = :id')
    ->bindParam(':id', 123);
 
echo $select;
print_r($select->getBindParams());
```

The above example will output:

```text
SELECT * FROM "table" WHERE id = :id
 
Array
(
    [:id] => 123
)
```

**Select with join**

```php
$builder = new SqlBuilder('pgsql');
$select = $builder->select()
    ->column('*')
    ->from('table')
    ->join($builder->join('table2', "table2.user_id = table.id")
    ->where('id = :id')
    ->bindParam(':id', 123);
 
echo $select;
print_r($select->getBindParams());
```

The above example will output:

```text
SELECT * FROM "table" JOIN "table2" ON table2.user_id = table.id WHERE id = :id
 
Array
(
    [:id] => 123
)
```
