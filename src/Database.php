<?php

namespace GearmanWorkers;

class Database
{
  protected $pdo;

  public function __construct($connection)
  {
    try {
      $dsn = $this->getDSN($connection['database'], $connection['host']);
      $user = isset($connection['username']) ? $connection['username'] : $connection['user']; // username (drupal) or user (wordpress)
      $this->pdo = new \PDO($dsn, $user, $connection['password']);
    } catch (\PDOException $e) {
      echo 'PDO error: ' . $e->getMessage() . "\n";
    } catch (\Throwable $e) {
      echo 'Other error: ' . $e->getMessage() . "\n";
    }
  }

  public function getDSN($database, $host)
  {
    return "mysql:dbname={$database};host={$host}";
  }

  public function __call($method, $args = [])
  {
    return call_user_func_array([$this->pdo, $method], $args);
  }
}
