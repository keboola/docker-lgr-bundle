<?php

namespace Keboola\DockerLGRBundle;

use Keboola\StorageApi\Config\Exception;

/**
 * Class TableReader extends StorageAPI reader to a bit more friendly in reading component
 *  configuration tables.
 *
 * @package Utility
 */
class TableReader extends \Keboola\StorageApi\Config\Reader
{
    /**
     * Read a component configuration table from SAPI.
     *
     * @param string $componentName Name of the running component.
     * @param string $tableName Name of the table.
     * @param string $token Storage API token.
     * @return array Associative array where index is configuration option and value is it's value.
     *     Array is nested so that configuration keys in form 'foo.bar' are accessible as $array['foo']['bar']
     */
    public static function readConfigTable($componentName, $tableName, $token)
    {
        $tableId = 'sys.c-'.$componentName.'.'.$tableName;
        return self::parse(self::keyValueMap(self::readTable($tableId, $token), 'name', 'value'));
    }


    /**
     * Read a table from SAPI. This is a more low-level method than {@see readConfigTable()}
     *
     * @param string $tableId Full name of the table including stage and bucket.
     * @param string $token Storage API token.
     * @throws Exception In case the configuration table does not exist.
     * @return array Table data, 1st index is row index, second index is column name.
     */
    public static function readTable($tableId, $token)
    {
        $client = new \Keboola\StorageApi\Client(array('token' => $token));
        if (!$client->tableExists($tableId)) {
            throw new Exception("Configuration table '$tableId' not found or not accessible.");
        }

        $csvData = $client->exportTable($tableId);
        if ($csvData) {
            $data = \Keboola\StorageApi\Client::parseCsv($csvData);
        } else {
            $data = array();
        }

        return $data;
    }


    /**
     * Map contents of a key-value table into a key-value array.
     *
     * @param array $data Data where 1st index is row index, 2nd index is column name
     * @param string $keyName Name of the column with key.
     * @param string $valueName Name of the column with value.
     * @return array Associative key-value array.
     */
    public static function keyValueMap($data, $keyName, $valueName)
    {
        $map = array();
        foreach ($data as $row) {
            $map[$row[$keyName]] = $row[$valueName];
        }
        return $map;
    }


    /**
     * Parse a key-value array into nested key-value array.
     *
     * @param array $data Associative key-value array
     * @return array associative nested array so that keys in form 'foo.bar' are accessible as $array['foo']['bar']
     */
    protected static function parse($data)
    {
        $ret = array();
        foreach ($data as $key => $value) {
            $newKey = array_reverse(explode('.', $key));
            self::saveKey($ret, $newKey, $value);
        }
        return $ret;
    }


    /**
     * Save key into a nested key-value array.
     *
     * @param array &$data Arbitrary array.
     * @param string[] $keys Array of keys.
     * @param mixed $value Arbitrary value.
     */
    private static function saveKey(&$data, $keys, $value)
    {
        $key = array_pop($keys);
        if ($key !== null) {
            if (!isset($data[$key])) {
                $data[$key] = array();
            }
            self::saveKey($data[$key], $keys, $value);
        } else {
            $data = $value;
        }
    }
}
