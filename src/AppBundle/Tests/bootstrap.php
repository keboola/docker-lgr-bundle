<?php

if (file_exists(__DIR__ . '/config.php')) {
    require_once __DIR__ . '/config.php';
}
defined('STORAGE_API_URL')
|| define('STORAGE_API_URL', getenv('STORAGE_API_URL') ? getenv('STORAGE_API_URL') : 'https://connection.keboola.com');
defined('STORAGE_API_TOKEN')
|| define('STORAGE_API_TOKEN', getenv('STORAGE_API_TOKEN') ? getenv('STORAGE_API_TOKEN') : 'your_token');

require_once __DIR__ . '/../vendor/autoload.php';
