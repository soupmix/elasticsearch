<?php
namespace tests;

use Composer\Autoload\ClassLoader;

require_once dirname(__DIR__) . '/vendor/autoload.php';
$loader = new ClassLoader();
$loader->add('tests', dirname(__DIR__));
$loader->register();
