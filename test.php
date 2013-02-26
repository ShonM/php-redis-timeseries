<?php

require 'redis-timeseries.php';

$redis = new Redis;
$redis->connect('127.0.0.1');

$ts = new RedisTimeseries($redis, 'test', 1, false);
$now = microtime(true);

echo 'Adding data' . PHP_EOL;
for ($i = 0; $i < 31; $i++) {
    echo $i . ' ';
    $ts->add($i);
    usleep(100000);
}
echo PHP_EOL;

$begin = $now + 1;
$end = $now + 2;

echo 'Get range from ' . $begin . ' to ' . $end . PHP_EOL;
$range = $ts->range($begin, $end);
foreach ($range as $record) {
    echo 'Record time ' . @$record['time'] . ', data ' . @$record['data'] . PHP_EOL;
}

echo PHP_EOL;

echo 'Get a single timestamp near ' . $begin . PHP_EOL;
$timestep = $ts->timestep($begin);
foreach ($timestep as $record) {
    echo 'Record time ' . $record['time'] . ', data ' . $record['data'] . PHP_EOL;
}
