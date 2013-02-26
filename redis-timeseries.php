<?php

class RedisTimeseries
{
    function __construct($redis, $prefix = 'default', $timestep = 1, $log = false) {
        $this->redis = $redis;
        $this->prefix = $prefix;
        $this->timestep = $timestep;
        $this->log = $log;
    }

    function out($message) {
        if ($this->log) {
            echo $message . PHP_EOL;
        }
    }

    function normalize($time) {
        return round($time / $this->timestep) * $this->timestep;
    }

    function getKey($time) {
        return "ts:" . $this->prefix . ":" . $this->normalize($time);
    }

    function encode($data) {
        if (strpos($data, "\x00") || strpos($data, "\x01")) {
            return 'E' . base64_encode($data);
        }

        return 'R' . $data;
    }

    function decode($data) {
        if ($data[0] == 'E') {
            return base64_decode(substr($data, 1));
        }

        return substr($data, 1);
    }

    function add($data, $origin = null) {
        $now = microtime(true);
        $this->out('Add ' . $now);

        $value = $now . "\x01" . $this->encode($data);

        if ($origin) {
            $value .= "\x01" . $this->encode($origin);
        }

        $this->redis->append($this->getKey($now), $value .= "\x00");

        return $value;
    }

    function record($record) {
        $parts = explode("\x01", $record);

        if (empty($parts[1])) {
            return false;
        }

        return array(
            'time'   => $parts[0],
            'data'   => $this->decode($parts[1]),
            'origin' => (isset($parts[2])) ? $this->decode($parts[2]) : null,
        );
    }

    function seek($time) {
        $bestStart = null;
        $bestTime = null;
        $rangelen = 64;

        $key = $this->getKey($time);
        $len = $this->redis->strlen($key);

        $this->out('Seek ' . $key . ' of length ' . $len);

        if ($len == 0) {
            return 0;
        }

        $min = 0;
        $max = $len - 1;

        while (true) {
            $p = floor($min + (($max - $min) / 2));
            $this->out('Seek p ' . $p);

            while (true) {
                $rangeEnd = $p + $rangelen - 1;

                if ($rangeEnd > $len) {
                    $rangeEnd = $len;
                }

                $r = $this->redis->getrange($key, $p, $rangeEnd);

                $sep = -1;
                $sep2 = false;

                if ($p != 0) {
                    $sep = strpos($r, "\x00");
                }

                $sep2 = strpos($r, "\x00", $sep + 1);
                $this->out('Sep2 from ' . $sep . ' to ' . $sep2 . ' of ' . $r);

                if ($sep2) {
                    $record = substr($r, $sep + 1, $sep2 - $sep - 1);

                    $recordStart = $p + $sep + 1;
                    $recordEnd = $p + $sep2 - 1;

                    $dr = $this->record($record);

                    if ($dr['time'] >= $time) {
                        if (! $bestTime || $bestTime > $dr['time']) {
                            $bestStart = $recordStart;
                            $bestTime = $dr['time'];
                        }
                    }

                    if ($max - $min == 1) {
                        return $bestStart;
                    }

                    break;
                }

                if ($rangeEnd == $len) {
                    return $len + 1;
                }

                $rangelen *= 2;
            }

            $this->out(json_encode($dr));

            $this->out('Seek | ' . $p .  ' | ' . $max . ' | ' . $min);

            if ($dr['time'] == $time) {
                return $recordStart;
            }

            if ($dr['time'] > $time) {
                $this->out('Seek one ' . $dr['time'] . ' > ' . $time);
                $max = $p;
            } else {
                $this->out('Seek two ' . $dr['time'] . ' < ' . $time);
                $min = $p;
            }

            $this->out('Seek | ' . $p .  ' | ' . $max . ' | ' . $min);
        }
    }

    function result($key, $begin, $end) {
        $len = $this->redis->strlen($key);
        $this->out('Result from ' . $key . ' for ' . $begin . ' to ' . $end . ' of ' . $len);

        $result = array();
        if ($range = $this->redis->getrange($key, $begin, $end)) {
            foreach (explode("\x00", $range) as $record) {
                if (trim($record)) {
                    $result[] = $this->record($record);
                }
            }
        }

        return $result;
    }

    function range($begin, $end) {
        $beginKey = $this->getKey($begin);
        $beginOff = $this->seek($begin);

        $endKey = $this->getKey($end);
        $endOff = $this->seek($end);

        $this->out('Range begin ' . $beginKey . ' up to ' . $beginOff);
        $this->out('Range end ' . $endKey . ' up to ' . $endOff);

        if ($beginKey == $endKey) {
            $this->out('Range end');
            return $this->result($beginKey, $beginOff, $endOff - 1);
        }

        $result = $this->result($beginKey, $beginOff, -1);
        $time = $this->normalize($begin);

        while (true) {
            $time += $this->timestep;
            $key = $this->getKey($time);

            $add = $this->result($key, 0, $endOff - 1);
            foreach ($add as $toadd) {
                $result[] = $toadd;
            }

            if ($key == $endKey) {
                break;
            }
        }

        return $result;
    }

    function timestep($time) {
        return $this->result($this->getKey($time), 0, -1);
    }
}
