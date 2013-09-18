<?php
/**
 * Redis service class
 *
 * PHP version 5
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @package    ResqueBoard
 * @subpackage ResqueBoard.Lib.Service
 * @author     Wan Qi Chen <kami@kamisama.me>
 * @copyright  2012-2013 Wan Qi Chen
 * @link       http://resqueboard.kamisama.me
 * @since      2.0.0
 * @license    MIT License (http://www.opensource.org/licenses/mit-license.php)
 */

namespace ResqueBoard\Lib\Service;

/**
 * Redis service class
 *
 * @subpackage ResqueBoard.Lib.Service
 * @author     Wan Qi Chen <kami@kamisama.me>
 * @since      2.0.0
 */
class Redis extends AbstractService
{
    public static $instance = null;

    public function __construct($settings)
    {
        parent::bootstrap();

        $t = microtime(true);
        $redis = new \Redis();

        try {
            $redis->connect($settings['host'], $settings['port']);

            if (isset($settings['password']) && !empty($settings['password'])) {
                if ($redis->auth($settings['password']) === false) {
                    throw new \Exception('Unable to authenticate with redis!');
                }
            }
        } catch (\Exception $e) {
            throw new \Exception('Unable to connect to Redis server');
        }

        if (isset($settings['prefix'])) {
            $redis->setOption(\Redis::OPT_PREFIX, $settings['prefix'] . ':');
        }

        parent::$serviceInstance[get_class()] = $redis;

        $queryTime = round((microtime(true) - $t) * 1000, 2);
        self::logQuery(
            array(
                'command' => 'CONNECTION to ' . $settings['host'] . ':' . $settings['port'],
                'time' => $queryTime
            )
        );
        self::$_totalTime[get_class()] += $queryTime;
        self::$_totalQueries[get_class()]++;
    }

    public static function init($settings)
    {
        if (self::$instance === null) {
            self::$instance = new Redis($settings);
        }

        return self::$instance;
    }

    public function pipeline($commands, $type = \Redis::PIPELINE)
    {
        $t = microtime(true);
        $redisPipeline = parent::$serviceInstance[get_class()]->multi($type);
        foreach ($commands as $command) {
            call_user_func_array(array($redisPipeline, $command[0]), (array)$command[1]);
        }
        $results = $redisPipeline->exec();
        $queryTime = round((microtime(true) - $t) * 1000, 2);
        self::logQuery(
            array(
                'command' => array('PIPE' => $commands),
                'time' => $queryTime
            )
        );
        self::$_totalTime[get_class()] += $queryTime;
        self::$_totalQueries[get_class()]++;

        return $results;
    }
}
