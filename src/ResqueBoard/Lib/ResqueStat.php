<?php
/**
 * ResqueStat classes
 *
 * Contains all classes required to fetch static datas
 * from php-resque and the cube server
 *
 * PHP version 5
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @author        Wan Qi Chen <kami@kamisama.me>
 * @copyright     Copyright 2012, Wan Qi Chen <kami@kamisama.me>
 * @link          http://resqueboard.kamisama.me
 * @package       resqueboard
 * @subpackage    resqueboard.lib
 * @since         1.0.0
 * @license       MIT License (http://www.opensource.org/licenses/mit-license.php)
 */

namespace ResqueBoard\Lib;

/**
 * ResqueStat class
 *
 * Connect to the backend to retrieve php-resque datas, or metrics
 *
 * @subpackage      resqueboard.lib
 * @since            1.0.0
 * @author           Wan Qi Chen <kami@kamisama.me>
 */
class ResqueStat
{
    protected $stats = array();
    protected $workers = array();
    protected $queues = array();

    const JOB_STATUS_WAITING = 1;
    const JOB_STATUS_RUNNING = 2;
    const JOB_STATUS_FAILED = 3;
    const JOB_STATUS_COMPLETE = 4;
    const JOB_STATUS_SCHEDULED = \ResqueScheduler\Job\Status::STATUS_SCHEDULED;

    const CUBE_STEP_10SEC = '1e4';
    const CUBE_STEP_1MIN = '6e4';
    const CUBE_STEP_5MIN = '3e5';
    const CUBE_STEP_1HOUR = '36e5';
    const CUBE_STEP_1DAY = '864e5';

    protected $settings;
    private $redis;
    private $mongo;

    /**
     * @since 1.0.0
     */
    public function __construct($settings = array())
    {
        $this->settings = array(
                'mongo' => array('host' => 'localhost', 'port' => 27017, 'database' => 'cube_development'),
                'redis' => array('host' => '127.0.0.1', 'port' => 6379, 'database' => 0),
                'cube'  => array('host' => 'localhost', 'port' => 1081),
                'resquePrefix' => 'resque'
        );

        $this->queues = array();

        $this->settings = array_merge($this->settings, $settings);
        $this->settings['resquePrefix'] = $this->settings['resquePrefix'] .':';

        $cube = $this->getMongo()->selectDB($this->settings['mongo']['database']);

        $thisQueues =& $this->queues;
        $this->workers = array_map(
            function ($name) use (&$thisQueues) {
                list($host, $process, $q) = explode(':', $name);
                $q = explode(',', $q);
                array_walk(
                    $q,
                    function ($qu) use (&$thisQueues) {
                        if (isset($thisQueues[$qu])) {
                            $thisQueues[$qu]['workers']++;
                        } else {
                            $thisQueues[$qu]['workers'] = 1;
                        }
                    }
                );
                return array('fullname' => $name, 'host' => $host, 'process' => $process, 'queues' => $q);
            },
            $this->getRedis()->smembers($this->settings['resquePrefix'] . 'workers')
        );

        // Assign all active queues as active
        array_walk(
            $this->queues,
            function (&$queue) {
                $queue['active'] = true;
            }
        );

        // Get all queues and compute complete list of active and inactive queues
        $allQueues = $this->getRedis()->smembers($this->settings['resquePrefix'] . 'queues');
        foreach ($allQueues as $queue) {
            if (!isset($this->queues[$queue])) {
                $this->queues[$queue] = array('workers' => 0, 'active' => false);
            }
        }

        // Populate queues pending jobs counter
        $redisPipeline = $this->getRedis()->multi(\Redis::PIPELINE);
        foreach ($this->queues as $name => $stats) {
            $redisPipeline->llen($this->settings['resquePrefix'] . 'queue:' . $name);
        }

        $result = $redisPipeline->exec();
        $i = 0;
        foreach ($this->queues as &$queue) {
            $queue['jobs'] = $result[$i];
            $i++;
        }

        foreach ($this->queues as $queueName => $queueStats) {
            if ($queueStats['workers'] === $queueStats['jobs'] && $queueStats['jobs'] === 0) {
                unset($this->queues[$queueName]);
            }
        }


        $redisPipeline = $this->getRedis()->multi(\Redis::PIPELINE);
        foreach ($this->workers as $worker) {
            $redisPipeline
            ->get($this->settings['resquePrefix'] . 'worker:' . $worker['fullname'] . ':started')
            ->get($this->settings['resquePrefix'] . 'stat:processed:' . $worker['fullname'])
            ->get($this->settings['resquePrefix'] . 'stat:failed:' . $worker['fullname']);
        }

        $result = $redisPipeline->exec();
        unset($redisPipeline);

        $this->stats['active'] = array('processed' => 0, 'failed' => 0);

        for ($i = 0, $total = count($result), $j = 0; $i < $total; $i += 3) {
            $this->workers[$j]['start'] = new \DateTime($result[$i]);
            $this->stats['active']['processed'] += $this->workers[$j]['processed'] = (int) $result[$i+1];
            $this->stats['active']['failed'] += $this->workers[$j++]['failed'] = (int) $result[$i+2];

        }

        unset($result);

        // Getting Scheduler Worker Stats
        $this->schedulerWorkers = array();
        for ($j = count($this->workers)-1; $j >=0; --$j) {
            if (implode('', $this->workers[$j]['queues']) === \ResqueScheduler\ResqueScheduler::QUEUE_NAME) {
                $this->workers[$j]['scheduled'] = \ResqueScheduler\ResqueScheduler::getDelayedQueueScheduleSize();
                $this->schedulerWorkers[] = $this->workers[$j];
                unset($this->workers[$j]);
                break;
            }
        }

        $this->stats['total'] = array_combine(
            array('processed', 'failed'),
            array_map(
                function ($s) {
                    return (int) $s;
                },
                $this->getRedis()->multi(\Redis::PIPELINE)
                    ->get($this->settings['resquePrefix'] . 'stat:processed')
                    ->get($this->settings['resquePrefix'] . 'stat:failed')
                    ->exec()
            )
        );
        $this->stats['total']['scheduled'] = \ResqueScheduler\Stat::get();

        $this->stats['total']['processed'] = max($this->stats['total']['processed'], $cube->selectCollection('got_events')->find()->count());
        $this->stats['total']['failed'] = max($this->stats['total']['failed'], $cube->selectCollection('fail_events')->find()->count());

        $this->setupIndexes();
    }


    /**
     * Return a mongo connection instance
     *
     * @since 1.0.0
     */
    protected function getMongo()
    {
        if ($this->mongo !== null) {
            return $this->mongo;
        }

        try {
            $this->mongo = new \Mongo($this->settings['mongo']['host'] . ':' . $this->settings['mongo']['port']);
            return $this->mongo;
        } catch (\MongoConnectionException $e) {
            throw new DatabaseConnectionException('Could not connect to Mongo Server');
        }
    }


    /**
     * Return a redis connection instance
     *
     * @since 1.0.0
     */
    protected function getRedis()
    {
        if ($this->redis !== null) {
            return $this->redis;
        }

        try {
            $this->redis = new \Redis();
            $this->redis->connect($this->settings['redis']['host'], $this->settings['redis']['port']);
            $this->redis->select($this->settings['redis']['database']);
            return $this->redis;
        } catch (\RedisException $e) {
            throw new DatabaseConnectionException('Could not connect to Redis Server');
        }
    }


    /**
     * Return count of each jobs status
     *
     * @since 1.0.0
     * @return array indexed by job status if more than one job status requested, else int
     */
    public function getStats($type = null)
    {
        $stats = array();
        $validType = array(
            self::JOB_STATUS_FAILED => 'fail',
            self::JOB_STATUS_COMPLETE => 'done',
            self::JOB_STATUS_SCHEDULED => 'movescheduled'
        );

        $cube = $this->getMongo()->selectDB($this->settings['mongo']['database']);

        if ($type === null) {
            $stats = array_combine(array_keys($validType), array_fill(0, count($validType), 0));
        } elseif (array_key_exists($type, $validType)) {
            $stats[$type] = 0;
        } else {
            return false;
        }

        foreach ($stats as $key => $value) {
            $stats[$key] = $cube->selectCollection($validType[$key] . '_events')->count();
        }

        return $type === null ? $stats : $stats[$type];
    }


    /**
     * Return active workers stats
     *
     * @since 1.0.0
     * @return multitype:
     */
    public function getWorkers()
    {
        return $this->workers;
    }

    /**
     * Return active workers stats
     *
     * @since 1.0.0
     * @return multitype:
     */
    public function getSchedulerWorkers()
    {
        return $this->schedulerWorkers;
    }


    /**
     * Return a worker stats
     *
     * @since 1.2.0
     */
    public function getWorker($workerId)
    {
        $cube = $this->getMongo()->selectDB($this->settings['mongo']['database']);
        $collection = $cube->selectCollection('start_events');
        $workerStatsMongo = $collection->findOne(array('d.worker' => $workerId), array('d.queues'));

        $workerFullName = $workerId . ':' . implode(',', $workerStatsMongo['d']['queues']);
        list($host, $process) = explode(':', $workerId, 2);

        return array(
            'fullname' => $workerFullName,
            'host' => $host,
            'process' => $process,
            'queues' => $workerStatsMongo['d']['queues'],
            'start' => new \DateTime($this->getRedis()->get($this->settings['resquePrefix'] . 'worker:' . $workerFullName . ':started')),
            'processed' => (int)$this->getRedis()->get($this->settings['resquePrefix'] . 'stat:processed:' . $workerFullName),
            'failed' => (int)$this->getRedis()->get($this->settings['resquePrefix'] . 'stat:failed:' . $workerFullName)
        );
    }


    /**
     * Return list of all known queues
     *
     * @since 1.4.0
     */
    public function getAllQueues()
    {
        return $this->getRedis()->smembers($this->settings['resquePrefix'] . 'queues');
    }

    /**
     * Return list of all queues
     *
     * @since 1.4.0
     */
    public function getQueues($fields = array(), $queues = array())
    {
        $cube = $this->getMongo()->selectDB($this->settings['mongo']['database']);
        $queuesCollection = $cube->selectCollection('got_events');

        if (in_array('totaljobs', $fields) || empty($queues)) {
            $lastRefresh = $cube
                ->selectCollection('map_reduce_stats')
                ->findOne(array('_id' => 'queue_stats'), array("date", "stats"));

            $now = new \MongoDate();

            $conditions = array();

            if (isset($lastRefresh['date'])) {
                $conditions['t'] = array('$gt' => $now);
            }

            $queues = $queuesCollection->distinct('d.args.queue', $conditions);

            $results = isset($lastRefresh['stats']) ? $lastRefresh['stats'] : array();
            foreach ($queues as $q) {
                $conditions['d.args.queue'] = $q;
                $results[$q] = $queuesCollection->count($conditions) + (isset($results[$q]) ? $results[$q] : 0);
            }

            $cube
                ->selectCollection('map_reduce_stats')
                ->save(
                    array(
                        '_id' => 'queue_stats',
                        'date' => $now,
                        'stats' => $results
                    )
                );

            // Format results for the API
            $r = array();
            foreach ($results as $name => $count) {
                $r[$name] = array(
                    'name' => $name,
                    'stats' => array(
                        'totaljobs' => $count,
                        'pendingjobs' => 0,
                        'workerscount' => 0
                    )
                );
            }
        } else {
            $r = array();
            foreach ($queues as $name) {
                $r[$name] = array(
                    'name' => $name,
                    'stats' => array(
                        'totaljobs' => 0,
                        'pendingjobs' => 0,
                        'workerscount' => 0
                    )
                );
            }
        }

        if (in_array('pendingjobs', $fields)) {
            $pipeline = $this->getRedis()->multi(\Redis::PIPELINE);
            foreach ($r as $name => $stats) {
                $keyName = $this->settings['resquePrefix'] . 'queue:' . $name;
                $pipeline->llen($keyName);
            }

            $count = $pipeline->exec();
            $i = 0;
            foreach ($r as $name => $stats) {
                $r[$name]['stats']['pendingjobs'] = $count[$i];
                $i++;
            }

        }

        if (in_array('workers', $fields)) {
            $workers = $this->getWorkers();

            foreach ($workers as $w) {
                foreach ($w['queues'] as $q) {
                    if (isset($r[$q])) {
                        $r[$q]['stats']['workerscount']++;
                    } else {
                        $r[$q] = array(
                            'name' => $q,
                            'stats' => array(
                                'totaljobs' => 0,
                                'pendingjobs' => 0,
                                'workerscount' => 1
                            )
                        );
                    }
                }
            }
        }

        return $r;
    }


    /**
     * Return jobs filtered by conditions specified in $options
     *
     * @since 1.1.0
     */
    public function getJobs($options = array())
    {

        $default = array(
                'workerId' => null,
                'jobId' => null,
                'page' => 1,
                'limit' => null,
                'sort' => array('t' => -1),
                'status' => null,
                'type' => 'find',
                'date_after' => null,
                'date_before' => null,
                'class' => null,
                'queue' => null,
                'worker' => array(),
                'format' => true
            );

        $options = array_merge($default, $options);

        if ($options['date_before'] !== null && !is_int($options['date_before'])) {
            $options['date_before'] = strtotime($options['date_before']);
        }

        if ($options['date_after'] !== null && !is_int($options['date_after'])) {
            $options['date_after'] = strtotime($options['date_after']);
        }

        if (isset($options['status']) && $options['status'] === self::JOB_STATUS_WAITING) {
            return $this->getPendingJobs($options);
        }

        $cube = $this->getMongo()->selectDB($this->settings['mongo']['database']);
        $jobsCollection = $cube->selectCollection('got_events');

        $conditions = array();

        if (!empty($options['jobId'])) {
            if (!is_array($options['jobId'])) {
                $options['jobId'] = array($options['jobId']);
            }
            $conditions['d.args.payload.id'] = array('$in' => $options['jobId']);
        } else {
            if ($options['workerId'] !== null) {
                $conditions['d.worker'] = $options['workerId'];
            }

            if (!empty($options['class'])) {
                $conditions['d.args.payload.class'] = array('$in' => array_map('trim', explode(',', $options['class'])));
            }

            if (!empty($options['queue'])) {
                $conditions['d.args.queue'] = array('$in' => array_map('trim', explode(',', $options['queue'])));
            }

            if (!empty($options['worker'])) {
                if (in_array('old', $options['worker'])) {
                    $workers = array_map(
                        function ($a) {
                            return $a['host'].':'.$a['process'];
                        },
                        $this->getWorkers()
                    );
                    $exclude = array_diff($workers, $options['worker']);

                    if (!empty($exclude)) {
                        $conditions['d.worker'] = array('$nin' => $exclude);
                    }
                } else {
                    $conditions['d.worker'] = array('$in' => $options['worker']);
                }
            }

            if ($options['status'] === self::JOB_STATUS_FAILED) {
                $cursor = $cube->selectCollection('fail_events')
                    ->find(array(), array('d.job_id'))
                    ->sort($options['sort'])
                    ->limit($options['limit']);
                $ids = array();
                foreach ($cursor as $c) {
                    $ids[] = $c['d']['job_id'];
                }
                $conditions['d.args.payload.id'] = array('$in' => $ids);
            }
        }

        if (!empty($options['date_after'])) {
            $conditions['t']['$gte'] = new \MongoDate($options['date_after']);
        }

        if (!empty($options['date_before'])) {
            $conditions['t']['$lte'] = new \MongoDate($options['date_before']);
        }

        $jobsCursor = $jobsCollection->find($conditions);
        $jobsCursor->sort($options['sort']);

        if (!empty($options['page']) && !empty($options['limit'])) {
            $jobsCursor->skip(($options['page']-1) * $options['limit'])->limit($options['limit']);
        }

        if ($options['type'] == 'count') {
            return $jobsCursor->count();
        }

        $results = $this->formatJobs($jobsCursor);
        if ($options['format']) {
            return $this->setJobStatus($results);
        }

        return $results;
    }

    /**
     *
     *
     * @since 2.0.0
     */
    public function getJobsCount($options = array())
    {

        $default = array(
                'workerId' => null,
                'status' => null,
                'type' => 'find',
                'date_after' => null,
                'date_before' => null,
                'class' => null,
                'queue' => null,
                'worker' => array(),
                'format' => true
            );

        $options = array_merge($default, $options);

        if ($options['date_before'] !== null && !is_int($options['date_before'])) {
            $options['date_before'] = strtotime($options['date_before']);
        }

        if ($options['date_after'] !== null && !is_int($options['date_after'])) {
            $options['date_after'] = strtotime($options['date_after']);
        }

        $cube = $this->getMongo()->selectDB($this->settings['mongo']['database']);


        $conditions = array();

        if (!empty($options['date_after'])) {
            $conditions['t']['$gte'] = new \MongoDate($options['date_after']);
        }

        if (!empty($options['date_before'])) {
            $conditions['t']['$lt'] = new \MongoDate($options['date_before']);
        }

        $results = array();

        $jobsDoneCollection = $cube->selectCollection('done_events');
        $jobsFailCollection = $cube->selectCollection('fail_events');

        $jobsDoneCursor = $jobsDoneCollection->find($conditions, array('t' => true));
        foreach ($jobsDoneCursor as $job) {
            if (isset($results[$job['t']->sec])) {
                $results[$job['t']->sec] += 1;
            } else {
                $results[$job['t']->sec] = 1;
            }
        }

        $jobsFailCursor = $jobsFailCollection->find($conditions, array('t' => true));
        foreach ($jobsFailCursor as $job) {
            if (isset($results[$job['t']->sec])) {
                $results[$job['t']->sec] += 1;
            } else {
                $results[$job['t']->sec] = 1;
            }
        }

        return $results;
    }

    /**
     * Return a list of pending jobs
     *
     * @since 1.4.0
     * @return Array an Array of jobs
     */
    protected function getPendingJobs($options)
    {

        $queuesList = empty($options['queue']) ? $this->getAllQueues() : array($options['queue']);
        $jobs = array();
        $queues = array();
        foreach ($queuesList as $queueName) {
            $keyName = $this->settings['resquePrefix'] . 'queue:' . $queueName;
            $limit = $options['limit'] === null ? $this->getRedis()->llen($keyName)-1 : $options['limit'];
            $queues[$queueName] = $this->getRedis()->lrange($keyName, 0, $limit);
        }


        foreach ($queues as $queue => $jobs) {
            for ($i = count($jobs)-1; $i >= 0; $i--) {
                $jobs[$i] = json_decode($jobs[$i], true);
                $jobs[$i] = array(
                    'd' => array(
                        'args' => array(
                            'queue' => $queue,
                            'payload' => array(
                                'class' => $jobs[$i]['class'],
                                'id' => $jobs[$i]['id'],
                                'args' => $jobs[$i]['args']
                                )
                            )
                        )
                    );
            }
        }

        $jobs = $this->formatJobs($jobs);
        $pendingStatus = self::JOB_STATUS_WAITING;
        array_walk(
            $jobs,
            function (&$j) use ($pendingStatus){
                $j['status'] = $pendingStatus;
            }
        );
        return $jobs;
    }

    /**
     * Return the number of pending jobs in a queue
     * @param  String $queue    Name of the queue, or null to get the count of pending jobs from all queues
     * @return array            Queue name indexed array of jobs count
     */
    public function getPendingJobsCount($queue = null)
    {
        $queuesList = $queue === null ? $this->getAllQueues() : array($queue);
        $pipeline = $this->getRedis()->multi(\Redis::PIPELINE);
        foreach ($queuesList as $queueName) {
            $pipeline->llen($this->settings['resquePrefix'] . 'queue:' . $queueName);
        }

        return array_combine($queuesList, $pipeline->exec());
    }




    /**
     * Return logs filtered by conditions specified in $options
     *
     * @since 1.2.0
     */
    public function getLogs($options = array())
    {
        $cube = $this->getMongo()->selectDB($this->settings['mongo']['database']);

        $eventTypeList = array('check' ,'done', 'fail', 'fork', 'found', 'got', 'kill', 'process', 'prune', 'reconnect', 'shutdown', 'sleep', 'start');

        $default = array(
            'page' => 1,
            'limit' => null,
            'sort' => array('t' => 1),
            'event_level' => array(),
            'event_type' => '',
            'date_after' => null,
            'date_before' => null,
            'type' => 'find'
        );


        $options = array_merge($default, $options);


        if ($options['date_before'] !== null && !is_int($options['date_before'])) {
            $options['date_before'] = strtotime($options['date_before']);
        }

        if ($options['date_after'] !== null && !is_int($options['date_after'])) {
            $options['date_after'] = strtotime($options['date_after']);
        }

        $conditions = array();


        if (!empty($options['event_level'])) {
            $conditions['d.level'] = array(
                '$in' => array_map(
                    function ($level) {
                        return (int)$level;
                    },
                    $options['event_level']
                )
            );
        }

        if (!empty($options['date_after'])) {
            $conditions['t']['$gte'] = new \MongoDate($options['date_after']);
        }

        if (!empty($options['date_before'])) {
            $conditions['t']['$lt'] = new \MongoDate($options['date_before']);
        }

        $results = array();
        $pageCount = array();


        $jobsCollection = $cube->selectCollection($options['event_type'] . '_events');

        $jobsCursor = $jobsCollection->find($conditions);
        $jobsCursor->sort($options['sort']);

        if (!empty($options['page']) && !empty($options['limit'])) {
            $jobsCursor->skip(($options['page']-1) * $options['limit'])->limit($options['limit']);
        }

        if ($options['type'] == 'count') {
            return $jobsCursor->count();
        }

        foreach ($jobsCursor as $cursor) {
            $temp = array();

            $temp['date'] = new \DateTime('@' . $cursor['t']->sec);

            if (isset($cursor['d']['worker'])) {
                $temp['worker'] = $cursor['d']['worker'];
            }

            if (isset($cursor['d']['level'])) {
                $temp['level'] = $cursor['d']['level'];
            }

            if (isset($cursor['d']['job_id'])) {
                $temp['job_id'] = $cursor['d']['job_id'];
            } else if (isset($cursor['d']['args']['payload']['id'])) {
                $temp['job_id'] = $cursor['d']['args']['payload']['id'];
            }

            $temp['event_type'] = $options['event_type'];

            $results[] = $temp;
        }


        usort(
            $results,
            function (
                $a,
                $b
            ) {
                if ($a['date'] == $b['date']) {
                    return 0;
                }
                return ($a['date'] < $b['date']) ? -1 : 1;
            }
        );

        return $results;
    }

    /**
     * Get the number of jobs processed for a period of time
     *
     * Get the number of jobs processed between a $start and an $end date,
     * divided into $step interval
     *
     * @since  1.3.0
     * @param  DateTime $start Start date
     * @param  DateTime $end   End date
     * @param  const    $step  Cube constant for step
     * @throws Exception       If curl extension is not installed
     * @throws Exception       If unable to init the curl session
     * @throws Exception       If cube does not return a valid response
     * @return array
     */
    public function getJobsMatrix($start, $end, $step)
    {
        if (!extension_loaded('curl')) {
            throw new \Exception('The curl extension is needed');
        }

        $link = 'http://'.$this->settings['cube']['host'] . ':' .
            $this->settings['cube']['port'].'/1.0/metric?expression=sum(got)&start=' . urlencode($start->format('Y-m-d\TH:i:sO')) .
            '&stop=' . urlencode($end->format('Y-m-d\TH:i:sO')) . '&step=36e5';

        $this->httpConnection = curl_init($link);

        if (!$this->httpConnection) {
            throw new \Exception('Unable to connect to ' . $this->settings['cube']['host'] . ':' . $this->settings['cube']['port']);
        }

        curl_setopt($this->httpConnection, CURLOPT_RETURNTRANSFER, true);
        curl_setopt(
            $this->httpConnection,
            CURLOPT_HTTPHEADER,
            array(  'Content-Type: application/json')
        );

        $response = curl_exec($this->httpConnection);
        if (!$response) {
            throw new \Exception('Unable to connect to Cube server');
        }

        return json_decode($response, true);
    }

    /**
     * Return the distribution of jobs by classes
     *
     * @since 1.1.0
     * @param int $limit Number of results to return, null to return all results
     */
    public function getJobsRepartionStats($limit = 10)
    {
        $cube = $this->getMongo()->selectDB($this->settings['mongo']['database']);
        $mapReduceStats = new \MongoCollection($cube, 'map_reduce_stats');
        $startDate = $mapReduceStats->findOne(array('_id' => 'job_stats'), array('date'));
        if (!isset($startDate['date']) || empty($startDate['date'])) {
            $startDate = null;
        } else {
            $startDate = $startDate['date'];
        }

        $stopDate = new \MongoDate();

        $mapReduceStats->update(
            array('_id' => 'job_stats'),
            array('$set' => array('date' => $stopDate)),
            array('upsert' => true)
        );

        $stats = new \stdClass();

        // Computing total jobs distribution stats
        $map = new \MongoCode("function() {emit(this.d.args.payload.class, 1); }");
        $reduce = new \MongoCode(
            "function(key, val) {".
            "var sum = 0;".
            "for (var i in val) {".
            "sum += val[i];".
            "}".
            "return sum;".
            "}"
        );

        $conditions = array('$lt' => $stopDate);
        if ($startDate != null) {
            $conditions['$gte'] = $startDate;
        }
        $cube->command(
            array(
                'mapreduce' => 'got_events',
                'map' => $map,
                'reduce' => $reduce,
                'query' => array('t' => $conditions),
                'out' => array('merge' => 'jobs_repartition_stats')
            )
        );

        $cursor = $cube->selectCollection('jobs_repartition_stats')->find()->sort(array('value' => -1))->limit($limit);

        $stats->total = $cube->selectCollection('got_events')->find()->count();
        $stats->stats = array();
        foreach ($cursor as $c) {
            $c['percentage'] = round($c['value'] / $stats->total * 100, 2);
            $stats->stats[] = $c;
        }

        return $stats;
    }


    /**
     * Get general jobs statistics, by status
     *
     * @since 1.1.0
     * @return array An array of jobs count, by status
     */
    public function getJobsStats($options = array())
    {
        $cube = $this->getMongo()->selectDB($this->settings['mongo']['database']);

        $filter = array();

        if (isset($options['start'])) {
            $filter['t']['$gte'] = new \MongoDate(strtotime($options['start']));
        }

        if (isset($options['end'])) {
            $filter['t']['$lt'] = new \MongoDate(strtotime($options['end']));
        }

        $stats = new \stdClass();
        $stats->total = $cube->selectCollection('got_events')->find($filter)->count();
        $stats->count[self::JOB_STATUS_COMPLETE] = $cube->selectCollection('done_events')->find($filter)->count();
        $stats->count[self::JOB_STATUS_FAILED] = $cube->selectCollection('fail_events')->find($filter)->count();
        $stats->perc[self::JOB_STATUS_FAILED] =
            ($stats->total == 0)
                ? 0
                : round($stats->count[self::JOB_STATUS_FAILED] / $stats->total * 100, 2);
        $stats->count[self::JOB_STATUS_WAITING] = 0;
        $stats->count[self::JOB_STATUS_SCHEDULED] = $cube->selectCollection('movescheduled_events')->find($filter)->count();
        $stats->perc[self::JOB_STATUS_SCHEDULED] =
            ($stats->total == 0)
                ? 0
                : round($stats->count[self::JOB_STATUS_SCHEDULED] / $stats->total * 100, 2);

        $queues = $this->getAllQueues();
        $redisPipeline = $this->getRedis()->multi(\Redis::PIPELINE);
        foreach ($queues as $queueName) {
            $redisPipeline->llen($this->settings['resquePrefix'] . 'queue:' . $queueName);
        }
        $stats->count[self::JOB_STATUS_WAITING] = array_sum($redisPipeline->exec());

        $stats->count[self::JOB_STATUS_RUNNING] = 0; // TODO
        $stats->total_active = $this->stats['active']['processed'];

        $stats->oldest = null;
        $stats->newest = null;

        $cursors = $cube->selectCollection('got_events')->find(array(), array('t'))->sort(array('t' => 1))->limit(1);
        foreach ($cursors as $cursor) {
            $stats->oldest = new \DateTime('@'.$cursor['t']->sec);
        }

        $cursors = $cube->selectCollection('got_events')->find(array(), array('t'))->sort(array('t' => -1))->limit(1);
        foreach ($cursors as $cursor) {
            $stats->newest = new \DateTime('@'.$cursor['t']->sec);
        }


        return $stats;
    }


    /**
     * Convert jobs document from MongoDB or Redis entry to a formatted array
     *
     * @param A traversable object
     * @return an array of jobs
     */
    private function formatJobs($cursor)
    {
        $jobs = array();
        foreach ($cursor as $doc) {
            $jobs[$doc['d']['args']['payload']['id']] = array(
                            'time' => isset($doc['t']) ? date('c', $doc['t']->sec) : null,
                            'queue' => $doc['d']['args']['queue'],
                            'worker' => isset($doc['d']['worker']) ? $doc['d']['worker'] : null,
                            'level' => isset($doc['d']['level']) ? $doc['d']['level'] : null,
                            'class' => $doc['d']['args']['payload']['class'],
                            'args' => var_export($doc['d']['args']['payload']['args'][0], true),
                            'job_id' => $doc['d']['args']['payload']['id']

            );


            $jobs[$doc['d']['args']['payload']['id']]['took'] = isset($doc['d']['time']) ? $doc['d']['time'] : null;

        }

        return $jobs;
    }


    /**
     * Assign a status to each jobs
     *
     * @since 1.0.0
     * @param array $jobs An array of jobs
     * @return An array of jobs
     */
    private function setJobStatus($jobs)
    {
        $jobIds = array_keys($jobs);

        $cube = $this->getMongo()->selectDB($this->settings['mongo']['database']);


        $jobsCursor = $cube->selectCollection('done_events')->find(array('d.job_id' => array('$in' => $jobIds)));
        foreach ($jobsCursor as $successJob) {
            $jobs[$successJob['d']['job_id']]['status'] = self::JOB_STATUS_COMPLETE;
            $jobs[$successJob['d']['job_id']]['took'] = $successJob['d']['time'];
            unset($jobIds[array_search($successJob['d']['job_id'], $jobIds)]);
        }

        if (!empty($jobIds)) {
            $redisPipeline = $this->getRedis()->multi(\Redis::PIPELINE);

            $jobsCursor = $cube->selectCollection('fail_events')->find(array('d.job_id' => array('$in' => $jobIds)));
            foreach ($jobsCursor as $failedJob) {
                $jobs[$failedJob['d']['job_id']]['status'] = self::JOB_STATUS_FAILED;
                $jobs[$failedJob['d']['job_id']]['log'] = $failedJob['d']['log'];
                $jobs[$failedJob['d']['job_id']]['took'] = $failedJob['d']['time'];
                $redisPipeline->get($this->settings['resquePrefix'] . 'failed:' . $failedJob['d']['job_id']);
                unset($jobIds[array_search($failedJob['d']['job_id'], $jobIds)]);
            }

            $failedTrace = array_filter($redisPipeline->exec());

            foreach ($failedTrace as $trace) {
                $trace = unserialize($trace);
                $jobs[$trace['payload']['id']]['trace'] = var_export($trace, true);
            }

        }

        if (!empty($jobIds)) {
            $jobsCursor = $cube->selectCollection('process_events')->find(array('d.job_id' => array('$in' => $jobIds)));
            foreach ($jobsCursor as $processJob) {
                $jobs[$processJob['d']['job_id']]['status'] = self::JOB_STATUS_RUNNING;
                unset($jobIds[array_search($processJob['d']['job_id'], $jobIds)]);
            }
        }
        if (!empty($jobIds)) {
            foreach ($jobIds as $id) {
                $jobs[$id]['status'] = self::JOB_STATUS_WAITING;
            }
        }

        return array_values($jobs);
    }

    public function getCubeMetric($options)
    {
        if (!extension_loaded('curl')) {
            throw new \Exception('The curl extension is needed to use http URLs with the CubeHandler');
        }

        $string = 'http://'.$this->settings['cube']['host'] . ':' .
            $this->settings['cube']['port'].'/1.0/metric?expression=' . $options['expression'] .
            '&start=' . urlencode($options['start']->format('c')) .
            (isset($options['end']) ? ('&stop=' . urlencode($options['end']->format('c'))) : '') .
            (isset($options['step']) ? ('&step=' . $options['step']) : '') .
            (isset($options['limit']) ? ('&limit=' . $options['limit']) : '');

        $this->httpConnection = curl_init($string);

        if (!$this->httpConnection) {
            throw new \Exception('Unable to connect to ' . $this->settings['cube']['host'] . ':' . $this->settings['cube']['port']);
        }

        curl_setopt($this->httpConnection, CURLOPT_TIMEOUT, 5);
        curl_setopt($this->httpConnection, CURLOPT_RETURNTRANSFER, true);
        curl_setopt(
            $this->httpConnection,
            CURLOPT_HTTPHEADER,
            array(  'Content-Type: application/json')
        );

        $response = json_decode(curl_exec($this->httpConnection), true);

        $responseCode = curl_getinfo($this->httpConnection, CURLINFO_HTTP_CODE);

        if ($responseCode === 404) {
            throw new \Exception('Unable to connect to ' . $this->settings['cube']['host'] . ':' . $this->settings['cube']['port']);
        } else if ($responseCode !== 200) {
            throw new \Exception('Cube server return an error : ' . $response['error']);
        }

        return $response;
    }

    /**
     * Create Mongo Collection index
     *
     * @since 1.1.0
     */
    private function setupIndexes()
    {
        $cube = $this->getMongo()->selectDB($this->settings['mongo']['database']);
        $cube->selectCollection('got_events')->ensureIndex('d.args.queue');
        $cube->selectCollection('got_events')->ensureIndex('d.args.payload.class');
        $cube->selectCollection('got_events')->ensureIndex('d.worker');
        $cube->selectCollection('fail_events')->ensureIndex('d.job_id');
        $cube->selectCollection('done_events')->ensureIndex('d.job_id');
        $cube->selectCollection('shutdown_events')->ensureIndex('d.worker');
        $cube->selectCollection('start_events')->ensureIndex('d.worker');
    }
}
