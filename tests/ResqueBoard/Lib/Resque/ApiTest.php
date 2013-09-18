<?php
/**
 * Api Test file
 *
 * Test the Resque API class
 *
 * PHP version 5
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @package    ResqueBoard
 * @subpackage ResqueBoard.Test.Lib.Resque
 * @author     Wan Qi Chen <kami@kamisama.me>
 * @copyright  2012-2013 Wan Qi Chen
 * @link       http://resqueboard.kamisama.me
 * @since      2.0.0
 * @license    MIT License (http://www.opensource.org/licenses/mit-license.php)
 */

namespace ResqueBoard\Test\Lib\Resque;

# Include configuration files
require_once(dirname(__FILE__) . '/../../../../src/ResqueBoard/Config/Bootstrap.php');
require_once(dirname(__FILE__) . '/../../../../src/ResqueBoard/Config/Core.php');

/**
 * ApiTest Class
 *
 * Test the Resque API class
 *
 * @author Wan Qi Chen <kami@kamisama.me>
 */
class ApiTest extends \PHPUnit_Framework_TestCase
{
    public function setUp()
    {
        $this->mock = $this->getMock('ResqueBoard\Lib\Resque\Api', array('sendSignal'), array(), '', false);
        $this->mock->expects($this->any())->method('sendSignal')->will($this->returnValue(true));
        $this->mock->ResqueStatus = $this->ResqueStatus = $this->getMock('\ResqueStatus\ResqueStatus', array(), array(new \stdClass()));

        $this->hostname = function_exists('gethostname') ? gethostname() : php_uname('n');

        $this->validWorkerId = $this->hostname . ':9999999:queue';
        $this->invalidWorkerId = $this->hostname . ':_78:queue';

        $this->workersList = array(
            '9999999' => array()
        );

        $this->getProcessIdReflection = new \ReflectionMethod('ResqueBoard\Lib\Resque\Api', 'getProcessId');
        $this->getProcessIdReflection->setAccessible(true);

        $this->sendSignalReflection = new \ReflectionMethod('ResqueBoard\Lib\Resque\Api', 'sendSignal');
        $this->sendSignalReflection->setAccessible(true);
    }

    public function testConstructor()
    {
        $shell = new \ResqueBoard\Lib\Resque\Api();
        $this->assertInstanceOf('\ResqueStatus\ResqueStatus', $shell->ResqueStatus);
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::stop
     */
    public function testStopWorker()
    {
        $this->ResqueStatus->expects($this->once())->method('getWorkers')->will($this->returnValue($this->workersList));
        $this->ResqueStatus->expects($this->once())->method('removeWorker')->with($this->equalTo('9999999'));
        $this->mock->expects($this->once())->method('sendSignal')->with($this->equalTo('9999999'), $this->equalTo('QUIT'));
        $this->assertTrue($this->mock->stop($this->validWorkerId));
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::pause
     */
    public function testPauseWorker()
    {
        $this->ResqueStatus->expects($this->once())->method('getWorkers')->will($this->returnValue($this->workersList));
        $this->ResqueStatus->expects($this->once())->method('getPausedWorker')->will($this->returnValue(array()));
        $this->mock->expects($this->once())->method('sendSignal')->with($this->equalTo('9999999'), $this->equalTo('-USR2'));
        $this->ResqueStatus->expects($this->once())->method('setPausedWorker')->with($this->equalTo($this->validWorkerId));
        $this->assertTrue($this->mock->pause($this->validWorkerId));
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::resume
     */
    public function testResumeWorker()
    {
        $this->ResqueStatus->expects($this->once())->method('getWorkers')->will($this->returnValue($this->workersList));
        $this->ResqueStatus->expects($this->once())->method('getPausedWorker')->will($this->returnValue(array($this->validWorkerId)));
        $this->mock->expects($this->once())->method('sendSignal')->with($this->equalTo('9999999'), $this->equalTo('-CONT'));
        $this->ResqueStatus->expects($this->once())->method('setPausedWorker')->with($this->equalTo($this->validWorkerId), $this->equalTo(false));
        $this->assertTrue($this->mock->resume($this->validWorkerId));
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::stop
     * @expectedException   ResqueBoard\Lib\Resque\InvalidWorkerNameException
     */
    public function testStopWorkerWithInvalidWorkerId()
    {
        $this->ResqueStatus->expects($this->never())->method('getWorkers');
        $this->ResqueStatus->expects($this->never())->method('removeWorker');
        $this->mock->expects($this->never())->method('sendSignal');
        $this->mock->stop($this->invalidWorkerId);
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::stop
     * @expectedException   ResqueBoard\Lib\Resque\WorkerNotExistException
     */
    public function testStopWorkerWithInexistentWorker()
    {
        $this->ResqueStatus->expects($this->once())->method('getWorkers')->will($this->returnValue(array()));
        $this->ResqueStatus->expects($this->never())->method('removeWorker');
        $this->mock->expects($this->never())->method('sendSignal');
        $this->mock->stop($this->validWorkerId);
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::pause
     * @expectedException   ResqueBoard\Lib\Resque\InvalidWorkerNameException
     */
    public function testPauseWorkerWithInvalidWorkerId()
    {
        $this->ResqueStatus->expects($this->never())->method('getWorkers');
        $this->ResqueStatus->expects($this->never())->method('removeWorker');
        $this->mock->expects($this->never())->method('sendSignal');
        $this->mock->pause($this->invalidWorkerId);
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::pause
     * @expectedException   ResqueBoard\Lib\Resque\WorkerNotExistException
     */
    public function testPauseWorkerWithInexistentWorker()
    {
        $this->ResqueStatus->expects($this->once())->method('getWorkers')->will($this->returnValue(array()));
        $this->ResqueStatus->expects($this->never())->method('getPausedWorker');
        $this->mock->expects($this->never())->method('sendSignal');
        $this->mock->pause($this->validWorkerId);
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::pause
     * @expectedException   ResqueBoard\Lib\Resque\WorkerAlreadyPausedException
     */
    public function testPauseWorkerWithAlreadyPausedWorker()
    {
        $this->ResqueStatus->expects($this->once())->method('getWorkers')->will($this->returnValue($this->workersList));
        $this->ResqueStatus->expects($this->once())->method('getPausedWorker')->will($this->returnValue(array($this->validWorkerId)));
        $this->mock->expects($this->never())->method('sendSignal');
        $this->mock->pause($this->validWorkerId);
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::resume
     * @expectedException   ResqueBoard\Lib\Resque\WorkerNotPausedException
     */
    public function testResumeWorkerWithNotPausedWorker()
    {
        $this->ResqueStatus->expects($this->once())->method('getWorkers')->will($this->returnValue($this->workersList));
        $this->ResqueStatus->expects($this->once())->method('getPausedWorker')->will($this->returnValue(array()));
        $this->ResqueStatus->expects($this->never())->method('setPausedWorker');
        $this->mock->expects($this->never())->method('sendSignal');
        $this->mock->resume($this->validWorkerId);
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::resume
     * @expectedException   ResqueBoard\Lib\Resque\WorkerNotExistException
     */
    public function testResumeWorkerWithInexistentWorker()
    {
        $this->ResqueStatus->expects($this->once())->method('getWorkers')->will($this->returnValue(array()));
        $this->ResqueStatus->expects($this->never())->method('getPausedWorker');
        $this->mock->expects($this->never())->method('sendSignal');
        $this->mock->resume($this->validWorkerId);
    }


    /**
     * @covers ResqueBoard\Lib\Resque\Api::getProcessId
     */
    public function testGetProcessId()
    {
        $this->assertEquals('125', $this->getProcessIdReflection->invoke($this->mock, $this->hostname . ':125:queue'));
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::getProcessId
     * @expectedException   ResqueBoard\Lib\Resque\InvalidWorkerNameException
     */
    public function testGetProcessIdWithInvalidWorkerIdThatHasANumericButNegativeProcessId()
    {
        $this->getProcessIdReflection->invoke($this->mock, $this->hostname . ':-125');
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::getProcessId
     * @expectedException   ResqueBoard\Lib\Resque\InvalidWorkerNameException
     */
    public function testGetProcessIdWithInvalidWorkerIdThatHasMoreTokensThanExpected()
    {
        $this->getProcessIdReflection->invoke($this->mock, $this->hostname . ':125:as:25');
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::getProcessId
     * @expectedException   ResqueBoard\Lib\Resque\InvalidWorkerNameException
     */
    public function testGetProcessIdWithInvalidWorkerIdThatHasLessTokensThanExpected()
    {
        $this->getProcessIdReflection->invoke($this->mock, $this->hostname . ':125');
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::getProcessId
     * @expectedException   ResqueBoard\Lib\Resque\NotLocalWorkerException
     */
    public function testGetProcessIdWithNonLocalWorker()
    {
        $this->getProcessIdReflection->invoke($this->mock, 'invalidhost:125:queue');
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::sendSignal
     */
    public function testsendSignalKillANonExistentProcess()
    {
        $this->stringContains('kill', $this->sendSignalReflection->invoke($this->mock, '9999999', 'SIGTERM'));
    }

    /**
     * @covers ResqueBoard\Lib\Resque\Api::sendSignal
     */
    public function testsendSignalKillAProcessWithWrongPermission()
    {
        $this->stringContains('kill', $this->sendSignalReflection->invoke($this->mock, '1', 'SIGTERM'));
    }
}
