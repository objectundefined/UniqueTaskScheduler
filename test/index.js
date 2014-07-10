var assert = require('assert');
var UTQ = require('../lib/UniqueTaskQueue');
var redis_client_pub = require('redis').createClient();
var redis_client_sub_1 = require('redis').createClient();
var redis_client_sub_2 = require('redis').createClient();
var utq_pub = new UTQ( 'test_queue', redis_client_pub );
var utq_sub_1 = new UTQ( 'test_queue', redis_client_sub_1 );
var utq_sub_2 = new UTQ( 'test_queue', redis_client_sub_1 );

describe('UniqueTaskQueue', function(){
  var t1 = Date.now();
  var t2 = Date.now() + 1000;
  describe('#schedule(unique_task_id, date, data, cb)', function(){
    it('it should schedule without error', function(done){
      utq_pub.schedule('task-once', t1, {exec_after:t1, num:1}, done);
    })
    it('it should reschedule without error', function(done){
      utq_pub.schedule('task-once', t2, {exec_after:t2, num:2}, done);
    });
  })
  describe('#createConsumer()', function(){
    it('it should consume the correct number of tasks, given acks, rescheduled-just-once, and multiple consumers', function(done){
      var task_cts = {
        'task-once':0,
        'task-retry':0,
        'task-other-1':0,
        'task-other-2':0,
      };
      
      utq_pub.schedule('task-retry',Date.now());
      utq_pub.schedule('task-other-1',Date.now());
      utq_pub.schedule('task-other-2',Date.now());
      
      utq_sub_1.createConsumer().on('task', onTask).start();
      utq_sub_2.createConsumer().on('task', onTask).start();
      
      setTimeout(function(){
        assert.equal(task_cts['task-once'], 1, 'task-once should have been called once.')
        assert.equal(task_cts['task-retry'], 2, 'task-retry should have been called twice.')
        assert.equal(task_cts['task-other-1'], 1, 'task-other-1 should have been called once.')
        assert.equal(task_cts['task-other-2'], 1, 'task-other-2 should have been called once.')
        done();
      },1500);
      
      function onTask (task_id, task_data, ack_fn) {
        task_cts[task_id] += 1;
        if ('task-once' == task_id) {
          var should_ack = task_data.num == 2 && Date.now() >= task_data.exec_after ;
          ack_fn(should_ack);
        } else if ('task-retry' == task_id) {
          var should_ack = task_cts['task-retry'] > 1; // do not ack the first time
          ack_fn(should_ack);
        } else {
          ack_fn(true); // just ack others to ensure just-once delivery
        }
      }
      
    });
  });
});