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
      utq_pub.schedule('task-1', t1, {val:1}, done);
    })
    it('it should reschedule without error', function(done){
      var exec_time_2 = Date.now() + 1000;
      utq_pub.schedule('task-1', t2, {val:2}, done);
    });
  })
  describe('#createConsumer()', function(){
    var consumer_1 = utq_sub_1.createConsumer();
    var consumer_2 = utq_sub_2.createConsumer();
    var times_called = 0;
    it('it should consume the task with updated data & timing', function(done){
      var onTask = function(task_id, task_data, ack_fn){
        if (task_id == 'task-1' && task_data.val==2 && Date.now() >= t2) {
          times_called++;
          ack_fn(true);
          done();
        } else {
          done(new Error('task emitted with wrong data'));
        }
      };
      consumer_1.on('task', onTask );
      consumer_2.on('task', onTask );
      consumer_1.start();
      consumer_2.start();
    })
    it( 'it should consume the uniq item only once,' +
        ' even with multiple consumers drawing from' +
        ' the queue using different connections', function(done){
      setTimeout(function(){
        if (times_called == 1) {
          done();
        } else {
          done(new Error('times_called is not 1.'))
        }
      }, 1000);
    });
  })
});