var _ = require('underscore');
var util = require('util');
var format = util.format;
var assert = require('assert');
var momentjs = require('moment');
var async = require('async');
var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;

module.exports = UniqueTaskQueue;

function UniqueTaskQueue ( queue_name, redis_client, opts ) {
  var self = this;
  var options = self.options = _.defaults(opts || {}, {
    keyspace: 'utq',
    ring_size: 100 // IMPORTANT - do not change once you start storing data
  });
  simpleAssert(
    queue_name && ( _.isString(queue_name) || _.isNumber(queue_name) ),
    "First argument must be a valid queue name, string or number"
  );
  simpleAssert(
    // duck-type checking in order to allow alt implementations of the redis client interface
    redis_client && redis_client.multi && redis_client.multi && redis_client.watch && redis_client.unwatch,
    "Second argument must be a valid redis client or implementation of the redis client interface, including multi, watch, and unwatch."
  );
  self.options.queuespace = format('%s:%s', options.keyspace, queue_name);
  self.redis_client = redis_client;

}

UniqueTaskQueue.prototype.schedule = function ( task_id, task_date, task_data_or_cb, cb_or_und ) {
  var self = this;
  var client = self.redis_client;
  var cb = (arguments.length > 3 ? cb_or_und : task_data_or_cb) || function(){}
  var task_data = arguments.length > 3 ? task_data_or_cb : undefined;
  var task;
  try {
    task = new Task(task_id, task_date, task_data);
  } catch (e) {
    return cb(e);
  }
  async.series([
    function (cb) {
      var serialized = JSON.stringify(task.data);
      var parent_hash_key = self._parentHashKey(task.id);
      if (typeof serialized !== 'undefined') {
        client.hset(parent_hash_key, task_id, serialized, cb);
      } else {
        cb();
      }
    },
    function (cb) {
      var parent_queue_key = self._parentQueueKey(task.id);
      client.zadd([ parent_queue_key, task.date.getTime(), task.id ], cb);
    }
  ], function(err){
    if (err) return cb(err);
    cb(null, task);
  })
  
}

UniqueTaskQueue.prototype.createConsumer = function () {
  return new Consumer(this);
}

UniqueTaskQueue.prototype._parentHashKey = function(key) {
  return format("%s:data:%s", this.options.queuespace, this._ringLoc(key));
}

UniqueTaskQueue.prototype._parentQueueKey = function(key) {
  return format("%s:queue:%s", this.options.queuespace, this._ringLoc(key));
}

UniqueTaskQueue.prototype._ringLoc = function(key){
  var ring_size = this.options.ring_size;
  var hash = crypto.createHash('sha1').update(key).digest("hex");
  var hashCut = hash.substring(0,4);
  var hashNum = parseInt(hashCut, 16);
  return hashNum%ring_size;
}

function Task ( task_id, task_date, task_data ) {
  var self = this;
  
  simpleAssert(
    task_id && task_date,
    "Function must be called with a valid and task_id (unique), task_date, and task_data (optional)\n\t" +
    "ex: new Task('email-user-1234', new Date('01/01/2015'), { message: 'come back soon' })"
  );
  simpleAssert(
    _.isString(task_id) || _.isNumber(task_id),
    "First argument must be a string or number uniquely defining this task."
  );
  simpleAssert(
    momentjs(task_date).isValid(),
    "Second argument must be a valid or parseabale Date;"
  );
  
  self.date = momentjs(task_date).toDate();
  self.id = task_id.toString();
  self.data = task_data;
}

util.inherits(Consumer, EventEmitter);

function Consumer(utq_inst) {
  EventEmitter.call(this);
  this._utq_inst = utq_inst;
  this._running = false;
  this._sources = _.range(utq_inst.options.ring_size).map(function(n){
    return {
      queue: format("%s:queue:%s", utq_inst.options.queuespace, n),
      data: format("%s:data:%s", utq_inst.options.queuespace, n)
    }
  });
}

Consumer.prototype.start = function() {
  this._running = true ;
  this._run();
}

Consumer.prototype.stop = function() {
  this._running = false ;
  this._run();
}

Consumer.prototype._run = function () {
  
  var self = this;
  var utq_inst = self._utq_inst;
  var client = utq_inst.redis_client;
  var sources = self._sources;
  // by forcing the client to lock only a small, random subset 
  // of internal queues at a time, we are able to avoid most
  // deadlocks associated with concurrent consumers
  var draw_pile = [];
  var sample_size = Math.ceil(utq_inst.options.ring_size/10);
  
  runIteration();
  
  function getSample () {
    if (draw_pile.length < sample_size) draw_pile = _.shuffle(sources);
    return draw_pile.splice(0, sample_size);
  }
  
  function runIteration () {
    if (! self._running) return false;
    async.eachSeries(getSample(), function(source, cb){
      if (! self._running) return cb();
      async.waterfall([
        function (cb) {
          // by calling watch, we are ensuring that no volitile 
          // ops can happen to this keyspace for the multi txn to commit
          client.watch(source.queue, cb);
        },
        function (watch_ok, cb) {
          client.zrangebyscore([ source.queue, 0, Date.now(), 'LIMIT', 0, 1 ], function(err, results){
            if (err || ! results.length) return cb(err)            
            var num_results = results.length;
            var txn = client.multi();
            var evt_map = {};
            txn.zrem(([source.queue]).concat(results));
            results.forEach(function(id){
              txn.hget(source.data, id, function(err, d){
                if (!err) evt_map[id] = d;
              });
              txn.hdel(source.data, id);
            });
            txn.exec(function(err, mult_results){
              if (err) return cb(err);
              // check to see if this was locked and rolled back
              // we will either get 'null' results or the amt removed will be less than we expect
              if (!mult_results || mult_results[0] < results.length) return cb();
              // otherwise, fire the handlers with associated data
              Object.keys(evt_map).forEach(function(task_id){
                var task_data;
                try{
                  task_data = evt_map[task_id] ? JSON.parse(evt_map[task_id]) : null;
                  self.emit('task', task_id, task_data, function (success){
                    if (success) return cb();
                    utq_inst.schedule(task_id, Date.now(), task_data, function(){ cb() });
                  });
                  
                } catch(e) {
                  cb(e);
                }
              });
            })
            
          });
        },
        function (cb) {
          client.unwatch(cb);
        }
      ], cb);
    }, function(err){
      if (err) console.log('utq-error:', err);
      setTimeout(runIteration, 50);
    });
  }
  
}

function simpleAssert (cond, message) {
  try {
    assert(cond, message);
  } catch (e) {
    throw new Error( format("PreconditionFailed: %s", e.message) )
  }
}