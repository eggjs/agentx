'use strict';

var fs = require('fs');
var path = require('path');
var through = require('through2');
var split = require('split2');
var helper = require('../utils');

var MAX_LINES = 250; // 最多200行数据
exports.logdir = []; // 日志路径

var keyMap = new Map(); // 记录每个key实际对应的路径值, key 是目录
var map = new Map(); // 记录每个文件访问的位置
var caches = new Map(); // 缓存各个文件的 buffered

var patt = /\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}\] \[(.+)\] \[(.+)\] \[(\d+)\] (.*)/g;
var reg = /([^\s]*): (\d+)/g;

function getNodeLog(msg) {
  var matched;
  var result = {ok: true, data: []};
  while ((matched = patt.exec(msg)) !== null) {
    var pid = matched[3];
    var detail = matched[4];

    var pair;
    while ((pair = reg.exec(detail)) !== null)  {
      result.data.push({
        pid: pid,
        item: pair[1],
        value: parseFloat(pair[2])
      });
    }
  }
  return result;
}

var getRealPath = function (filepath) {
  var now = new Date();
  var date = helper.getYYYYMMDD(now);
  return path.join(filepath, 'node-' + date + '.log');
};

var readFile = function (key, filepath, callback) {
  fs.stat(filepath, function (err, stats) {
    if (err) {
      return callback(err);
    }

    if (!stats.isFile()) {
      return callback(new Error(filepath + ' is not a file'));
    }

    var start = map.get(filepath) || 0;
    if (stats.size === start) {
      return callback(null);
    }

    var buffered = caches.get(key);
    var readable = fs.createReadStream(filepath, {start: start});
    readable.pipe(split()).pipe(through(function (line, _, next) {
      if (line.length) {
        buffered.push(line);
        if (buffered.length > MAX_LINES) {
          buffered.shift(); // 删掉前面的
        }
      }
      next();
    }));

    readable.on('data', function (data) {
      start += data.length;
    });

    readable.on('end', function () {
      // filepath --> offset
      map.set(filepath, start);
      callback(null);
    });
  });
};

var readLog = function (key, callback) {
  var currentPath = getRealPath(key);
  var current = keyMap.get(key);

  if (currentPath !== current) {
    keyMap.set(key, currentPath); // replace real path
    readFile(key, current, function (err) {
      if (err) {
        return callback(err);
      }
      readFile(key, currentPath, callback);
    });
  } else {
    readFile(key, currentPath, callback);
  }
};

var readLogs = function (callback) {
  var count = exports.logdir.length;
  var returned = 0;
  var done = function (err) {
    returned++;
    if (returned === count) {
      callback(err); // 返回最后一个err
    }
  };
  for (var i = 0; i < count; i++) {
    var key = exports.logdir[i];
    readLog(key, done);
  }
};

exports.init = function (config) {
  if (config.logdir && Array.isArray(config.logdir)) {
    exports.logdir = config.logdir;
    var logs = exports.logdir;
    for (var i = 0; i < logs.length; i++) {
      var key = logs[i];
      // dir --> filepath
      keyMap.set(key, getRealPath(key));
      // dir --> array
      caches.set(key, []);
    }
  }
};


exports.run = function (callback) {
  if (exports.logdir.length < 1) {
    return callback(new Error('Specific logdir in agentx config file as Array'));
  }

  readLogs(function (err) {
    if (err) {
      return callback(err);
    }

    var logs = exports.logdir;
    var list = [];
    for (var i = 0; i < logs.length; i++) {
      var key = logs[i];
      var buffered = caches.get(key);
      list = list.concat(buffered);
      buffered = [];
    }

    var message = list.join('\n');

    callback(null, {
      type: 'node_log',
      metrics: getNodeLog(message)
    });
  });
};
