'use strict';

var fs = require('fs');
var path = require('path');
var through = require('through2');
var split = require('split2');
var helper = require('../utils');

var MAX_LINES = 10; // 最多10行数据

exports.logdir = []; // 日志路径

var keyMap = new Map(); // 记录每个key实际对应的路径值, key 是目录
var map = new Map();
var caches = new Map(); // 缓存各个文件的 buffered

var patt = /^\[([^\]]+)\] (.+) ([-><]{2}) (.+) "(.+) (.+) (.+) (\d+)" (\d+)$/;

function getSlowHTTPLog(lines) {
  var parsed = [];
  for (var i = 0; i < lines.length; i++) {
    var line = lines[i];
    var matched = line.match(patt);
    if (matched) {
      parsed.push({
        timestamp: matched[1],
        from: matched[2],
        type: matched[3] === '->' ? 'receive': 'send',
        to: matched[4],
        method: matched[5],
        url: matched[6],
        protocol: matched[7],
        code: parseInt(matched[8]),
        rt: parseInt(matched[9])
      });
    }
  }
  return parsed;
}

var getRealPath = function (filepath) {
  var now = new Date();
  var date = helper.getYYYYMMDD(now);
  return path.join(filepath, 'access-' + date + '.log');
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
        buffered.push(line.toString());
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
    var buffered = [];
    var logs = exports.logdir;
    for (var i = 0; i < logs.length; i++) {
      var key = logs[i];
      var cache = caches.get(key);
      buffered = buffered.concat(cache);
      cache = [];
    }
    // no data
    if (buffered.length === 0) {
      return callback(null, null);
    }

    var metrics = getSlowHTTPLog(buffered);
    // clean
    buffered = [];

    callback(null, {
      type: 'slow_http',
      metrics: metrics
    });
  });
};
