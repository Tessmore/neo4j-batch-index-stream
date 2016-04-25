'use strict';

var util = require('util');
var FlushWritable = require('flushwritable');
var request = require('request');

util.inherits(Neo4jBatchWritable, FlushWritable);

// Wrapper using request to batch insert into a neo4j server
function Neo4jBatchWritable(username, password, options) {
    options = options || {};
    options.objectMode = true;

    FlushWritable.call(this, options);

    this._auth = new Buffer(username + ":" + password).toString("base64");
    this.highWaterMark = options.highWaterMark || 256;
    this.queue = [];
}

Neo4jBatchWritable.prototype.batch = function(records, callback) {
    var options = {
        "url"    : 'http://localhost:7474/db/data/batch',
         "headers": {
            "X-Stream": true,
            'Authorization': 'Basic ' + this._auth
        },
        "method" : 'POST',
        "json"   : records,
    };

    // console.log(JSON.stringify(options.json, null, 2))

    request(options, function(err, res, data) {
        if (err) {
            console.log(err);
            return callback(err);
        }

        callback();
    });
};

// Inherit _flush
Neo4jBatchWritable.prototype._flush = function _flush(callback) {
    if (this.queue.length === 0) {
        return callback();
    }

    try {
        var records = [];

        for (var i=0; i < this.queue.length; i++) {
            var node = {
                "method": 'POST',
                "to"    : '/node',
                "id"    : i,
                "body"  : this.queue[i]
            }

            var label = {
                method : 'POST',
                to: '{' + i + '}/labels',
                body:  "Person",
            }

            records.push(node, label);
        }
    }
    catch (err) {
        console.log(err)
        return callback(err);
    }

    this.queue = [];
    this.batch(records, function(err) {
        if (err) {
            return callback(err);
        }
        callback();
    });
};


// Inherit _write
Neo4jBatchWritable.prototype._write = function _write(record, enc, callback) {
    this.queue.push(record);

    if (this.queue.length >= this.highWaterMark) {
        return this._flush(callback);
    }

    callback();
};

module.exports = Neo4jBatchWritable;
