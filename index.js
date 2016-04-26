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
    this.highWaterMark = options.highWaterMark || 512;

    this.queue = [];
    this._id = 0;
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

    console.log(JSON.stringify(options.json, null, 2))

    request(options, function(err, res, data) {
        if (err) {
            console.log(err);
            return callback(err);
        }

        console.log(data)
        // console.log(JSON.stringify(data, null, 2));

        callback();
    });
};


// Inherit _flush
Neo4jBatchWritable.prototype._flush = function _flush(callback) {
    if (this.queue.length === 0) {
        return callback();
    }

    var records = [];
    var nodemap = {};

    try {
        var nodes = [];
        var labels = [];
        var relations = [];

        var nodeId = 1;

        for (var i=0; i < this.queue.length; i++) {
            var node = this.queue[i];

            if (node.hasOwnProperty("label")) {
                // Build Label
                labels.push({
                    "method" : 'POST',
                    "to"     : '{' + nodeId + '}/labels',
                    "body"   : node["label"],
                });

                // Build Node
                var tmp = {
                    "method": 'POST',
                    "to"    : '/node',
                    "id"    : nodeId,
                    "body"  : ""
                }

                nodemap[node._id] = nodeId;

                delete node["_id"];
                delete node["label"];

                // Put rest of body as Node
                tmp["body"] = node;

                nodeId++;
                nodes.push(tmp);
            }

            else if (node.hasOwnProperty("relation")) {
                // Create Relation by nodeID's
                relations.push({
                    "method": 'POST',
                    "to": '{' + nodemap[node["start"]] + '}/relationships',
                    "body": {
                         "to": '{' + nodemap[node["end"]] + '}',
                         "type": node["relation"],
                    }
                });
            }
            else {
                // pass
            }
        }

        console.log(nodemap)
        records = [].concat(nodes, labels, relations);
    }
    catch (err) {
        console.log(err)
        return callback(err);
    }

    this.queue = [];

    this.batch(records, function(err) {
        if (err) {
            console.log(err);
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
