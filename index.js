'use strict';

var crypto = require('crypto');
var util = require('util');
var FlushWritable = require('flushwritable');
var request = require('request');

util.inherits(Neo4jBatchWritable, FlushWritable);


function generateSHA1FromObj(obj) {
    var str = "";

    for (var k in obj) {
        str += k + obj[k].toString().toLowerCase().trim();
    }

    var shasum = crypto.createHash('sha1');
    shasum.update(str);
    return shasum.digest('hex');
}


// Function to set contraints
// CREATE CONSTRAINT ON (p:Person) ASSERT p.sha1 IS UNIQUE


// Wrapper using request to batch insert into a neo4j server
function Neo4jBatchWritable(username, password, options) {
    options = options || {};
    options.objectMode = true;

    FlushWritable.call(this, options);

    this.url = options.url || "http://localhost:7474/db/data/";
    this._auth = new Buffer(username + ":" + password).toString("base64");
    this.highWaterMark = options.highWaterMark || 512;


    this.queue = [];

    this.nodeId = 1;
    this.nodemap = {};
}


Neo4jBatchWritable.prototype.getArgs = function(endpoint, body) {
    var url = this.url + (endpoint === "transaction" ? "transaction/commit" : "batch");

    return {
        "method": 'POST',
        "url" : url,
        "headers": {
            'Content-Type': "application/json; charset=UTF-8; stream=true",
            'Authorization': 'Basic ' + this._auth,
            'User-Agent': 'neo4j-batch-index'
        },
        "json": body,
    };
}

Neo4jBatchWritable.prototype.batch = function(nodes, callback) {
    var args = this.getArgs("batch", nodes);

    request(args, function(err, res, data) {
        if (err) {
            console.log(err);
            return callback(err);
        }
        callback();
    });
};


Neo4jBatchWritable.prototype.add_relations = function(relations, callback) {
    var args = this.getArgs("transaction", { "statements": relations });

    request(args, function(err, res, data) {
        if (err) {
            console.log(err);
            return callback(err);
        }
        callback();
    });
};

// Inherit _flush
Neo4jBatchWritable.prototype._flush = function (callback) {
    if (this.queue.length === 0) {
        return callback();
    }

    var nodes = [];
    var labels = [];
    var relations = [];

    try {
        for (var i=0; i < this.queue.length; i++) {
            // Clone node object
            var node = this.queue[i];

            if (node.hasOwnProperty("label")) {
                var _id = generateSHA1FromObj(node);
                var localNodeId = this.nodeId;

                // Check if we didn't already add the node earlier
                if (this.nodemap.hasOwnProperty(_id)) {
                    localNodeId = this.nodemap[_id];
                }
                else {
                    this.nodemap[_id] = this.nodeId;

                    // Build clone, so you don't get reference problems
                    var clone = util._extend({}, node);

                    // Add sha1 for relation referencing
                    clone["sha1"] = _id;
                    delete clone["label"];

                    // Insert node first
                    nodes.push({
                        "method": 'POST',
                        "to"    : '/node',
                        "id"    : i,
                        "body"  : clone
                    });

                    // Add reference label (e.g. batch pointer to node)
                    labels.push({
                        "method" : 'POST',
                        "to": '{' + i + '}/labels',
                        "body":  node["label"],
                    });
                }
            }
            else if (node.hasOwnProperty("relation")) {
                // Create relation
                relations.push({
                    "statement": `MATCH (a:${node.start.label} {sha1: {START} }), (b:${node.end.label} {sha1: {END} }) WITH a,b
                       CREATE (a)-[:${node.relation}]->(b) return Null`,
                    "parameters": {
                        "START": generateSHA1FromObj(node["start"]),
                        "END"  : generateSHA1FromObj(node["end"])
                    }
                });
            }
            else {
                // pass
            }

            this.nodeId++;
        }
    }
    catch (err) {
        console.log(err)
        return callback(err);
    }

    this.queue = [];
    var records = [].concat(nodes, labels);

    this.batch(records, function(err) {
        if (err) {
            console.log("ERROR adding nodes", err);
            return callback(err);
        }

        this.add_relations(relations, function(err) {
            if (err) {
                console.log("ERROR adding relations", err);
                return callback(err);
            }
            callback();
        });
    }.bind(this));
};


// Inherit _write
Neo4jBatchWritable.prototype._write = function (record, enc, callback) {
    this.queue.push(record);

    if (this.queue.length >= this.highWaterMark) {
        return this._flush(callback);
    }

    callback();
};

module.exports = Neo4jBatchWritable;
