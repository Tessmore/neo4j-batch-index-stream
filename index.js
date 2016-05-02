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
//
// Possible options:
//   - objectMode
//   - highWaterMark
//   - url
//
function Neo4jBatchWritable(username, password, options) {
    options = options || {};
    options.objectMode = true;
    this.highWaterMark = options.highWaterMark || 512;

    FlushWritable.call(this, options);

    this.url = options.url || "http://localhost:7474/db/data/";
    this._auth = new Buffer(username + ":" + password).toString("base64");

    this.index_key = options.index_key || "id";
    this.queue = [];

    this.nodeId = 1;
    this.nodemap = {};

    // Quick check if connection is available
    var args = this.getArgs()
    request(args, function(err, res, data) {
        if (err) {
            throw Error(err);
        }

        var parsed = false;
        try {
            parsed = JSON.parse(data);
        }
        catch (err) {
            throw Error("Neo4j returned invalid JSON.")
        }

        if (parsed && parsed.hasOwnProperty("errors")) {
            throw Error(data);
        }
    });
}


Neo4jBatchWritable.prototype.getArgs = function(endpoint, body) {
    var url = this.url;
    var method = "GET";

    if (endpoint) {
        // Supported endpoints
        if (endpoint === "transaction") {
            url += "transaction/commit";
            method = "POST";
        }
        else if (endpoint === "batch") {
            url += "batch";
            method = "POST";
        }
    }

    var args = {
        "method": method,
        "url" : url,
        "headers": {
            'Content-Type': "application/json; charset=UTF-8; stream=true",
            'Authorization': 'Basic ' + this._auth,
            'User-Agent': 'neo4j-batch-index'
        }
    };

    if (typeof body !== "undefined") {
        args["json"] = body;
    }

    return args;
}

// Add index for fast node lookup when inserting relations
//
// @labels : list of objects
//         = [ [ Label, index_key ], .. ]
Neo4jBatchWritable.prototype.index = function(labels, callback) {
    callback = typeof callback === "function" ? callback : function(){};
    var statements = [];

    for (var i=0; i<labels.length; i++) {
        statements.push({
            "statement": `CREATE INDEX ON :${labels[i][0]}(${labels[i][1]})`
        });
    }

    var args = this.getArgs("transaction", {"statements": statements })
    request(args, function(err, res, data) {
        if (err) {
            callback(err, false);
        }

        callback(data);
    });
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
                // Build clone, so you don't get reference problems
                var clone = util._extend({}, node);
                var localNodeId = this.nodeId;
                var _id;

                // Check for unique index
                if (node.hasOwnProperty(this.index_key)) {
                    _id = node[this.index_key];
                }
                else {
                    // No given index, use sha1 of node object
                    _id = generateSHA1FromObj(node);

                    // Add sha1 for relation referencing
                    clone["sha1"] = _id;
                    this.index_key = "sha1";
                }

                // Check if we didn't already add the node earlier
                if (this.nodemap.hasOwnProperty(_id)) {
                    localNodeId = this.nodemap[_id];
                }
                else {
                    this.nodemap[_id] = this.nodeId;

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

                // If no index key is given -> find node by sha1
                if (this.index_key === "sha1") {
                    var start = generateSHA1FromObj(node["start"]);
                    var end = generateSHA1FromObj(node["end"]);
                }
                else {
                    var start = node["start"][this.index_key];
                    var end = node["end"][this.index_key];
                }

                // Create relation
                relations.push({
                    "statement": `MATCH (a:${node.start.label} {${this.index_key}: {START} }), (b:${node.end.label} {${this.index_key}: {END} }) WITH a,b
                       CREATE (a)-[:${node.relation}]->(b) return Null`,
                    "parameters": {
                        "START": start,
                        "END"  : end
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
