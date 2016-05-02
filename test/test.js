#!/usr/bin/env node

var through2 = require('through2');
var split2 = require('split2');
var Neo4jStream = require('../index.js');


var buildRecords = through2({ objectMode: true }, function(chunk, enc, callback) {
    var line = chunk.toString().trim();

    if (line && line.length) {
        var parts = line.split("|");

        var person = {
            "label" : "Person",
            "name"  : parts[0],
        };

        var obj = {
            "label": "Object",
            "name" : parts[2],
        }

        var relation = {
            "relation" : parts[1],
            "start"    : person,
            "end"      : obj,
        }

        this.push(person);
        this.push(obj);
        this.push(relation);
    }

    callback();
});


var username = "neo4j";
var password = "test123";

var stream = new Neo4jStream(username, password, {
    highWaterMark: 3
});


process.stdin
.pipe(split2())
.pipe(buildRecords)
.pipe(stream)
.on('error', function(error) {
    console.log(error);
})
.on('finish', function() {
    console.log("DONE");
})
