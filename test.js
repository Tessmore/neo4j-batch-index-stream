#!/usr/bin/env node

var _ = require("lodash");
var through2 = require('through2');
var split2 = require('split2');

var Neo4jStream = require('./index.js');


var buildRecords = through2({ objectMode: true }, function(chunk, enc, callback) {
    var line = chunk.toString().trim();

    if (line && line.length) {
        var parts = line.split("|");

        var name = parts[0];
        var obj  = parts[1];
        var rel  = parts[2];

        this.push({
            "name": name
        });
    }

    callback();
});


var username = "neo4j";
var password = "test123";

var stream = new Neo4jStream(username, password, {
  highWaterMark: 256
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
