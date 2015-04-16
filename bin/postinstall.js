#!/usr/bin/env node


var mongorc = require('mongorc');
var path = require('path');
var pkg = require(__dirname + '/../package.json');
mongorc._init(function(){
  console.log('backing up current mongorc');
  mongorc.install(pkg.name, path.resolve(__dirname + '/../lib/mongo.js'), function(){
    console.log('installed to mongorc');
  });
});
