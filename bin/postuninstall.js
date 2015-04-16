#!/usr/bin/env node

var mongorc = require('mongorc');
var path = require('path');
var pkg = require(__dirname + '/../package.json');

mongorc.uninstall(pkg.name, path.resolve(__dirname + '/../lib/index.js'), function(){
  console.log('uninstalled %s', pkg.name);
  mongorc.restore(function(){
    console.log('restored mongorc.js backup');
  });
});
