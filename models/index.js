/* vim: set softtabstop=4 ts=4 sw=4 expandtab tw=120 syntax=javascript: */
/* jshint node:true, unused:true */
'use strict';

module.exports = function modelLoader(conn, prefix) {
    var models = {};
    fs.readdirSync(__dirname).forEach(function fileLoader(filename) {
        if (filename !== 'index.js' && filename.substr(-3) === '.js') {
            var moduleFilename = __dirname + '/' + filename;
            var mod = require(moduleFilename);
            if (typeof mod === 'function' && !mod.modelName) {
                mod = mod(conn, prefix);
            }
            if (!mod) { return; }
            models[mod.modelName] = mod;
        }
    });
    return models;
};

var fs = require('fs');
