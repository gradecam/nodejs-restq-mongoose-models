/* vim: set softtabstop=4 ts=4 sw=4 expandtab tw=120 syntax=javascript: */
/* jshint node:true, unused:true */
'use strict';

module.exports = function(conn, prefix) {
    conn = conn || mongoose;
    prefix = prefix || '';
    var modelName = prefix + 'LogEntry';

    var schema = new Schema({
        obj: {type: SchemaTypes.ObjectId, required: true},
        ts: {type: Date, required: true, default: Date.now},
        level: {type: String, required: true, lowercase: true},
        message: {type: String, required: true},
    });

    schema.index({obj: 1, level: 1, ts: 1});

    var model = conn.model(modelName, schema);

    return model;
};

var mongoose = require('mongoose'),
    Schema = mongoose.Schema,
    SchemaTypes = Schema.Types;
