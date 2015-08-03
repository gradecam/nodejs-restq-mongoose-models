/* vim: set softtabstop=4 ts=4 sw=4 expandtab tw=120 syntax=javascript: */
/* jshint node:true, unused:true */
'use strict';

module.exports = function(conn, prefix) {
    conn = conn || mongoose;
    prefix = prefix || '';
    var modelName = prefix + 'JobTemplate';

    var schema = new Schema({
        topic: {type: String, required: false, default: null},
        priority: {type: Number, required: true, default: 0},
        name: {type: String, required: true},
        schedule: {type: String, required: true},
        retries: {type: Number, required: true, default: 0},
        timeout: {type: Number, required: true, default: settings.timeout},
        metadata: {type: SchemaTypes.Mixed},
        paused: {type: Boolean, required: true, default: false},
    });

    schema.methods.scheduleJob = function scheduleJob(job) {
        var self = this;
        if (self.paused) { return null; }
        var Job = conn.model(prefix + 'Job');
        var attempt = getAttemptNum(job, self.retries);
        var query = {template: self._id, status: {$in: [status.scheduled, status.running]}};
        var schAt = nextSchedule(self.schedule, attempt > 1);
        return q(Job.findOne(query).exec()).then(function(job) {
            if (job) { return job; }
            return q(Job.create({
                template: self._id,
                attempt: attempt,
                retries: self.retries,
                scheduledAt: schAt,
                priority: self.priority,
                worker: unassigned,
                topic: self.topic,
                name: self.name,
                metadata: self.metadata,
                timeout: self.timeout,
            }));
        });
    };

    schema.post('save', function postSave(template) {
        return template.scheduleJob();
    });

    var model = conn.model(modelName, schema);

    return model;
};

function getAttemptNum(job, retries) {
    if (!job || job && job.attempt > retries || job.status === status.success) {
        return 1;
    }
    return job.attempt + 1;
}

function nextSchedule(schedule, immediate) {
    if (immediate) {
        return new Date(0);
    }
    if (_.isNaN(Date.parse(schedule))) {
        return cronParser(schedule).next();
    } else {
        return moment(schedule).toDate();
    }
}

var cronParser = require('cron-parser').parseExpression;
var moment = require('moment-timezone');
var _ = require('lodash');
var q = require('q');

var mongoose = require('mongoose'),
    Schema = mongoose.Schema,
    SchemaTypes = Schema.Types;

var settings = require('../settings'),
    unassigned = settings.unassigned,
    status = settings.status;
