/* vim: set softtabstop=4 ts=4 sw=4 expandtab tw=120 syntax=javascript: */
/* jshint node:true, unused:true */
'use strict';

module.exports = function(conn, prefix) {
    conn = conn || mongoose;
    prefix = prefix || '';
    var modelName = prefix + 'Job';

    var schema = new Schema({
        template: {type: SchemaTypes.ObjectId, ref: prefix + 'JobTemplate'},
        topic: {type: String, required: false, default: null},
        priority: {type: Number, required: true, default: 0},
        name: {type: String, required: true},
        attempt: {type: Number, required: true, default: 1},
        retries: {type: Number, required: true, default: 0},
        timeout: {type: Number, required: true, default: settings.timeout},
        created: {type: Date, required: true, default: Date.now},
        scheduledAt: {type: Date, required: true, default: settings.epoch},
        status: {type: String, required: true,
            enum: Object.keys(status),
            lowercase: true,
            default: status.scheduled},
        metadata: {type: SchemaTypes.Mixed},
        lockExpires: {type: Date, required: false, default: null},
        worker: {type: String, required: true, default: unassigned},
        started: {type: Date},
        finished: {type: Date},
    });
    schema.index({def: 1, status: 1});
    schema.index({status: 1, scheduledAt: 1, worker: 1, topic: 1});
    schema.index({status: 1, lockExpires: 1, topic: 1});

    schema.statics.findNext = function findNext(opts, worker) {
        var query = _.extend({
            status: status.scheduled,
            scheduledAt: {$lte: new Date()},
            worker: unassigned,
        }, opts);
        var update = {
            status: status.running,
            worker: worker,
            started: new Date(),
        };
        if (!worker) { throw new Error('worker required'); }
        var promise = q(this.findOneAndUpdate(query, update, {new: true, sort: {priority: -1}}).exec());
        return promise.then(function(job) {
            if (!job) { return; }
            return job.addLogEntry({
                ts: job.started,
                level: 'info',
                message: util.format('assigned worker: %s', worker),
            }).then(function() {
                return q(job.save());
            });
        });
    };

    schema.statics.findAssigned = function(opts) {
        if (!(opts.id && opts.worker)) { throw new Error('id and worker are required'); }
        opts._id = opts.id;
        delete opts.id;
        return q(this.findOne(opts).exec());
    }

    schema.statics.findFuture = function findFuture(opts) {
        var now = new Date();
        var future = _.extend({
            status: status.scheduled,
            scheduledAt: {$gte: now},
            worker: unassigned,
        }, opts);
        var current = {status: status.running, lockExpires: {$gte: now}};
        if (opts.topic) {
            current.topic = opts.topic;
        }
        var query = {$or: [future, current]};
        return q(this.findOne(query).exec());
    };

    schema.statics.findExpired = function findExpired(opts, lean) {
        var query = _.extend({
            status: status.running,
            lockExpires: {$lte: new Date()},
        }, opts);
        query = this.find(query);
        if (!lean) { query.populate('template'); }
        return q(query.exec());
    };

    schema.statics.scheduleJob = function scheduleJob(tpl, previous) {
        var dfd;
        if (!template) { throw new Error('template required'); }
        if (template.paused) { return null; }
        if (tpl._id) {
            dfd = model.findOne({
                template: tpl._id,
                status: {$in: [status.scheduled, status.running]}
            }).exec();
        }
        return q(dfd).then(function(job) {
            if (job) { return job; }
            var attempt = getAttemptNum(previous, tpl.retries || 0);
            var schAt = nextSchedule(tpl.schedule, attempt > 1);
            return q(model.create({
                template: tpl._id,
                attempt: attempt,
                retries: tpl.retries || 0,
                scheduledAt: schAt,
                priority: tpl.priority || 0,
                worker: unassigned,
                topic: tpl.topic,
                name: tpl.name,
                metadata: tpl.metadata,
                timeout: tpl.timeout || settings.timeout,
            }));
        });
    }

    schema.methods.finish = function finish(status) {
        var self = this, entry;
        self.status = status;
        self.finished = new Date();
        entry = self.addLogEntry({level: 'info', message: util.format('finished: %s', self.status)});
        return q.all([entry, self.save()]).then(function() {
            if (!(self.template && typeof(self.template.scheduleJob) === 'function')) {
                return;
            }
            return self.template.scheduleJob(self);
        });
    };

    schema.methods.addLogEntry = function addLogEntry(entry) {
        var self = this;
        var LogEntry = conn.model(prefix + 'LogEntry');
        return q(LogEntry.create(_.extend({obj: self._id}, entry)));
    };

    schema.methods.logEntries = function logEntries(options) {
        var self = this;
        var LogEntry = conn.model(prefix + 'LogEntry');
        options = options || {};
        if ('string' === typeof options.level) { options.level = options.level.split(','); }
        if (Array.isArray(options.level)) { options.level = {$in: options.level}; }
        return q(LogEntry.find(_.extend({obj: self._id}, options)).sort({ts: 1}).lean().exec())
    };

    schema.pre('save', function(next) {
        var self = this;
        if (self.status === status.running) {
            this.lockExpires = moment().add(self.timeout, 'milliseconds').toDate();
        }
        next();
    });

    var model = conn.model(modelName, schema);

    return model;
};

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

function getAttemptNum(job, retries) {
    if (!job || job && job.attempt > retries || job.status === status.success) {
        return 1;
    }
    return job.attempt + 1;
}

var util = require('util');

var q = require('q');
var cronParser = require('cron-parser').parseExpression;
var moment = require('moment-timezone');
var _ = require('lodash');

var mongoose = require('mongoose'),
    Schema = mongoose.Schema,
    SchemaTypes = Schema.Types;

var settings = require('../settings'),
    status = settings.status,
    unassigned = settings.unassigned;
