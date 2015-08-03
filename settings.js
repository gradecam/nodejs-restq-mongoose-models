/* vim: set softtabstop=4 ts=4 sw=4 expandtab tw=120 syntax=javascript: */
/* jshint node:true, unused:true */
'use strict';

var ms = require('ms');

var settings = module.exports = {
    timeout: ms('60 seconds'),
    unassigned: 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX',
    status: {
        scheduled: 'scheduled',
        running: 'running',
        failed: 'failed',
        success: 'success',
        timedout: 'timedout',
    },
    epoch: function() { return new Date(0); },
    ttl: ms('90 days'),
};
