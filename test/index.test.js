var test = require('tape').test;
var path = require('path');
var init = require('../index.js');
var util = require('util');
//var exec = require('child_process').exec;
//var filepath = path.join(tmp, 'copy.mbtiles');
//try { fs.unlinkSync(filepath); } catch(e) {}

var srcuri = __dirname + '/fixtures/valid.mbtiles';
console.log(srcuri);
var dsturi = 'http://tilestream-tilesets-develot.s3.amazonaws.com/_pending/test-carol-mapbox-tile-copy/{z}/{x}/{y}.png';
console.log(dsturi);

var options = {};
options.progress = report;

test('index.js init', function(t) {
	init(srcuri, dsturi, options, function(err){
    	if (err) console.log("ERROR");
    	else console.log("DONE");
	});
});


function report(stats, p) {
    util.print(util.format('\r\033[K[%s] %s%% %s/%s @ %s/s | ✓ %s □ %s | %s left',
        pad(formatDuration(process.uptime()), 4, true),
        pad((p.percentage).toFixed(4), 8, true),
        pad(formatNumber(p.transferred),6,true),
        pad(formatNumber(p.length),6,true),
        pad(formatNumber(p.speed),4,true),
        formatNumber(stats.done - stats.skipped),
        formatNumber(stats.skipped),
        formatDuration(p.eta)
    ));
}

function formatDuration(duration) {
    var seconds = duration % 60;
    duration -= seconds;
    var minutes = (duration % 3600) / 60;
    duration -= minutes * 60;
    var hours = (duration % 86400) / 3600;
    duration -= hours * 3600;
    var days = duration / 86400;

    return (days > 0 ? days + 'd ' : '') +
        (hours > 0 || days > 0 ? hours + 'h ' : '') +
        (minutes > 0 || hours > 0 || days > 0 ? minutes + 'm ' : '') +
        seconds + 's';
}

function pad(str, len, r) {
    while (str.length < len) str = r ? ' ' + str : str + ' ';
    return str;
}

function formatNumber(num) {
    num = num || 0;
    if (num >= 1e6) {
        return (num / 1e6).toFixed(2) + 'm';
    } else if (num >= 1e3) {
        return (num / 1e3).toFixed(1) + 'k';
    } else {
        return num.toFixed(0);
    }
    return num.join('.');
}