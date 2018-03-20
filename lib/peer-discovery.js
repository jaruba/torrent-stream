var events = require('events');		
var dht = require('bittorrent-dht');		
var tracker = require('bittorrent-tracker');		
		
var DEFAULT_PORT = 6881;		

module.exports = function(opts) {	

	var noSeeding = opts.noSeeding

	var torrent;	
		
	var port = opts.port || DEFAULT_PORT;
		
	var discovery = new events.EventEmitter();		
		
	discovery.dht = null;	
	discovery.tracker = null;
	discovery.niceSeeder = false;
	discovery.whenSeeder = 0;
	discovery.seedDelay = 0;
		
	var onpeer = function(addr) {		
		discovery.emit('peer', addr);
	};		
	
	var onforcepeer = function(addr) {		
		discovery.emit('forcePeer', addr);
	};
		
	var createDHT = function(infoHash) {		
		if (opts.dht === false) return;		
		
		var table = dht();

		table.on('peer', function(addr) {
			if (discovery.amSeeder && !noSeeding) {
				onforcepeer(addr);
			} else {
				onpeer(addr);
			}
		});		
		table.on('ready', function() {		
			table.lookup(infoHash);		
		});		
		table.listen();		
		
		return table;		
	};		
		
	var createTracker = function(torrent) {	
		if (discovery.amSeeder && discovery.tracker) {
			return discovery.tracker;
		}
		if (opts.trackers) {		
			torrent = Object.create(torrent);		
			var trackers = (opts.tracker !== false) && torrent.announce ? torrent.announce : [];		
			torrent.announce = trackers.concat(opts.trackers);		
		} else if (opts.tracker === false) {		
			return;		
		}		
		
		if (!torrent.announce || !torrent.announce.length) return;		
		
		var tr = new tracker.Client(new Buffer(opts.id), port, torrent);

		tr.on('peer', function(addr) {
			if (discovery.amSeeder && !noSeeding) {
				onforcepeer(addr);
			} else {
				onpeer(addr);
			}
		});		
		tr.on('error', function() { /* noop */ });		
		
		if (!noSeeding) {
			if (discovery.amSeeder) {
				tr.complete();
			} else {
				tr.start();
			}
		} else tr.start();
		return tr;		
	};
		
	discovery.setTorrent = function(t) {
		torrent = t;		
		
		if (discovery.tracker) {		
			// If we have tracker then it had probably been created before we got infoDictionary.		
			// So client do not know torrent length and can not report right information about uploads		
			discovery.tracker.torrentLength = torrent.length;		
		} else {		
			process.nextTick(function() {		
				if (!discovery.dht) discovery.dht = createDHT(torrent.infoHash);		
				if (!discovery.tracker) discovery.tracker = createTracker(torrent);		
			});		
		}		
	};

	discovery.updatePort = function(p) {
		if (port === p) return;
		port = p;
		if (discovery.tracker) discovery.tracker.stop();
		if (torrent) {
			if (discovery.dht) discovery.dht.announce(torrent.infoHash, port);
			discovery.tracker = createTracker(torrent);
		}
	};
	
	discovery.seedCheck = function () {
		discovery.complete();
		if (discovery.seedDelay == 180000 && discovery.whenSeeder < Date.now() - 600000) {
			// 10 min passed => 5 min interval
			discovery.seedDelay = 300000;
			var changed = true;
		} else if (discovery.seedDelay == 300000 && discovery.whenSeeder < Date.now() - 1800000) {
			// 30 min passed => 10 min interval
			discovery.seedDelay = 600000;
			var changed = true;
		} else if (discovery.seedDelay == 600000 && discovery.whenSeeder < Date.now() - 3600000) {
			// 60 min passed => 15 min interval
			discovery.seedDelay = 900000;
			var changed = true;
		} else if (discovery.seedDelay == 600000 && discovery.whenSeeder < Date.now() - 7200000) {
			// 120 min passed => 30 min interval
			discovery.seedDelay = 1800000;
			var changed = true;
		}
		if (changed) {
			clearInterval(discovery.niceSeeder);
			discovery.niceSeeder = setInterval(discovery.seedCheck, discovery.seedDelay);
		}
	};
	
	discovery.complete = function (opts) {
		discovery.amSeeder = true;
		if (!noSeeding) {
			if (discovery.tracker) {
				if (!discovery.niceSeeder) {
					discovery.whenSeeder = Date.now();
					discovery.seedDelay = 180000; // start at a 3 min interval
					discovery.niceSeeder = setInterval(discovery.seedCheck, discovery.seedDelay);
				}
				discovery.tracker.complete(opts);
			}
		}
	};

	discovery.update = function (opts) {
		discovery.tracker.update(opts);
	};
	
	discovery.stop = function(opts) {
		if (discovery.niceSeeder) {
			clearInterval(discovery.niceSeeder);
			discovery.niceSeeder = false;
		}
		if (discovery.tracker) discovery.tracker.stop(opts);		
		if (discovery.dht) discovery.dht.destroy();		
	};		
		
	discovery.restart = function() {
		if (!torrent) return;
		if (discovery.tracker) discovery.tracker.stop();
		if (discovery.dht) discovery.dht.destroy();
		discovery.tracker = createTracker(torrent);
		discovery.dht = createDHT(torrent.infoHash);
	};
		
	return discovery;		
};
