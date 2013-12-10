//     storage.js v0.0.1
//     (c)(by) Alan Sikora (alansikora)

// The storage is the database driver for couchbase.
//
// __Example:__
//
//      require('[pathToStorage]/storage').createStorage({}, function(err, storage) {
//          ...
//      });

var couchbase = require('couchbase')
  , tolerate = require('tolerance')
  , _ = require('lodash')
  , uuid = require('./uuid')
  , root = this
  , couchbaseStorage
  , Storage;

if (typeof exports !== 'undefined') {
    couchbaseStorage = exports;
} else {
    couchbaseStorage = root.couchbaseStorage = {};
}

couchbaseStorage.VERSION = '0.0.1';

// Create new instance of storage.
couchbaseStorage.createStorage = function(options, callback) {
    return new Storage(options, callback);
};


// ## Couchbase storage
Storage = function(options, callback) {

    this.filename = __filename;
    this.isConnected = false;

    if (typeof options === 'function')
        callback = options;
        
    var defaults = {
        host: 'localhost',
        port: 8091,
        dbName: 'eventstore',
        eventsCollectionName: 'events',
        snapshotsCollectionName: 'snapshots',
        query: { stale: false }
    };
    
    this.options = mergeOptions(options, defaults);
    
    // create couchbase specific variable names
    this.options.originalHost       = this.options.host;
    this.options.host               = this.options.host + ':' + this.options.port;
    this.options.connectionTimeout  = this.options.timeout;
    this.options.bucket             = this.options.dbName;
    this.options.designDocument     = 'dev_' + this.options.bucket;
    this.options.eventsViewName     = this.options.eventsCollectionName;
    this.options.snapshotsViewName  = this.options.snapshotsCollectionName;

    var defaultOpt = { };

    this.options.options = this.options.options || {};

    this.options.options = mergeOptions(this.options.options, defaultOpt);
    
    if (callback) {
        this.connect(callback);
    }
};

Storage.prototype = {

    // __connect:__ connects the underlaying database.
    //
    // `storage.connect(callback)`
    //
    // - __callback:__ `function(err, storage){}`
    connect: function(callback) {
        var self = this;

        tolerate(function(callback) {
            var couchbaseOptions = { host: self.options.host, bucket: self.options.bucket, connectionTimeout: self.options.connectionTimeout };
            var server = new couchbase.Connection(couchbaseOptions, function(err) {
                callback(err, server);
            });
        }, this.options.timeout || 0, function(err, client) {
            if (err) {
                if (callback) callback(err);
            } else {
                self.client = client;
                self.isConnected = true;
              
                self.events = self.client.view(self.options.designDocument, self.options.eventsViewName, self.options.query);
                self.snapshots = self.client.view(self.options.designDocument, self.options.snapshotsViewName, self.options.query);

                if (callback) callback(null, self);
            }
        });
    },

    // __addEvents:__ saves all events.
    //
    // `storage.addEvents(events, callback)`
    //
    // - __events:__ the events array
    // - __callback:__ `function(err){}`
    addEvents: function(events, callback) {
        var eventsObj = {};
        
        for(var i in events) {
            var evt = events[i];
            evt._cbtype = 'events';
            evt._id = evt.commitId + evt.commitSequence
            
            eventsObj[this.options.eventsViewName + '_' + evt._id] = { key: {}, value: evt };
        }
        
        this.client.setMulti(eventsObj, {}, callback);
    },

    // __addSnapshot:__ stores the snapshot
    // 
    // `storage.addSnapshot(snapshot, callback)`
    //
    // - __snapshot:__ the snaphot to store
    // - __callback:__ `function(err){}` [optional]
    addSnapshot: function(snapshot, callback) {
        snapshot._id = snapshot.snapshotId;
        snapshot._cbtype = 'snapshots';
        
        this.client.set(this.options.snapshotsViewName + '_' + snapshot._id, snapshot, function(err, res) {
            callback(err || null);
        });
    },

    // __getEvents:__ loads the events from _minRev_ to _maxRev_.
    // 
    // `storage.getEvents(streamId, minRev, maxRev, callback)`
    //
    // - __streamId:__ id for requested stream
    // - __minRev:__ revision startpoint
    // - __maxRev:__ revision endpoint (hint: -1 = to end) [optional]
    // - __callback:__ `function(err, events){}`
    getEvents: function(streamId, minRev, maxRev, callback) {
        if (typeof maxRev === 'function') {
            callback = maxRev;
            maxRev = -1;
        }
        
        this.events.query(function(err, results) {
          
          var events = _.filter(_.pluck(results, 'key'), function(result) {
              if(maxRev == -1) {
                  return (result.streamId == streamId && result.streamRevision >= minRev);
              } else {
                  return (result.streamId == streamId && result.streamRevision >= minRev && result.streamRevision < maxRev);
              }
            });
            
            events = _.sortBy(events, ['streamRevision']);
            
            callback(err, events);
        });
    },

    // __getEventRange:__ loads the range of events from given storage.
    // 
    // `storage.getEventRange(match, amount, callback)`
    //
    // - __match:__ match query in inner event (payload)
    // - __amount:__ amount of events
    // - __callback:__ `function(err, events){}`
    getEventRange: function(match, amount, callback) {
        var self = this,
            query = {};

        if (match) {
            for (var m in match) {
                if (match.hasOwnProperty(m)) {
                    query['payload.' + m] = match[m];
                    break;
                }
            }
        }
        
        var queryItems = _.keys(query);
        
        
        // this.events.findOne(query, function(err, evt) {
        // 
        //     self.events.find({
        //         'commitStamp': {'$gte': evt.commitStamp},
        //         '$or': [
        //             { 'streamId': {'$ne': evt.streamId}},
        //             {'commitId': {'$ne': evt.commitId}} ]},
        //         {sort:[['commitStamp','asc']], limit: amount}).toArray(callback);
        // 
        // });
        
        
        this.events.query(function(err, results) {
          
            var events = _.pluck(results, 'key');
            
            var event = _.find(events, function(event) {
                for(var i = 0; i < queryItems.length; i++) {
                    var arr = queryItems[i].split('.');
                    
                    var value = event;
                    while(arr.length && (value = value[arr.shift()]));
                    
                    if(value != query[queryItems[i]]) return false;
                };
                return true;
            });
            
            events = _.filter(events, function(result) {
                return result.commitStamp >= event.commitStamp && (result.streamId != event.streamId || result.commitId != event.commitId);
            });
            
            events = _.sortBy(events, ['commitStamp']).splice(0, amount);
            
            callback(err, events);
        });
    },

    // __getSnapshot:__ loads the next snapshot back from given max revision or the latest if you 
    // don't pass in a _maxRev_.
    // 
    // `storage.getSnapshot(streamId, maxRev, callback)`
    //
    // - __streamId:__ id for requested stream
    // - __maxRev:__ revision endpoint (hint: -1 = to end)
    // - __callback:__ `function(err, snapshot){}`
    getSnapshot: function(streamId, maxRev, callback) {
        if (typeof maxRev === 'function') {
            callback = maxRev;
            maxRev = -1;
        }
        
        this.snapshots.query(function(err, results) {
          
          var snapshots = _.filter(_.pluck(results, 'key'), function(result) {
              return (maxRev > -1) ? result.streamId == streamId && result.revision <= maxRev : result.streamId == streamId;
          });
          
          snapshots = _.sortBy(snapshots, ['revision']);
          _(snapshots).reverse();
          
          if (err) {
              callback(err);
          } else {
              snapshots.length > 0 ? callback(null, snapshots[0]) : callback(null, {});
          }
          
        });
        
    },

    // __getUndispatchedEvents:__ loads all undispatched events.
    //
    // `storage.getUndispatchedEvents(callback)`
    //
    // - __callback:__ `function(err, events){}`
    getUndispatchedEvents: function(callback) {
        this.events.query(function(err, results) {
          
          var events = _.filter(_.pluck(results, 'key'), function(result) {
              return result.dispatched == false;
          });
          
          events = _.sortBy(events, ['streamId', 'streamRevision']);
          
          callback(err, events);
        });
    },

    // __setEventToDispatched:__ sets the given event to dispatched.
    //
    // __hint:__ instead of the whole event object you can pass: {_id: 'commitId'}
    //
    // `storage.setEventToDispatched(event, callback)`
    //
    // - __event:__ the event
    // - __callback:__ `function(err, events){}` [optional]
    setEventToDispatched: function(event, callback) {
        var self = this;
        
        this.client.get(this.options.eventsViewName + '_' + event._id, function(err, result) {
            if(err) callback(err);
            
            var evt = result.value;
            
            evt.dispatched = true;
            
            self.client.set(self.options.eventsViewName + '_' + evt._id, evt, function(err, res) {
              callback(err);
            });
        });
    },

    // __getId:__ loads a new id from storage.
    //
    // `storage.getId(callback)`
    //
    // - __callback:__ `function(err, id){}`
    getId: function(callback) {
        callback(null, uuid().toString());
    }
};

// helper
var mergeOptions = function(options, defaultOptions) {
    if (!options || typeof options === 'function') {
        return defaultOptions;
    }
    
    var merged = {};
    for (var attrname in defaultOptions) { merged[attrname] = defaultOptions[attrname]; }
    for (attrname in options) { if (options[attrname]) merged[attrname] = options[attrname]; }
    return merged;
};
