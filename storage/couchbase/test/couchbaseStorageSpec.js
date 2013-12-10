var expect = require('expect.js')
  , storageModule = require('../storage')
  , _ = require('lodash')
  , async = require('async');
 
 describe('Storage', function() {

    var storage;

    describe('beeing not connected', function() {

        describe('calling createStorage', function() {

            describe('without a callback', function() {

                before(function() {
                    storage = storageModule.createStorage({ dbName: 'testeventstore' });
                });

                describe('calling connect', function() {

                    it('it should connect successfully', function(done) {

                        storage.connect(function(err) {
                            expect(err).not.to.be.ok();
                            done();
                        });

                    });

                });

            });

            describe('with a callback', function() {

                it('it should connect successfully', function(done) {

                    storageModule.createStorage({ dbName: 'testeventstore' }, function(err, str) {
                        storage = str;
                        expect(err).not.to.be.ok();
                        expect(str).to.not.eql(null);
                        done();
                    });

                });

            });

        });

    });

    describe('beeing connected', function() {

        before(function(done) {
            async.series([
                function(callback) {
                    storage.events.query(function(err, results) {
                        var kv = _.pluck(results, 'id');
                        if(kv.length == 0) return callback(err, 1);
                        storage.client.removeMulti(kv, {}, function(err, results) {
                            callback(err, 1);
                        });
                    });
                },
                function(callback) {
                    storage.snapshots.query(function(err, results) {
                        var kv = _.pluck(results, 'id');
                        if(kv.length == 0) return callback(err, 2);
                        storage.client.removeMulti(kv, {}, function(err, results) {
                            callback(err, 2);
                        });
                    });
                }
            ],
            function(err, results) {
                done(err, results);
            });
        });

        describe('calling getId', function() {

            it('it should callback with a new id', function(done) {

                storage.getId(function(err, id) {
                    expect(err).not.to.be.ok();
                    expect(id).to.be.an('string');
                    done();
                });

            });

        });

        describe('calling addEvents', function() {

            var event = {
                streamId: 'id1',
                streamRevision: 0,
                commitId: '10',
                commitSequence: '10',
                dispatched: false,
                payload: {
                    event:'bla'
                }
            };

            it('it should save the events', function(done) {

                storage.addEvents([event], function(err) {
                    expect(err).not.to.be.ok();

                    storage.getEvents('id1', -1, function(err, evts) {
                        expect(err).not.to.be.ok();
                        expect(evts).to.be.an('array');
                        expect(evts).to.have.length(1);

                        done();
                    });
                });

            });

            describe('calling getUndispatchedEvents', function() {

                it('it should be in the array', function(done) {
                    storage.getUndispatchedEvents(function(err, evts) {
                        expect(err).not.to.be.ok();
                        expect(evts).to.be.an('array');
                        expect(evts).to.have.length(1);
                        expect(event).to.eql(evts[0]);

                        done();
                    });
                });

                describe('calling setEventToDispatched', function() {

                    it('it should not be in the undispatched array anymore', function(done) {

                        storage.setEventToDispatched(event, function(err, evt) {
                            expect(err).not.to.be.ok();
                            storage.getUndispatchedEvents(function(err, evts) {
                                expect(err).not.to.be.ok();
                                expect(evts).to.be.an('array');
                                expect(evts).to.have.length(0);

                                done();
                            });
                        });

                    });

                });
                
            });

        });

        describe('calling addSnapshot', function() {

            it('it should save the snapshot', function(done) {

                var snapshot = {
                    snapshotId: '1',
                    streamId: '3',
                    revision: 1,
                    data: 'data'
                };
                
                storage.addSnapshot(snapshot, function(err) {
                    expect(err).to.eql(null);

                    storage.getSnapshot('3', function(err, snap) {
                        expect(err).not.to.be.ok();
                        expect(snap.data).to.eql(snapshot.data);
                        expect(snap.snapshotId).to.eql(snapshot.snapshotId);
                        expect(snap.revision).to.eql(snapshot.revision);
                        expect(snap.streamId).to.eql(snapshot.streamId);

                        done();
                    });
                });

            });

        });

        describe('having a filled store with example data', function() {
        
            before(function(done) {
                storage.addEvents([
                    {streamId: '2', streamRevision: 0, commitId: 0, commitSequence: 0, 
                        commitStamp: new Date(2012, 3, 14, 8, 0, 0), payload: {id: '1', event:'events1'}, dispatched: false},
                    {streamId: '2', streamRevision: 1, commitId: 1, commitSequence: 1, 
                        commitStamp: new Date(2012, 3, 14, 9, 0, 0), payload: {id: '2', event:'events2'}, dispatched: false},
                    {streamId: '2', streamRevision: 2, commitId: 2, commitSequence: 2, 
                        commitStamp: new Date(2012, 3, 14, 10, 0, 0), payload: {id: '3', event:'events3'}, dispatched: false},
                    {streamId: '2', streamRevision: 3, commitId: 3, commitSequence: 3, 
                        commitStamp: new Date(2012, 3, 15, 8, 0, 0), payload: {id: '4', event:'events4'}, dispatched: false}
                ],
                function (err) {
                    storage.addEvents([
                        {streamId: '3', streamRevision: 0, commitId: 4, commitSequence: 4, 
                            commitStamp: new Date(2012, 3, 16, 8, 0, 0), payload: {id: '5', event:'events5'}, dispatched: false},
                        {streamId: '3', streamRevision: 1, commitId: 5, commitSequence: 5, 
                            commitStamp: new Date(2012, 3, 17, 8, 0, 0), payload: {id: '6', event:'events6'}, dispatched: false}
                        ], 
                        function (err) {
                            storage.addSnapshot({snapshotId: '1', streamId: '3', revision: 1, data: 'data'}, function() {
                                storage.addSnapshot({snapshotId: '2', streamId: '3', revision: 2, data: 'dataPlus'}, done);
                            });
                        }
                    );
                });
            });
        
            describe('calling getEvents for id 2', function() {
        
                it('it should callback with the correct values', function(done) {
                    storage.getEvents('2', 0, -1, function(err, events) {
                        expect(err).not.to.be.ok();
                        expect(events).to.have.length(4);
                        expect(events[0].commitId).to.eql('0');
                        expect(events[1].commitId).to.eql('1');
                        expect(events[3].commitId).to.eql('3');
        
                        done();
                    });
                });
        
            });
        
            describe('calling getEvents for id 3', function() {
        
                it('it should callback with the correct values', function(done) {
                    storage.getEvents('3', 0, -1, function(err, events) {
                        expect(err).not.to.be.ok();
                        expect(events).to.have.length(2);
        
                        done();
                    });
                });
        
            });
        
            describe('calling getEvents for id 2 from 1 to 3', function() {
        
                it('it should callback with the correct values', function(done) {
                    storage.getEvents('2', 1, 3, function(err, events) {
                        expect(err).to.eql(null);
                        expect(events).to.have.length(2);
        
                        done();
                    });
                });
        
            });
        
            describe('calling getUndispatchedEvents', function() {
        
                it('it should callback with the correct values', function(done) {
                    storage.getUndispatchedEvents(function(err, events) {
                        expect(err).not.to.be.ok();
                        expect(events).to.have.length(6);
                        expect(events[0].commitId).to.eql('0');
                        expect(events[2].commitId).to.eql('2');
                        expect(events[5].commitId).to.eql('5');
        
                        done();
                    });
                });
        
            });
        
            describe('calling getEventRange searching by event id', function() {
        
                it('it should callback with the correct values', function(done) {
                    storage.getEventRange({id: '2'}, 2, function(err, events) {
                        expect(err).not.to.be.ok();
                        expect(events).to.have.length(2);
                        expect(events[0].commitId).to.eql('2');
                        expect(events[1].commitId).to.eql('3');
        
                        done();
                    });
                });
        
            });
        
            describe('calling getSnapshot for id 3', function() {
        
                it('it should callback with the correct values', function(done) {
                    storage.getSnapshot('3', -1, function(err, snap) {
                        expect(err).not.to.be.ok();
                        expect(snap.data).to.eql('dataPlus');
                        expect(snap.snapshotId).to.eql('2');
                        expect(snap.streamId).to.eql('3');
                        expect(snap.revision).to.eql('2');
        
                        done();
                    });
                });
        
            });
        
            describe('calling getSnapshot for id 3 with maxRev 1', function() {
        
                it('it should callback with the correct values', function(done) {
                    storage.getSnapshot('3', 1, function(err, snap) {
                        expect(err).not.to.be.ok();
                        expect(snap.data).to.eql('data');
                        expect(snap.snapshotId).to.eql('1');
                        expect(snap.streamId).to.eql('3');
                        expect(snap.revision).to.eql('1');
        
                        done();
                    });
                });
        
            });
        
        });

    });

 });