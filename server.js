var express = require("express");
var bodyParser = require("body-parser");
var winston = require('winston');
var dateFormat = require('dateformat');
var r = require('rethinkdb');
var Q = require('q');
var config = require('./config');
if(config.mqtt.enable){
   var mqtt = require('mqtt');
   var mqttclient = mqtt.createClient(config.mqtt.port, config.mqtt.host);
}
var app = express();
var io;

var sock = { emit: function(){} }; // stub

process.stdin.resume();
winston.add(winston.transports.File, { filename: 'credit.log', json: false });

var users;


var connection = null;
r.connect( {host: config.rethinkdb.host, port: config.rethinkdb.port, db: config.rethinkdb.db}, function(err, conn) {
    if (err) {criticalError("Couldn't connect to RethinkDB.");}
    connection = conn;
    serverStart(connection);
})

app.use('/', express.static(__dirname + '/static'));
app.use(bodyParser());


function serverStart(connection){
    server = require('http').createServer(app);
    io = require('socket.io').listen(server);

    io.sockets
    .on('connection', function (socket) {
        sock = socket;
        getAllUsersAsync().then(function(data) {
            socket.emit('accounts', JSON.stringify(data));
        });
        socket.on('getAccounts', function (data) {
            getAllUsersAsync().then(function(data) {
                socket.emit('accounts', JSON.stringify(data));
            });
        })
    });

    var server = server.listen(8000, function(){
        winston.log('info', 'Server started!');
    })
}


app.get("/users/all", function(req, res){
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "X-Requested-With");
    getAllUsersAsync()
    .then(function(data){
        res.send(JSON.stringify({result: data}))
    },
    function(error){
        winston.log('error', '/users/all ' + error.name + " " + error.msg);
        res.send(JSON.stringify({error: error}))
    });
});

app.get("/transactions/all", function(req, res){
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "X-Requested-With");
    getAllTransactionsAsync()
    .then(function(data){
        res.send(JSON.stringify({result: data}))
    },
    function(error){
        winston.log('error', '/transactions/all ' + error.name + " " + error.msg);
        res.send(JSON.stringify({error: error}))
    });
});

app.get("/transactions/:name", function(req, res){
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "X-Requested-With");
    getUserTransactionsAsync(req.params.name)
    .then(function(data){
        res.send(JSON.stringify({result: data}))
    },
    function(error){
        winston.log('error', '/transactions/:name error: ' + error.name + " " + error.msg);
        res.send(JSON.stringify({error: error}))
    });
});

app.post('/user/add', function(req, res){
    var username = req.body.username;
    addUser(username, res);
});

app.post('/user/rename', function(req, res){
    getUserAsync(req.body.username).then(function(user){
        if(user == undefined){
            res.send(404, "User not found");
            winston.log('error', '[userCredit] No user ' + req.body.username + ' found.')
            return;
        }

        var newname = req.body.newname;

        renameUser(user, newname, res);

        res.send(JSON.stringify(user));
        getAllUsersAsync().then(function(users){
            sock.broadcast.emit('accounts', JSON.stringify(users));
            sock.emit('accounts', JSON.stringify(users));
        });
    })
});

app.post("/user/credit", function(req, res){
    var user = undefined;
    getUserAsync(req.body.username).then(function(userObj){
        user = userObj;

        var delta = parseFloat(req.body.delta);

        if(user == undefined){
            res.send(404, "User not found");
            winston.log('error', '[userCredit] No user ' + req.body.username + ' found.')
            return;
        }
        if(isNaN(delta) || delta >= 100 || delta <= -100){
            res.send(406);
            winston.log('error', "[userCredit] delta must be a number.");
            return;
        }
        
        updateCredit(user, delta);
        
        getAllUsersAsync().then(function(users){
            sock.broadcast.emit('accounts', JSON.stringify(users));
            sock.emit('accounts', JSON.stringify(users));
            res.send(JSON.stringify(user));
        });

    }).done();
});

function getUserAsync(username){
    var deferred = Q.defer();
    r.table("users").get(username).run(connection, function(err, user){
        if (err) {
            deferred.reject(err);
            return;
        }
        deferred.resolve(user);
    });
    return deferred.promise;
}

function getAllUsersAsync(){
    var deferred = Q.defer();
    r.table('users').run(connection, function(err, cursor) {
        if (err) {
            deferred.reject(err);
            return;
        }
        cursor.toArray(function(err, result) {
            if (err) {
                deferred.reject(err);
                return;
            }
            deferred.resolve(result);
        });
    });
    return deferred.promise;
}

function getUserTransactionsAsync(username){
    var deferred = Q.defer();
    r.table('transactions').filter(r.row('username').eq(username)).
    run(connection, function(err, cursor) {
        if (err) {
            deferred.reject(err);
            return;
        }
        cursor.toArray(function(err, result) {
            if (err) {
                deferred.reject(err);
                return;
            }
            deferred.resolve(result);
        });
    });
    return deferred.promise;
}

function getAllTransactionsAsync(){
    var deferred = Q.defer();
    r.table('transactions').run(connection, function(err, cursor) {
        if (err) {
            deferred.reject(err);
            return;
        }
        cursor.toArray(function(err, result) {
            if (err) {
                deferred.reject(err);
                return;
            }
            deferred.resolve(result);
        });
    });
    return deferred.promise;
}


exports.addUser = function addUser(username, res){
    r.table("users").insert({
        name: username,
        credit: 0,
        lastchanged: r.now()
    }).run(connection, function(err, dbres){
        if(dbres.errors){
            winston.log('error', "Couldn't save user " + username + err);
            res.send(409, "User exists already.");
        }else{
            getAllUsersAsync().then(function(users){
                sock.broadcast.emit('accounts', JSON.stringify(users));
                sock.emit('accounts', JSON.stringify(users));

                res.send(200);
                winston.log('info', '[addUser] New user ' + username + ' created');
                return true;
            });
        }
    });
}

function renameUser(user, newname, res) {
    r.table("users").insert({
            name: newname, 
            credit: user.credit, 
            lastchanged: r.now()
    }).run(connection, function(err, dbres){
        if(dbres.errors){
            winston.log('error', "Couldn't save user " + newname);
            res.send(409, "That username is already taken");
        }else{
                r.table("users")
                    .filter({name: user.name})
                    .delete()
                    .run(connection, function(err){
                        if(err){
                            winston.log('error', "Couldn't delete old user " + user.name);
                            res.send(409, "Can't delete old user");
                        }
                    });
                r.table("transactions")
                    .filter({username: user.name})
                    .update({username: newname})
                    .run(connection, function(err){
                        if(err){
                            winston.log('error', "Couldn't update transactions of old user " + user.name);
                            res.send(409, "Can't update transactions. Better call silsha!");
                        }
                    });
            }
        });
}

function updateCredit(user, delta) {
    user.credit += +delta;
    user.credit = Math.round(user.credit * 100) / 100;
    user.lastchanged = Date.now();

   var transaction = {
       username: user.name,
       delta: delta,
       credit: user.credit,
       time: r.now()
   }

    r.table("transactions").insert(transaction).run(connection, function(err){
        if(err)
            winston.log('error', "Couldn't save transaction for user " + user.name + err);
            if(config.mqtt.enable){
          mqttPost('transactions', transaction);
        }
    });
    r.table("users")
        .filter({name: user.name})
        .update({credit: user.credit, lastchanged: r.now()})
        .run(connection, function(err){
            if(err)
                winston.log('error', "Couldn't save transaction for user " + user.name + err);
        });

      if(delta < 0)
         sock.emit('ka-ching', JSON.stringify(users));
      else
         sock.emit('one-up', JSON.stringify(users));
    winston.log('info', '[userCredit] Changed credit from user ' + user.name + ' by ' + delta + '. New credit: ' + user.credit);
}

function mqttPost(service, payload){
   mqttclient.publish(config.mqtt.prefix + '/' + service, JSON.stringify(payload));
}

function criticalError(errormsg){
    winston.log('error', errormsg);
    process.exit(1);
}

process.on('SIGTERM', function() {
    winston.log('info', 'Server shutting down. Good bye!');
    process.exit();
});
