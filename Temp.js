
module.exports = RedisTemp;

var util = require("util");
var Temp = require('rtcs.io-storage').Temp;
var redis = require('redis');
var base64id = require('base64id');

function RedisTemp(options) {
    Temp.call(this, options);

    if ('undefined' == typeof options.prefix) {
        this.options.prefix = 'rtcs.io:'
    }
    if ('undefined' == typeof options.prefixUserSockets) {
        this.options.prefixUserSockets = 'userSockets:'
    }
    if ('undefined' == typeof options.prefixUserRooms) {
        this.options.prefixUserRooms = 'userRooms:'
    }
    if ('undefined' == typeof options.prefixRoomUsers) {
        this.options.prefixRoomUsers = 'roomUsers:'
    }
    if ('undefined' == typeof options.prefixToken) {
        this.options.prefixToken = 'token:'
    }

    this.options.prefixUserSockets = this.options.prefix + this.options.prefixUserSockets;
    this.options.prefixUserRooms = this.options.prefix + this.options.prefixUserRooms;
    this.options.prefixRoomUsers = this.options.prefix + this.options.prefixRoomUsers;
    this.options.prefixToken = this.options.prefix + this.options.prefixToken;

    if (options.client) {
        this.redis = options.client;
    } else {
        var args = [];
        if (options.socket) {
            args.push(options.socket);
        } else if (options.port && options.host) {
            args.push(options.port);
            args.push(options.host);
        }
        args.push(options);

        this.redis = redis.createClient.apply(null, args);
    }

    var self = this;
    self.redis.on('error', function (err) {
        self.emit('error', err);
    });
}

util.inherits(RedisTemp, Temp);

RedisTemp.prototype.addSocket = function(socketId, userId, callback) {
    this.redis.sadd(this.options.prefixUserSockets + userId, socketId, callback);
};

RedisTemp.prototype.removeSocket = function(socketId, userId, callback) {
    var self = this;
    self.redis.srem(self.options.prefixUserSockets + userId, socketId, function(err) {
        if (err) {
            if (callback) callback(err);
        } else {
            self.redis.srandmember(self.options.prefixUserSockets + userId, function(err, reply) {
                if (err) {
                    callback(err);
                } else {
                    callback(null, !!reply);
                }
            });
        }
    });
};

RedisTemp.prototype.getSockets = function(userId, callback) {
    this.redis.smembers(this.options.prefixUserSockets + userId, callback);
};

RedisTemp.prototype.addUserToRoom = function(userId, roomId, callback) {
    var already;
    var pending = 2;

    this.redis.sadd(this.options.prefixUserRooms + userId, roomId, function(err, reply) {
        if (!err) {
            already = !reply;
        }
        done(err);
    });

    this.redis.sadd(this.options.prefixRoomUsers + roomId, userId, done);

    function done(err) {
        if (err) {
            if (callback) callback(err);
        } else if (!--pending) {
            if (callback) callback(null, already);
        }
    }
};

RedisTemp.prototype.removeUserFromRoom = function(userId, roomId, callback) {
    var self = this;
    var already;
    var has;
    var pending = 2;

    self.redis.srem(self.options.prefixUserRooms + userId, roomId, done);

    self.redis.srem(self.options.prefixRoomUsers + roomId, userId, function(err, reply) {
        if (err) {
            done(err);
        } else {
            already = !reply;
            self.redis.srandmember(self.options.prefixRoomUsers + roomId, function(err, reply) {
                if (!err) {
                    has = !!reply;
                }
                done(err);
            });
        }
    });

    function done(err) {
        if (err) {
            if (callback) callback(err);
        } else if (!--pending) {
            if (callback) callback(null, already, has);
        }
    }
};

RedisTemp.prototype.getUserRooms = function(userId, callback) {
    this.redis.smembers(this.options.prefixUserRooms + userId, callback);
};

RedisTemp.prototype.getRoomUsers = function(roomId, callback) {
    this.redis.smembers(this.options.prefixRoomUsers + roomId, callback);
};

RedisTemp.prototype.createToken = function(data, callback) {
    var self = this;
    var token = base64id.generateId();
    self.redis.setnx(self.options.prefixToken + token, JSON.stringify(data), function(err, reply) {
        if (err) {
            callback(err);
        } else if (reply) {
            self.redis.pexpire(self.options.prefixToken + token, self.options.tokenExpire, function(err) {
                if (err) {
                    callback(err);
                } else {
                    callback(null, token);
                }
            });
        } else {
            self.createToken(data, callback);
        }
    });
};

RedisTemp.prototype.releaseToken = function(token, callback) {
    var self = this;
    self.redis.get(self.options.prefixToken + token, function(err, data) {
        if (err) {
            callback(err);
        } else if (data) {
            callback(null, JSON.parse(data));
            self.redis.del(self.options.prefixToken + token);
        } else {
            callback();
        }
    });
};
