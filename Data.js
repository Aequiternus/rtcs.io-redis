
module.exports = RedisData;

var util = require("util");
var Data = require('rtcs.io-storage').Data;
var redis = require('redis');
var base64id = require('base64id');

function RedisData(options) {
    Data.call(this, options);

    if ('undefined' == typeof options.prefix) {
        this.options.prefix = 'rtcs.io:'
    }
    if ('undefined' == typeof options.prefixSession) {
        this.options.prefixSession = 'session:'
    }
    if ('undefined' == typeof options.prefixUser) {
        this.options.prefixUser = 'user:'
    }
    if ('undefined' == typeof options.prefixRoom) {
        this.options.prefixRoom = 'room:'
    }
    if ('undefined' == typeof options.prefixLog) {
        this.options.prefixLog = 'log:'
    }

    this.options.prefixSession = this.options.prefix + this.options.prefixSession;
    this.options.prefixUser = this.options.prefix + this.options.prefixUser;
    this.options.prefixRoom = this.options.prefix + this.options.prefixRoom;
    this.options.prefixLog = this.options.prefix + this.options.prefixLog;

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

util.inherits(RedisData, Data);

RedisData.prototype.close = function() {
    if (this.redis.connected) {
        this.redis.quit();
    }
};

RedisData.prototype.getSession = function(sessionId, callback) {
    this.redis.get(this.options.prefixSession + sessionId, callback);
};

RedisData.prototype.setSession = function(sessionId, userId, callback) {
    this.redis.set(this.options.prefixSession + sessionId, userId, callback);
};

RedisData.prototype.removeSession = function(sessionId, callback) {
    this.redis.del(this.options.prefixSession + sessionId, callback);
};

RedisData.prototype.getUser = function(userId, callback) {
    this.redis.get(this.options.prefixUser + userId, function(err, reply) {
        callback(err, err ? null : JSON.parse(reply));
    });
};

RedisData.prototype.setUser = function(userId, data, callback) {
    this.redis.set(this.options.prefixUser + userId, JSON.stringify(data), callback);
};

RedisData.prototype.removeUser = function(userId, callback) {
    this.redis.del(this.options.prefixUser + userId, callback);
};

RedisData.prototype.getUsers = function(userIds, callback) {
    var prefixedUserIds = [];
    userIds.forEach(function(userId) {
        prefixedUserIds.push(this.options.prefixUser + userId);
    }, this);
    this.redis.mget(prefixedUserIds, function(err, res) {
        if (err) {
            callback(err);
        } else {
            var users = {};
            userIds.forEach(function(userId, i) {
                users[userId] = res[i] && JSON.parse(res[i]);
            });
            callback(null, users);
        }
    });
};

RedisData.prototype.getGuest = function(name, callback) {
    var self = this;
    if (name) {
        name = name.trim();
    }
    if (name) {
        self.tryStoreGuest_(name, callback);
    } else {
        self.redis.incr(this.prefix + 'guestNum', function(err, name) {
            if (err) {
                callback(err);
            } else {
                self.tryStoreGuest_(name, callback);
            }
        });
    }
};

RedisData.prototype.tryStoreGuest_ = function(name, callback) {
    var self = this;
    var uid = 'guest:' + base64id.generateId();
    self.redis.setnx(this.options.prefixUser + uid, JSON.stringify({
        id: uid,
        public: {
            guest: true,
            name: self.options.guestName + name
        },
        rooms: self.options.guestRooms
    }), function(err, reply) {
        if (err) {
            callback(err);
        } else if (reply) {
            callback(null, uid);
        } else {
            self.tryStoreGuest_(name, callback);
        }
    });
};

Data.prototype.removeGuest = function(userId, callback) {
    if (userId.match(/guest:/)) {
        this.redis.del(this.options.prefixUser + userId, callback);
    } else {
        process.nextTick(callback.bind(null, null));
    }
};

RedisData.prototype.getRoom = function(roomId, callback) {
    this.redis.get(this.options.prefixRoom + roomId, function(err, reply) {
        callback(err, err ? null : JSON.parse(reply));
    });
};

RedisData.prototype.setRoom = function(roomId, data, callback) {
    this.redis.set(this.options.prefixRoom + roomId, JSON.stringify(data), callback);
};

RedisData.prototype.removeRoom = function(roomId, callback) {
    var pending = 2;

    this.redis.del(this.options.prefixRoom + roomId, done);
    this.redis.del(this.options.prefixLog + roomId, done);

    function done(err) {
        if (err) {
            callback(err);
        } else if (!--pending) {
            callback();
        }
    }
};

RedisData.prototype.addLog = function(roomId, msg, callback) {
    var pending = 1;

    if (this.options.historyLength) {
        ++pending;
        this.redis.ltrim(this.options.prefixLog + roomId, 1 - this.options.historyLength, -1, done);
    }

    this.redis.rpush(this.options.prefixLog + roomId, JSON.stringify(msg), done);

    if (this.options.historyExpire) {
        ++pending;
        this.redis.pexpire(this.options.prefixLog + roomId, this.options.historyExpire, done);
    }

    function done(err) {
        if (err) {
            callback(err);
        } else if (!--pending) {
            callback();
        }
    }
};

RedisData.prototype.getLog = function(roomId, time, callback) {
    this.redis.lrange(this.options.prefixLog + roomId, 0, -1, function(err, reply) {
        if (err) {
            callback(err);
        } else {
            var msglog = [];
            for (var i = 0, l = reply.length; i < l; i++) {
                if (!time || reply[i].time > time) {
                    msglog.push(JSON.parse(reply[i]));
                }
            }
            callback(null, msglog);
        }
    });
};
