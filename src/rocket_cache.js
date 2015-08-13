/**
 * Created by Jun.li on 2015/8/12.
 */
"use strict";
// 依赖lib：config,direct_solid,log4js
var redis = require("redis");
var Stash = require('node-stash');

global.ROCKET_CACHE_TYPE = function () {
    return {}
};

global._RocketCache = function (opts) {
    var extend = function (src, dst) {
        for (var property in src) {
            dst[property] = src[property];
        }
        return dst;
    };
    var conf = {
            redis: {
                wait: true,
                ttl: {
                    cache: 600000,
                    lock: 10000
                }
            },
            timeout: {
                retry: 1000
            },
            lru: {
                max: 1000000,
                maxAge: 600000,
                errTTL: 5000,
                timeout: 5000
            },
            retryLimit: 5
        },
        RocketCache = function (opts) {
            this.init(extend(opts, conf));
        };
    if (typeof CONFIG_REDIS !== 'undefined') {
        conf.redis.clients = {};
        var client1 = redis.createClient(CONFIG_REDIS.redis_port, CONFIG_REDIS.redis_host);
        client1.auth(CONFIG_REDIS.redis_pass);
        conf.redis.clients.cache = client1;
        var client2 = redis.createClient(CONFIG_REDIS.redis_port, CONFIG_REDIS.redis_host);
        client2.auth(CONFIG_REDIS.redis_pass);
        conf.redis.clients.broadcast = client2;
    }
    if (typeof _DirectSolid === 'undefined') {
        _Log.error('require module direct_solid not exist!');
    }
    RocketCache.prototype.init = function (conf) {
        this.sql = conf.sql;//mem和redis都没有时，使用sql语句从数据库读取
        this.piece = conf.piece;//数据碎片化(根据id单条查询)
        this.columns = conf.columns;//sql列(对应语句的?)
        this.dbPoolName = conf.dbPoolName;//数据库名
        this.fresh_time = conf.fresh_time;//数据刷新的时间
        this.dbCallBack = conf.dbCallBack;//数据库读取callBack
        this.type = 'RocketCache:' + conf.type;//数据类型
        if (!conf.piece && typeof conf.type === 'undefined') {
            _Log.error('conf.type 为空');
        }
        if (!isNaN(this.fresh_time)) {
            this.key_set = [];
        }
        this.stash = Stash.createStash(redis.createClient, conf);
        // 定时刷新
        if (this.key_set) {
            var scope = this;
            setInterval(function () {
                scope.key_set.forEach(function (e) {
                    scope.stash.del(e, function (err) {
                        if (err) {
                            _Log.errorObj('timer stash.del error:', err);
                        }
                    });
                });
                scope.key_set = [];
            }, this.fresh_time);
        }
    };

    RocketCache.prototype.getType = function (callBack) {
        var scope = this;
        var fetch = function (done) {
            _DirectSolid(scope.sql, scope.dbCallBack, {
                dbPoolName: scope.dbPoolName,
                columns: scope.columns,
                done: done
            });
        };
        var cb = function (err, results) {
            var backResult = results;
            if (err) {
                backResult = null;
                _Log.errorObj('RocketCache get:', err);
            }
            callBack(backResult);
        };
        var data_key = scope.type;
        this.key_set.push(data_key);
        this.stash.get(data_key, fetch, cb);
    };
    RocketCache.prototype.getPiece = function (opts, key, callBack) {
        if (this.piece !== true) {
            return callBack(null);
        }
        var scope = this;
        var fetch = function (done) {
            _DirectSolid(opts.sql, scope.dbCallBack, {
                dbPoolName: opts.dbPoolName,
                columns: [key],
                done: done
            });
        };
        var cb = function (err, results) {
            var backResult = results;
            //var backResult = (typeof  key === 'undefined' ? results : results[key]);
            if (err) {
                backResult = null;
                _Log.errorObj('RocketCache get:', err);
            }
            callBack(backResult);
        };
        var data_key = 'RocketCache:' + opts.type + ':' + key;
        this.key_set.push(data_key);
        this.stash.get(data_key, fetch, cb);
    };
    RocketCache.prototype.clearType = function (callBack) {
        this.stash.del(this.type, function (err) {
            if (err) {
                _Log.errorObj('stash.del error:', err);
            }
            callBack();
        });
    };
    RocketCache.prototype.clearPiece = function (type, key, callBack) {
        this.stash.del('RocketCache:' + type + ':' + key, function (err) {
            if (err) {
                _Log.errorObj('stash.del error:', err);
            }
            callBack();
        });
    };
    return new RocketCache(opts);
};

