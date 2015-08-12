/**
 * Created by Jun.li on 2015/8/12.
 */
"use strict";
// 依赖lib：config,direct_solid
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
        conf.redis.port = CONFIG_REDIS.redis_port;
        conf.redis.host = CONFIG_REDIS.redis_host;
        conf.redis.pass = CONFIG_REDIS.redis_pass;
    }
    if (typeof _DirectSolid === 'undefined') {
        console.error('require module direct_solid not exist!');
    }
    RocketCache.prototype.init = function (conf) {
        this.sql = conf.sql;
        this.columns = conf.columns;
        this.dbCallBack = conf.dbCallBack;
        if (typeof  conf.redis.clients === 'undefined') {
            var client = redis.createClient(conf.redis.port, conf.redis.host);
            client.auth(conf.redis.pass);
            conf.redis.clients.cache = client;
            var client1 = redis.createClient(conf.redis.port, conf.redis.host);
            client1.auth(conf.redis.pass);
            conf.redis.clients.broadcast = client1;
        }
        this.stash = Stash.createStash(redis.createClient, conf);
    };

    RocketCache.prototype.get = function (type, key, callBack) {
        var scope = this;
        var fetch = function (done) {
            _DirectSolid(scope.sql, scope.dbCallBack, {
                dbPoolName: '5miao_game',
                columns: scope.columns,
                done: done
            });
        };
        var cb = function (err, results) {
            var backResult = (typeof  key === 'undefined' ? results : results[key]);
            callBack(err, backResult);
        };
        this.stash.get('RocketCache:' + type + ':' + key, fetch, cb);
    };
    return new RocketCache(opts);
};

