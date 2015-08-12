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
        console.error('require module direct_solid not exist!');
    }
    RocketCache.prototype.init = function (conf) {
        this.sql = conf.sql;
        this.type = conf.type;
        this.columns = conf.columns;
        this.dbPoolName = conf.dbPoolName;
        this.dbCallBack = conf.dbCallBack;
        if (typeof conf.type === 'undefined') {
            _Log.error('conf.type 为空');
        }
        this.stash = Stash.createStash(redis.createClient, conf);
    };

    RocketCache.prototype.get = function (callBack) {
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
            //var backResult = (typeof  key === 'undefined' ? results : results[key]);
            if (err) {
                backResult = null;
                _Log.errorObj('RocketCache get:', err);
            }
            callBack(backResult);
        };
        this.stash.get('RocketCache:' + scope.type, fetch, cb);
    };
    return new RocketCache(opts);
};

