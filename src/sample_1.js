/**
 * Created by Jun.li on 2015/8/12.
 */
"use strict";
/**
 * Created by Jun.li on 2015/8/12.
 */
"use strict";
require('node-rocket-cache');

function LocalCache() {
    // 用户信息
    function UserInfo() {
        var opts = {
            sql: 'SELECT * FROM t_pb_users;',
            dbPoolName: 'db_boss_pay',
            type: 'test_ops_user'
        };
        opts.dbCallBack = function (results, params) {
            var obj_id = {};
            var obj_user_name = {};
            results.forEach(function (e) {
                obj_id[e.id] = e;
                obj_user_name[e.user_name] = e;
            });
            params.done(null, {by_id: obj_id, by_user_name: obj_user_name});
        };

        var r_cache = _RocketCache(opts);
        return {
            getById: function (id, callBack) {
                return r_cache.get(function (result) {
                    callBack(result.by_id[id]);
                });
            },
            getByUserName: function (user_name, callBack) {
                return r_cache.get(user_name, function (result) {
                    callBack(result.by_user_name[user_name]);
                });
            }
        }
    }

    return {
        UserInfo: UserInfo()
    }
}

global._LocalCache = LocalCache();
