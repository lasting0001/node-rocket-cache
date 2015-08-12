/**
 * Created by Jun.li on 2015/8/12.
 */
"use strict";
var opts = {
    sql: 'SELECT * FROM wumiao_user WHERE id <= ?;',
    dbPoolName: '5miao_game',
    columns: [10000004]
};
opts.dbCallBack = function (results, params) {
    var obj = {};
    results.forEach(function (e) {
        obj[e.id] = e;
    });
    params.done(null, obj);
};

var rocket_cache = _RocketCache(opts);
rocket_cache.get('user_info', 10000004, function (err, result) {
    console.log(result);
});